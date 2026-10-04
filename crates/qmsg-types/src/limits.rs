//! What a channel takes, and checking a message against it.
//!
//! Every platform limits messages differently, and often by account or
//! organization too, such as Discord's upload size growing with a server's
//! boosts. Providers report the limits that apply to the account in each
//! [`Channel`](crate::Channel), so the orchestrator can refuse a message with a
//! precise reason before sending it, instead of waiting for the platform.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Content, ContentKind};

/// What a channel takes of one kind of content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRule {
    pub kind: ContentKind,
    /// For text, the most characters (Unicode scalar values) in all of a
    /// message's text parts together. For anything else, the most bytes in
    /// each part.
    pub max_size: Option<u64>,
    /// The MIME types taken for media, such as `image/png`, or `image/*` for
    /// any image. Empty takes any.
    pub mime_types: Vec<String>,
}

impl ContentRule {
    /// Takes any amount of `kind`.
    pub fn new(kind: ContentKind) -> Self {
        Self {
            kind,
            max_size: None,
            mime_types: Vec::new(),
        }
    }

    pub fn max_size(self, max_size: u64) -> Self {
        Self {
            max_size: Some(max_size),
            ..self
        }
    }

    fn takes_mime(&self, mime: &str) -> bool {
        self.mime_types
            .iter()
            .any(|pattern| match pattern.strip_suffix("/*") {
                Some(prefix) => mime
                    .split_once('/')
                    .is_some_and(|(top, _)| top.eq_ignore_ascii_case(prefix)),
                None => pattern.eq_ignore_ascii_case(mime),
            })
    }
}

/// Limits on a whole message.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageLimits {
    /// The most media and custom parts in one message.
    pub max_attachments: Option<u32>,
    /// The most bytes of media and custom parts in one message, together.
    pub max_total_size: Option<u64>,
}

/// How a message breaks a channel's rules. Parts are numbered from 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Violation {
    /// The message has no content.
    Empty,
    /// The channel doesn't take the part's kind of content.
    Unsupported {
        part: u32,
        kind: ContentKind,
    },
    /// The channel doesn't take the part's MIME type, or needs one and the
    /// part has none.
    UnsupportedMime {
        part: u32,
        mime: Option<String>,
    },
    TextTooLong {
        length: u64,
        max: u64,
    },
    TooLarge {
        part: u32,
        size: u64,
        max: u64,
    },
    TooManyAttachments {
        count: u32,
        max: u32,
    },
    TotalTooLarge {
        size: u64,
        max: u64,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "the message is empty"),
            Self::Unsupported { part, kind } => {
                write!(f, "part {part}: the channel doesn't take {kind:?}")
            }
            Self::UnsupportedMime {
                part,
                mime: Some(mime),
            } => {
                write!(f, "part {part}: the channel doesn't take {mime}")
            }
            Self::UnsupportedMime { part, mime: None } => {
                write!(f, "part {part}: the channel needs a MIME type")
            }
            Self::TextTooLong { length, max } => {
                write!(f, "the text is {length} characters, over the {max} limit")
            }
            Self::TooLarge { part, size, max } => {
                write!(f, "part {part} is {size} bytes, over the {max} byte limit")
            }
            Self::TooManyAttachments { count, max } => {
                write!(f, "{count} attachments, over the limit of {max}")
            }
            Self::TotalTooLarge { size, max } => {
                write!(
                    f,
                    "the attachments are {size} bytes, over the {max} byte limit"
                )
            }
        }
    }
}

impl std::error::Error for Violation {}

/// Checks `content` against a channel's rules and limits.
///
/// Parts whose size isn't known, such as a URL without a size, pass the size
/// limits.
pub fn check(
    rules: &[ContentRule],
    limits: &MessageLimits,
    content: &[Content],
) -> Result<(), Violation> {
    if content.is_empty() {
        return Err(Violation::Empty);
    }
    let mut text_length = 0u64;
    let mut attachments = 0u32;
    let mut total_size = 0u64;
    for (part, item) in (0u32..).zip(content) {
        let Some(rule) = rules.iter().find(|rule| rule.kind.matches(item)) else {
            return Err(Violation::Unsupported {
                part,
                kind: item.kind(),
            });
        };
        let size = match item {
            Content::Text(text) => {
                text_length += text.chars().count() as u64;
                continue;
            }
            Content::Image(media)
            | Content::Video(media)
            | Content::Audio(media)
            | Content::File(media) => {
                if !rule.mime_types.is_empty()
                    && !media.mime.as_deref().is_some_and(|m| rule.takes_mime(m))
                {
                    return Err(Violation::UnsupportedMime {
                        part,
                        mime: media.mime.clone(),
                    });
                }
                media.size()
            }
            Content::Custom { data, .. } => Some(data.len() as u64),
        };
        attachments += 1;
        if let Some(size) = size {
            total_size += size;
            if let Some(max) = rule.max_size
                && size > max
            {
                return Err(Violation::TooLarge { part, size, max });
            }
        }
    }
    let text_max = rules
        .iter()
        .find(|rule| rule.kind == ContentKind::Text)
        .and_then(|rule| rule.max_size);
    if let Some(max) = text_max
        && text_length > max
    {
        return Err(Violation::TextTooLong {
            length: text_length,
            max,
        });
    }
    if let Some(max) = limits.max_attachments
        && attachments > max
    {
        return Err(Violation::TooManyAttachments {
            count: attachments,
            max,
        });
    }
    if let Some(max) = limits.max_total_size
        && total_size > max
    {
        return Err(Violation::TotalTooLarge {
            size: total_size,
            max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Media, MediaSource};

    fn image(mime: Option<&str>, size: u64) -> Content {
        Content::Image(Media {
            name: None,
            mime: mime.map(str::to_owned),
            size: Some(size),
            source: MediaSource::Url("https://example.com/a".into()),
        })
    }

    /// Roughly Discord's: 2000 characters, 10 files of up to 10 MB.
    fn rules() -> (Vec<ContentRule>, MessageLimits) {
        let rules = vec![
            ContentRule::new(ContentKind::Text).max_size(2000),
            ContentRule {
                mime_types: vec!["image/*".into()],
                ..ContentRule::new(ContentKind::Image).max_size(10_000_000)
            },
        ];
        let limits = MessageLimits {
            max_attachments: Some(10),
            max_total_size: Some(25_000_000),
        };
        (rules, limits)
    }

    #[test]
    fn accepts_content_within_the_limits() {
        let (rules, limits) = rules();
        let content = [
            Content::Text("look".into()),
            image(Some("image/PNG"), 1_000),
        ];
        assert_eq!(check(&rules, &limits, &content), Ok(()));
    }

    #[test]
    fn reports_the_first_violation() {
        let (rules, limits) = rules();
        let cases = [
            (vec![], Violation::Empty),
            (
                vec![Content::Text("hi".into()), Content::Audio(media())],
                Violation::Unsupported {
                    part: 1,
                    kind: ContentKind::Audio,
                },
            ),
            (
                vec![image(Some("video/mp4"), 1)],
                Violation::UnsupportedMime {
                    part: 0,
                    mime: Some("video/mp4".into()),
                },
            ),
            (
                vec![image(None, 1)],
                Violation::UnsupportedMime {
                    part: 0,
                    mime: None,
                },
            ),
            (
                vec![
                    Content::Text("é".repeat(1500)),
                    Content::Text("é".repeat(501)),
                ],
                Violation::TextTooLong {
                    length: 2001,
                    max: 2000,
                },
            ),
            (
                vec![image(Some("image/png"), 10_000_001)],
                Violation::TooLarge {
                    part: 0,
                    size: 10_000_001,
                    max: 10_000_000,
                },
            ),
            (
                vec![image(Some("image/png"), 1); 11],
                Violation::TooManyAttachments { count: 11, max: 10 },
            ),
            (
                vec![image(Some("image/png"), 9_000_000); 3],
                Violation::TotalTooLarge {
                    size: 27_000_000,
                    max: 25_000_000,
                },
            ),
        ];
        for (content, violation) in cases {
            assert_eq!(check(&rules, &limits, &content), Err(violation));
        }
    }

    #[test]
    fn unknown_sizes_pass() {
        let (rules, limits) = rules();
        let mut unsized_image = media();
        unsized_image.mime = Some("image/png".into());
        unsized_image.size = None;
        let content = [Content::Image(unsized_image)];
        assert_eq!(check(&rules, &limits, &content), Ok(()));
    }

    fn media() -> Media {
        Media {
            name: None,
            mime: None,
            size: None,
            source: MediaSource::Url("https://example.com/a".into()),
        }
    }
}

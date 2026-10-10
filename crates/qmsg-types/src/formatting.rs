//! Formatted text, mentions and reactions shared by every platform.
//!
//! Providers convert their platform's markup, such as Discord markdown or
//! Slack blocks, to and from [`FormattedText`]. Formatting a platform has no
//! form for, such as content only one platform has, stays
//! [`Content::Custom`](crate::Content::Custom).

use serde::{Deserialize, Serialize};

/// Text with styled ranges. Ranges can nest and overlap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormattedText {
    /// The text as it reads without formatting, including a mention's
    /// display text, such as `@Ana`.
    pub text: String,
    pub spans: Vec<Span>,
}

/// A style over `text[start..end]`, in UTF-8 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start: u32,
    pub end: u32,
    pub style: Style,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Style {
    Bold,
    Italic,
    Underline,
    Strikethrough,
    Code,
    CodeBlock { language: Option<String> },
    Quote,
    Spoiler,
    Link(String),
    Mention(Mention),
}

/// Who or what a mention points at. Ids are in the channel's scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mention {
    User(String),
    Channel(String),
    /// A group of users the platform names, such as a Discord role.
    Role(String),
    /// Everyone in the channel, such as `@everyone` or `@channel`.
    Everyone,
}

impl FormattedText {
    pub fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            spans: Vec::new(),
        }
    }

    /// Whether every span is in the text and starts and ends on a character
    /// boundary.
    pub fn is_valid(&self) -> bool {
        self.spans.iter().all(|span| {
            let (start, end) = (span.start as usize, span.end as usize);
            start <= end && self.text.get(start..end).is_some()
        })
    }

    /// The users mentioned.
    pub fn mentioned_users(&self) -> impl Iterator<Item = &str> {
        self.spans.iter().filter_map(|span| match &span.style {
            Style::Mention(Mention::User(id)) => Some(id.as_str()),
            _ => None,
        })
    }
}

/// The reactions to a message with one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reaction {
    /// A Unicode emoji such as `👍`, or the provider's id for a custom one.
    pub key: String,
    pub count: u32,
    /// Whether the account is one of them.
    pub me: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_must_fit_the_text() {
        let span = |start, end| Span {
            start,
            end,
            style: Style::Mention(Mention::User("ana".into())),
        };
        let mut text = FormattedText {
            text: "hi @Ána".into(),
            spans: vec![span(3, 8)],
        };
        assert!(text.is_valid());
        assert_eq!(text.mentioned_users().collect::<Vec<_>>(), ["ana"]);
        // Inside the two-byte `Á`, past the end, and backwards.
        for bad in [span(3, 5), span(3, 9), span(4, 3)] {
            text.spans = vec![bad];
            assert!(!text.is_valid());
        }
    }
}

//! The organizations, users and channels each provider is part of.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use qmsg_types::{Channel, ChannelRef, DirectoryUpdate, Message, Organization, User};

/// Organizations, users and channels by provider, kept up to date from
/// [`ProviderEvent::Directory`](crate::ProviderEvent::Directory).
#[derive(Debug, Default)]
pub struct Directory {
    providers: HashMap<String, Entry>,
}

#[derive(Debug, Default)]
struct Entry {
    organizations: BTreeMap<String, OrganizationEntry>,
    /// Users and channels outside any organization.
    standalone: Scope,
}

/// An organization as the [`Directory`] keeps it.
#[derive(Debug)]
pub struct OrganizationEntry {
    pub id: String,
    pub name: String,
    pub scope: Scope,
}

/// The users and channels in an organization, or outside any.
#[derive(Debug, Default)]
pub struct Scope {
    me: Option<String>,
    users: BTreeMap<String, User>,
    channels: BTreeMap<String, Channel>,
}

impl Scope {
    /// The account's own user id.
    pub fn me(&self) -> Option<&str> {
        self.me.as_deref()
    }

    /// Ordered by id.
    pub fn users(&self) -> impl Iterator<Item = &User> {
        self.users.values()
    }

    pub fn user(&self, id: &str) -> Option<&User> {
        self.users.get(id)
    }

    /// Ordered by id.
    pub fn channels(&self) -> impl Iterator<Item = &Channel> {
        self.channels.values()
    }

    pub fn channel(&self, id: &str) -> Option<&Channel> {
        self.channels.get(id)
    }

    fn is_empty(&self) -> bool {
        self.me.is_none() && self.users.is_empty() && self.channels.is_empty()
    }
}

/// Why [`Directory::apply`] changed nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// The update is for an organization the provider hasn't reported.
    UnknownOrganization(String),
    UnknownUser(String),
    UnknownChannel(String),
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownOrganization(id) => write!(f, "unknown organization `{id}`"),
            Self::UnknownUser(id) => write!(f, "unknown user `{id}`"),
            Self::UnknownChannel(id) => write!(f, "unknown channel `{id}`"),
        }
    }
}

impl std::error::Error for ApplyError {}

impl Directory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies an update from `provider`.
    ///
    /// Fails, changing nothing, for updates to an organization the provider
    /// hasn't reported and for removals of things that aren't there. Either
    /// means the provider sent updates out of order, or the directory missed
    /// some.
    pub fn apply(&mut self, provider: &str, update: DirectoryUpdate) -> Result<(), ApplyError> {
        let entry = self.providers.entry(provider.to_owned()).or_default();
        let result = entry.apply(update);
        if entry.organizations.is_empty() && entry.standalone.is_empty() {
            self.providers.remove(provider);
        }
        result
    }

    /// Forgets everything `provider` reported, such as once it has exited.
    pub fn remove_provider(&mut self, provider: &str) {
        self.providers.remove(provider);
    }

    /// The provider's organizations, ordered by id.
    pub fn organizations(&self, provider: &str) -> impl Iterator<Item = &OrganizationEntry> {
        self.providers
            .get(provider)
            .into_iter()
            .flat_map(|entry| entry.organizations.values())
    }

    pub fn organization(&self, provider: &str, id: &str) -> Option<&OrganizationEntry> {
        self.providers.get(provider)?.organizations.get(id)
    }

    /// The users and channels in `organization`, or outside any when it is
    /// `None`.
    pub fn scope(&self, provider: &str, organization: Option<&str>) -> Option<&Scope> {
        let entry = self.providers.get(provider)?;
        match organization {
            Some(id) => entry.organizations.get(id).map(|o| &o.scope),
            None => Some(&entry.standalone),
        }
    }

    pub fn user(&self, provider: &str, organization: Option<&str>, id: &str) -> Option<&User> {
        self.scope(provider, organization)?.user(id)
    }

    pub fn channel(&self, provider: &str, channel: &ChannelRef) -> Option<&Channel> {
        self.scope(provider, channel.organization.as_deref())?
            .channel(&channel.channel)
    }

    /// The message's author, from its channel's scope.
    pub fn author(&self, provider: &str, message: &Message) -> Option<&User> {
        self.user(
            provider,
            message.channel.organization.as_deref(),
            &message.author,
        )
    }
}

impl Entry {
    fn apply(&mut self, update: DirectoryUpdate) -> Result<(), ApplyError> {
        match update {
            DirectoryUpdate::OrganizationUpserted(organization) => {
                let Organization {
                    id,
                    name,
                    me,
                    users,
                    channels,
                } = organization;
                let scope = Scope {
                    me,
                    users: users.into_iter().map(|u| (u.id.clone(), u)).collect(),
                    channels: channels.into_iter().map(|c| (c.id.clone(), c)).collect(),
                };
                self.organizations
                    .insert(id.clone(), OrganizationEntry { id, name, scope });
            }
            DirectoryUpdate::OrganizationRemoved { id } => {
                self.organizations
                    .remove(&id)
                    .ok_or(ApplyError::UnknownOrganization(id))?;
            }
            DirectoryUpdate::UserUpserted { organization, user } => {
                self.scope_mut(organization)?
                    .users
                    .insert(user.id.clone(), user);
            }
            DirectoryUpdate::UserRemoved { organization, id } => {
                self.scope_mut(organization)?
                    .users
                    .remove(&id)
                    .ok_or(ApplyError::UnknownUser(id))?;
            }
            DirectoryUpdate::ChannelUpserted {
                organization,
                channel,
            } => {
                self.scope_mut(organization)?
                    .channels
                    .insert(channel.id.clone(), channel);
            }
            DirectoryUpdate::ChannelRemoved(ChannelRef {
                organization,
                channel,
            }) => {
                self.scope_mut(organization)?
                    .channels
                    .remove(&channel)
                    .ok_or(ApplyError::UnknownChannel(channel))?;
            }
            DirectoryUpdate::Me { organization, id } => {
                self.scope_mut(organization)?.me = Some(id);
            }
        }
        Ok(())
    }

    fn scope_mut(&mut self, organization: Option<String>) -> Result<&mut Scope, ApplyError> {
        match organization {
            Some(id) => match self.organizations.get_mut(&id) {
                Some(organization) => Ok(&mut organization.scope),
                None => Err(ApplyError::UnknownOrganization(id)),
            },
            None => Ok(&mut self.standalone),
        }
    }
}

#[cfg(test)]
mod tests {
    use qmsg_types::{ChannelKind, Content, ContentKind};

    use super::*;

    fn user(id: &str, name: &str) -> User {
        User {
            id: id.into(),
            name: name.into(),
        }
    }

    fn channel(id: &str, kind: ChannelKind) -> Channel {
        Channel {
            accepted_content: vec![ContentKind::Text],
            ..Channel::new(id, id, kind)
        }
    }

    fn organization() -> Organization {
        Organization {
            id: "org".into(),
            name: "Org".into(),
            me: Some("ana".into()),
            users: vec![user("ana", "Ana")],
            channels: vec![channel("general", ChannelKind::Text)],
        }
    }

    fn org() -> Option<String> {
        Some("org".into())
    }

    #[test]
    fn updates_users_and_channels() {
        let mut d = Directory::new();
        d.apply("p", DirectoryUpdate::OrganizationUpserted(organization()))
            .unwrap();
        let updates = [
            DirectoryUpdate::UserUpserted {
                organization: org(),
                user: user("ana", "Ana B"),
            },
            DirectoryUpdate::UserUpserted {
                organization: org(),
                user: user("bo", "Bo"),
            },
            DirectoryUpdate::ChannelUpserted {
                organization: org(),
                channel: Channel {
                    parent: Some("general".into()),
                    ..channel("thread", ChannelKind::Thread)
                },
            },
            DirectoryUpdate::ChannelRemoved(ChannelRef::new(Some("org"), "general")),
        ];
        for update in updates {
            d.apply("p", update).unwrap();
        }

        let scope = d.scope("p", Some("org")).unwrap();
        assert_eq!(scope.me(), Some("ana"));
        assert_eq!(
            scope.users().collect::<Vec<_>>(),
            [&user("ana", "Ana B"), &user("bo", "Bo")]
        );
        let ids: Vec<_> = scope.channels().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["thread"]);
        assert_eq!(d.user("p", Some("org"), "bo"), Some(&user("bo", "Bo")));
    }

    #[test]
    fn keeps_users_and_channels_outside_organizations() {
        let mut d = Directory::new();
        let dm = Channel {
            members: Some(vec!["me".into(), "ana".into()]),
            ..channel("dm-ana", ChannelKind::Direct)
        };
        let updates = [
            DirectoryUpdate::Me {
                organization: None,
                id: "me".into(),
            },
            DirectoryUpdate::UserUpserted {
                organization: None,
                user: user("ana", "Ana"),
            },
            DirectoryUpdate::ChannelUpserted {
                organization: None,
                channel: dm.clone(),
            },
        ];
        for update in updates {
            d.apply("p", update).unwrap();
        }
        assert_eq!(d.organizations("p").count(), 0);
        let dm_ref = ChannelRef::new(None, "dm-ana");
        assert_eq!(d.channel("p", &dm_ref), Some(&dm));
        assert_eq!(d.scope("p", None).unwrap().me(), Some("me"));
        // A channel with the same id in an organization is a different one.
        assert_eq!(
            d.channel("p", &ChannelRef::new(Some("org"), "dm-ana")),
            None
        );

        let message = Message {
            id: "1".into(),
            channel: dm_ref.clone(),
            author: "ana".into(),
            sent_at: 0,
            reply_to: None,
            content: vec![Content::Text("hi".into())],
        };
        assert_eq!(d.author("p", &message), Some(&user("ana", "Ana")));

        d.apply("p", DirectoryUpdate::ChannelRemoved(dm_ref))
            .unwrap();
        assert_eq!(d.scope("p", None).unwrap().channels().count(), 0);
    }

    #[test]
    fn organizations_are_kept_per_provider() {
        let mut d = Directory::new();
        d.apply("a", DirectoryUpdate::OrganizationUpserted(organization()))
            .unwrap();
        d.apply("b", DirectoryUpdate::OrganizationUpserted(organization()))
            .unwrap();

        d.apply(
            "a",
            DirectoryUpdate::OrganizationRemoved { id: "org".into() },
        )
        .unwrap();
        assert!(d.organization("a", "org").is_none());
        assert!(d.organization("b", "org").is_some());

        d.remove_provider("b");
        assert_eq!(d.organizations("b").count(), 0);
    }

    #[test]
    fn reports_updates_it_cannot_apply() {
        let mut d = Directory::new();
        assert_eq!(
            d.apply(
                "p",
                DirectoryUpdate::UserUpserted {
                    organization: org(),
                    user: user("ana", "Ana"),
                },
            ),
            Err(ApplyError::UnknownOrganization("org".into()))
        );
        assert_eq!(d.organizations("p").count(), 0);

        d.apply("p", DirectoryUpdate::OrganizationUpserted(organization()))
            .unwrap();
        assert_eq!(
            d.apply(
                "p",
                DirectoryUpdate::ChannelRemoved(ChannelRef::new(Some("org"), "nope")),
            ),
            Err(ApplyError::UnknownChannel("nope".into()))
        );
    }

    #[test]
    fn later_duplicates_in_a_snapshot_win() {
        let mut d = Directory::new();
        let mut organization = organization();
        organization.users.push(user("ana", "Ana 2"));
        d.apply("p", DirectoryUpdate::OrganizationUpserted(organization))
            .unwrap();
        let scope = d.scope("p", Some("org")).unwrap();
        assert_eq!(scope.users().count(), 1);
        assert_eq!(scope.user("ana"), Some(&user("ana", "Ana 2")));
    }
}

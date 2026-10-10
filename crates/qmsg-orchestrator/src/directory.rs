//! The organizations, users and channels each provider is part of.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::ProviderId;

use qmsg_types::{Channel, ChannelRef, DirectoryUpdate, Message, Organization, Presence, User};

/// Organizations, users and channels by provider instance, kept up to date from
/// [`ProviderEvent::Directory`](crate::ProviderEvent::Directory).
#[derive(Debug, Default)]
pub struct Directory {
    providers: HashMap<ProviderId, Entry>,
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
    presence: BTreeMap<String, Presence>,
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

    /// The user's presence, if the provider reported one.
    pub fn presence(&self, user: &str) -> Option<Presence> {
        self.presence.get(user).copied()
    }

    fn is_empty(&self) -> bool {
        self.me.is_none()
            && self.users.is_empty()
            && self.channels.is_empty()
            && self.presence.is_empty()
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
    pub fn apply(
        &mut self,
        provider: &ProviderId,
        update: DirectoryUpdate,
    ) -> Result<(), ApplyError> {
        let entry = self.providers.entry(provider.to_owned()).or_default();
        let result = entry.apply(update);
        if entry.organizations.is_empty() && entry.standalone.is_empty() {
            self.providers.remove(provider);
        }
        result
    }

    /// Forgets only this instance's state when its `Exited` arrives. A later
    /// spawn with the same name has a different id and keeps its state.
    pub fn remove_provider(&mut self, provider: &ProviderId) {
        self.providers.remove(provider);
    }

    /// The provider's organizations, ordered by id.
    pub fn organizations(&self, provider: &ProviderId) -> impl Iterator<Item = &OrganizationEntry> {
        self.providers
            .get(provider)
            .into_iter()
            .flat_map(|entry| entry.organizations.values())
    }

    pub fn organization(&self, provider: &ProviderId, id: &str) -> Option<&OrganizationEntry> {
        self.providers.get(provider)?.organizations.get(id)
    }

    /// The users and channels in `organization`, or outside any when it is
    /// `None`.
    pub fn scope(&self, provider: &ProviderId, organization: Option<&str>) -> Option<&Scope> {
        let entry = self.providers.get(provider)?;
        match organization {
            Some(id) => entry.organizations.get(id).map(|o| &o.scope),
            None => Some(&entry.standalone),
        }
    }

    pub fn user(
        &self,
        provider: &ProviderId,
        organization: Option<&str>,
        id: &str,
    ) -> Option<&User> {
        self.scope(provider, organization)?.user(id)
    }

    pub fn channel(&self, provider: &ProviderId, channel: &ChannelRef) -> Option<&Channel> {
        self.scope(provider, channel.organization.as_deref())?
            .channel(&channel.channel)
    }

    /// The message's author, from its channel's scope.
    pub fn author(&self, provider: &ProviderId, message: &Message) -> Option<&User> {
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
                    presence: BTreeMap::new(),
                };
                self.organizations
                    .insert(id.clone(), OrganizationEntry { id, name, scope });
            }
            DirectoryUpdate::OrganizationUpdated { id, name } => {
                match self.organizations.get_mut(&id) {
                    Some(organization) => organization.name = name,
                    None => return Err(ApplyError::UnknownOrganization(id)),
                }
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
                let scope = self.scope_mut(organization)?;
                if scope.users.remove(&id).is_none() {
                    return Err(ApplyError::UnknownUser(id));
                }
                scope.presence.remove(&id);
                // The account is no longer there.
                if scope.me.as_deref() == Some(id.as_str()) {
                    scope.me = None;
                }
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
                self.scope_mut(organization)?.me = id;
            }
            DirectoryUpdate::Presence {
                organization,
                user,
                presence,
            } => {
                self.scope_mut(organization)?
                    .presence
                    .insert(user, presence);
            }
            DirectoryUpdate::Reset => *self = Entry::default(),
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

    fn provider(name: &str) -> ProviderId {
        ProviderId {
            name: name.into(),
            instance: 0,
        }
    }

    fn user(id: &str, name: &str) -> User {
        User {
            id: id.into(),
            name: name.into(),
        }
    }

    fn channel(id: &str, kind: ChannelKind) -> Channel {
        Channel::new(id, id, kind).accepting([ContentKind::Text])
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
        d.apply(
            &provider("p"),
            DirectoryUpdate::OrganizationUpserted(organization()),
        )
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
            d.apply(&provider("p"), update).unwrap();
        }

        let scope = d.scope(&provider("p"), Some("org")).unwrap();
        assert_eq!(scope.me(), Some("ana"));
        assert_eq!(
            scope.users().collect::<Vec<_>>(),
            [&user("ana", "Ana B"), &user("bo", "Bo")]
        );
        let ids: Vec<_> = scope.channels().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["thread"]);
        assert_eq!(
            d.user(&provider("p"), Some("org"), "bo"),
            Some(&user("bo", "Bo"))
        );
    }

    #[test]
    fn updating_an_organization_keeps_its_users() {
        let mut d = Directory::new();
        d.apply(
            &provider("p"),
            DirectoryUpdate::OrganizationUpserted(organization()),
        )
        .unwrap();
        d.apply(
            &provider("p"),
            DirectoryUpdate::UserUpserted {
                organization: org(),
                user: user("bo", "Bo"),
            },
        )
        .unwrap();
        d.apply(
            &provider("p"),
            DirectoryUpdate::OrganizationUpdated {
                id: "org".into(),
                name: "Renamed".into(),
            },
        )
        .unwrap();
        let organization = d.organization(&provider("p"), "org").unwrap();
        assert_eq!(organization.name, "Renamed");
        assert_eq!(organization.scope.users().count(), 2);
        assert_eq!(
            d.apply(
                &provider("p"),
                DirectoryUpdate::OrganizationUpdated {
                    id: "nope".into(),
                    name: "x".into(),
                },
            ),
            Err(ApplyError::UnknownOrganization("nope".into()))
        );
    }

    #[test]
    fn the_account_goes_with_its_user() {
        let mut d = Directory::new();
        d.apply(
            &provider("p"),
            DirectoryUpdate::OrganizationUpserted(organization()),
        )
        .unwrap();
        // Removing someone else, or no one, leaves it.
        d.apply(
            &provider("p"),
            DirectoryUpdate::UserUpserted {
                organization: org(),
                user: user("bo", "Bo"),
            },
        )
        .unwrap();
        let remove = |id: &str| DirectoryUpdate::UserRemoved {
            organization: org(),
            id: id.into(),
        };
        d.apply(&provider("p"), remove("bo")).unwrap();
        assert!(d.apply(&provider("p"), remove("nope")).is_err());
        assert_eq!(
            d.scope(&provider("p"), Some("org")).unwrap().me(),
            Some("ana")
        );

        d.apply(&provider("p"), remove("ana")).unwrap();
        assert_eq!(d.scope(&provider("p"), Some("org")).unwrap().me(), None);

        // `Me` sets and clears it too.
        let me = |id: Option<&str>| DirectoryUpdate::Me {
            organization: org(),
            id: id.map(str::to_owned),
        };
        d.apply(&provider("p"), me(Some("bo"))).unwrap();
        assert_eq!(
            d.scope(&provider("p"), Some("org")).unwrap().me(),
            Some("bo")
        );
        d.apply(&provider("p"), me(None)).unwrap();
        assert_eq!(d.scope(&provider("p"), Some("org")).unwrap().me(), None);
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
                id: Some("me".into()),
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
            d.apply(&provider("p"), update).unwrap();
        }
        assert_eq!(d.organizations(&provider("p")).count(), 0);
        let dm_ref = ChannelRef::new(None, "dm-ana");
        assert_eq!(d.channel(&provider("p"), &dm_ref), Some(&dm));
        assert_eq!(d.scope(&provider("p"), None).unwrap().me(), Some("me"));
        // A channel with the same id in an organization is a different one.
        assert_eq!(
            d.channel(&provider("p"), &ChannelRef::new(Some("org"), "dm-ana")),
            None
        );

        let message = Message::new(
            "1",
            dm_ref.clone(),
            "ana",
            0,
            vec![Content::Text("hi".into())],
        );
        assert_eq!(
            d.author(&provider("p"), &message),
            Some(&user("ana", "Ana"))
        );

        d.apply(&provider("p"), DirectoryUpdate::ChannelRemoved(dm_ref))
            .unwrap();
        assert_eq!(d.scope(&provider("p"), None).unwrap().channels().count(), 0);
    }

    #[test]
    fn organizations_are_kept_per_provider() {
        let mut d = Directory::new();
        d.apply(
            &provider("a"),
            DirectoryUpdate::OrganizationUpserted(organization()),
        )
        .unwrap();
        d.apply(
            &provider("b"),
            DirectoryUpdate::OrganizationUpserted(organization()),
        )
        .unwrap();

        d.apply(
            &provider("a"),
            DirectoryUpdate::OrganizationRemoved { id: "org".into() },
        )
        .unwrap();
        assert!(d.organization(&provider("a"), "org").is_none());
        assert!(d.organization(&provider("b"), "org").is_some());

        d.remove_provider(&provider("b"));
        assert_eq!(d.organizations(&provider("b")).count(), 0);
    }

    #[test]
    fn reports_updates_it_cannot_apply() {
        let mut d = Directory::new();
        assert_eq!(
            d.apply(
                &provider("p"),
                DirectoryUpdate::UserUpserted {
                    organization: org(),
                    user: user("ana", "Ana"),
                },
            ),
            Err(ApplyError::UnknownOrganization("org".into()))
        );
        assert_eq!(d.organizations(&provider("p")).count(), 0);

        d.apply(
            &provider("p"),
            DirectoryUpdate::OrganizationUpserted(organization()),
        )
        .unwrap();
        assert_eq!(
            d.apply(
                &provider("p"),
                DirectoryUpdate::ChannelRemoved(ChannelRef::new(Some("org"), "nope")),
            ),
            Err(ApplyError::UnknownChannel("nope".into()))
        );
    }

    #[test]
    fn keeps_presence_and_forgets_everything_on_reset() {
        let mut d = Directory::new();
        let p = provider("p");
        d.apply(&p, DirectoryUpdate::OrganizationUpserted(organization()))
            .unwrap();
        let presence = |user: &str, presence| DirectoryUpdate::Presence {
            organization: org(),
            user: user.into(),
            presence,
        };
        d.apply(&p, presence("ana", Presence::Idle)).unwrap();
        // Users that aren't reported yet can have one too.
        d.apply(&p, presence("bo", Presence::Online)).unwrap();
        let scope = d.scope(&p, Some("org")).unwrap();
        assert_eq!(scope.presence("ana"), Some(Presence::Idle));
        assert_eq!(scope.presence("bo"), Some(Presence::Online));
        d.apply(
            &p,
            DirectoryUpdate::UserRemoved {
                organization: org(),
                id: "ana".into(),
            },
        )
        .unwrap();
        assert_eq!(d.scope(&p, Some("org")).unwrap().presence("ana"), None);

        d.apply(
            &p,
            DirectoryUpdate::ChannelUpserted {
                organization: None,
                channel: channel("dm", ChannelKind::Direct),
            },
        )
        .unwrap();
        // A reconnecting provider resets, then sends a full snapshot.
        d.apply(&p, DirectoryUpdate::Reset).unwrap();
        assert_eq!(d.organizations(&p).count(), 0);
        assert!(d.channel(&p, &ChannelRef::new(None, "dm")).is_none());
        d.apply(&p, DirectoryUpdate::OrganizationUpserted(organization()))
            .unwrap();
        let scope = d.scope(&p, Some("org")).unwrap();
        assert_eq!(scope.presence("bo"), None);
        assert!(scope.channel("general").is_some());
    }

    #[test]
    fn later_duplicates_in_a_snapshot_win() {
        let mut d = Directory::new();
        let mut organization = organization();
        organization.users.push(user("ana", "Ana 2"));
        d.apply(
            &provider("p"),
            DirectoryUpdate::OrganizationUpserted(organization),
        )
        .unwrap();
        let scope = d.scope(&provider("p"), Some("org")).unwrap();
        assert_eq!(scope.users().count(), 1);
        assert_eq!(scope.user("ana"), Some(&user("ana", "Ana 2")));
    }
}

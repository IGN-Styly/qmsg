//! The organizations and channels each provider is part of.

use std::collections::{BTreeMap, HashMap};

use qmsg_types::{Channel, DirectoryUpdate, Organization, User};

/// Organizations and channels by provider, kept up to date from
/// [`ProviderEvent::Directory`](crate::ProviderEvent::Directory).
#[derive(Debug, Default)]
pub struct Directory {
    providers: HashMap<String, Entry>,
}

#[derive(Debug, Default)]
struct Entry {
    organizations: BTreeMap<String, Organization>,
    /// Channels outside any organization, such as direct messages.
    channels: Vec<Channel>,
}

impl Directory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies an update from `provider`.
    ///
    /// Updates to users or channels of an organization the provider hasn't
    /// reported are ignored, and so are removals of things that aren't there.
    pub fn apply(&mut self, provider: &str, update: DirectoryUpdate) {
        let entry = self.providers.entry(provider.to_owned()).or_default();
        match update {
            DirectoryUpdate::OrganizationSet(organization) => {
                entry
                    .organizations
                    .insert(organization.id.clone(), organization);
            }
            DirectoryUpdate::OrganizationRemoved { id } => {
                entry.organizations.remove(&id);
            }
            DirectoryUpdate::UserSet { organization, user } => {
                if let Some(organization) = entry.organizations.get_mut(&organization) {
                    set(&mut organization.users, user, |u| &u.id);
                }
            }
            DirectoryUpdate::UserRemoved { organization, id } => {
                if let Some(organization) = entry.organizations.get_mut(&organization) {
                    organization.users.retain(|u| u.id != id);
                }
            }
            DirectoryUpdate::ChannelSet {
                organization,
                channel,
            } => {
                if let Some(channels) = entry.channels_mut(organization.as_deref()) {
                    set(channels, channel, |c| &c.id);
                }
            }
            DirectoryUpdate::ChannelRemoved { organization, id } => {
                if let Some(channels) = entry.channels_mut(organization.as_deref()) {
                    channels.retain(|c| c.id != id);
                }
            }
        }
        if entry.organizations.is_empty() && entry.channels.is_empty() {
            self.providers.remove(provider);
        }
    }

    /// Forgets everything `provider` reported, such as once it has exited.
    pub fn remove_provider(&mut self, provider: &str) {
        self.providers.remove(provider);
    }

    /// The provider's organizations, ordered by id.
    pub fn organizations(&self, provider: &str) -> impl Iterator<Item = &Organization> {
        self.providers
            .get(provider)
            .into_iter()
            .flat_map(|entry| entry.organizations.values())
    }

    pub fn organization(&self, provider: &str, id: &str) -> Option<&Organization> {
        self.providers.get(provider)?.organizations.get(id)
    }

    pub fn user(&self, provider: &str, organization: &str, id: &str) -> Option<&User> {
        self.organization(provider, organization)?
            .users
            .iter()
            .find(|u| u.id == id)
    }

    /// The channels in `organization`, or outside any organization when it is
    /// `None`.
    pub fn channels(&self, provider: &str, organization: Option<&str>) -> &[Channel] {
        let Some(entry) = self.providers.get(provider) else {
            return &[];
        };
        match organization {
            Some(id) => entry
                .organizations
                .get(id)
                .map_or(&[], |o| o.channels.as_slice()),
            None => &entry.channels,
        }
    }

    pub fn channel(
        &self,
        provider: &str,
        organization: Option<&str>,
        id: &str,
    ) -> Option<&Channel> {
        self.channels(provider, organization)
            .iter()
            .find(|c| c.id == id)
    }
}

impl Entry {
    fn channels_mut(&mut self, organization: Option<&str>) -> Option<&mut Vec<Channel>> {
        match organization {
            Some(id) => self.organizations.get_mut(id).map(|o| &mut o.channels),
            None => Some(&mut self.channels),
        }
    }
}

/// Replaces the item with `value`'s id, or adds `value` if there is none.
fn set<T>(items: &mut Vec<T>, value: T, id: impl Fn(&T) -> &String) {
    match items.iter_mut().find(|item| id(item) == id(&value)) {
        Some(item) => *item = value,
        None => items.push(value),
    }
}

#[cfg(test)]
mod tests {
    use qmsg_types::{ChannelKind, ContentKind};

    use super::*;

    fn user(id: &str, name: &str) -> User {
        User {
            id: id.into(),
            name: name.into(),
        }
    }

    fn channel(id: &str, kind: ChannelKind, inputs: Vec<ContentKind>) -> Channel {
        Channel {
            id: id.into(),
            name: id.into(),
            kind,
            inputs,
        }
    }

    fn organization() -> Organization {
        Organization {
            id: "org".into(),
            name: "Org".into(),
            users: vec![user("ana", "Ana")],
            channels: vec![channel(
                "general",
                ChannelKind::Text,
                vec![ContentKind::Text, ContentKind::Image],
            )],
        }
    }

    #[test]
    fn updates_users_and_channels() {
        let mut directory = Directory::new();
        directory.apply("p", DirectoryUpdate::OrganizationSet(organization()));

        directory.apply(
            "p",
            DirectoryUpdate::UserSet {
                organization: "org".into(),
                user: user("ana", "Ana B"),
            },
        );
        directory.apply(
            "p",
            DirectoryUpdate::UserSet {
                organization: "org".into(),
                user: user("bo", "Bo"),
            },
        );
        directory.apply(
            "p",
            DirectoryUpdate::ChannelSet {
                organization: Some("org".into()),
                channel: channel("lounge", ChannelKind::Voice, vec![]),
            },
        );
        directory.apply(
            "p",
            DirectoryUpdate::ChannelRemoved {
                organization: Some("org".into()),
                id: "general".into(),
            },
        );

        let org = directory.organization("p", "org").unwrap();
        assert_eq!(org.users, [user("ana", "Ana B"), user("bo", "Bo")]);
        assert_eq!(
            org.channels,
            [channel("lounge", ChannelKind::Voice, vec![])]
        );
        assert_eq!(directory.user("p", "org", "bo"), Some(&user("bo", "Bo")));
    }

    #[test]
    fn keeps_channels_outside_organizations() {
        let mut directory = Directory::new();
        let dm = channel("dm-ana", ChannelKind::Direct, vec![ContentKind::Text]);
        directory.apply(
            "p",
            DirectoryUpdate::ChannelSet {
                organization: None,
                channel: dm.clone(),
            },
        );
        assert_eq!(directory.organizations("p").count(), 0);
        assert_eq!(directory.channels("p", None), std::slice::from_ref(&dm));
        assert_eq!(directory.channel("p", None, "dm-ana"), Some(&dm));
        // A channel with the same id in an organization is a different one.
        assert_eq!(directory.channel("p", Some("org"), "dm-ana"), None);

        directory.apply(
            "p",
            DirectoryUpdate::ChannelRemoved {
                organization: None,
                id: "dm-ana".into(),
            },
        );
        assert!(directory.channels("p", None).is_empty());
    }

    #[test]
    fn organizations_are_kept_per_provider() {
        let mut directory = Directory::new();
        directory.apply("a", DirectoryUpdate::OrganizationSet(organization()));
        directory.apply("b", DirectoryUpdate::OrganizationSet(organization()));

        directory.apply(
            "a",
            DirectoryUpdate::OrganizationRemoved { id: "org".into() },
        );
        assert!(directory.organization("a", "org").is_none());
        assert!(directory.organization("b", "org").is_some());

        directory.remove_provider("b");
        assert_eq!(directory.organizations("b").count(), 0);
    }

    #[test]
    fn ignores_updates_to_unknown_organizations() {
        let mut directory = Directory::new();
        directory.apply(
            "p",
            DirectoryUpdate::UserSet {
                organization: "org".into(),
                user: user("ana", "Ana"),
            },
        );
        assert_eq!(directory.organizations("p").count(), 0);
    }
}

use std::{fmt::Display, num::ParseIntError, str::FromStr};

use reqwest::header::HeaderValue;
/// An incrementing serial used to identify queries from specific client
/// versions. Needed for mainting backwards compatibility with older clients
/// but supporting new features for new versions.
pub struct ClientVersion(u32);

impl ClientVersion {
    /// Whether or not the client supports reading responses in chunked capnp format.
    /// (see capnp schema for hypersync_net_types.capnp). All versions that send this
    /// header above 1 support chunked responses.
    pub fn supports_chunked_response(&self) -> bool {
        self.0 >= 1
    }

    /// For internally adding headers to the http client.
    /// Noting: X- prefix is no longer best practice since: https://datatracker.ietf.org/doc/html/rfc6648
    pub(crate) const HEADER_KEY: &'static str = "Hypersync-Client-Version";
    // Start with 5 for being able to cherry pick to the older client with a lower version
    pub(crate) const CURRENT: ClientVersion = ClientVersion(5);
    pub(crate) fn header_value(&self) -> HeaderValue {
        self.0.into()
    }
}

impl From<u32> for ClientVersion {
    fn from(version: u32) -> Self {
        Self(version)
    }
}

impl From<ClientVersion> for u32 {
    fn from(version: ClientVersion) -> Self {
        version.0
    }
}

impl FromStr for ClientVersion {
    type Err = ParseIntError;

    fn from_str(version: &str) -> Result<Self, Self::Err> {
        let version = u32::from_str(version)?;
        Ok(ClientVersion(version))
    }
}

impl Display for ClientVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

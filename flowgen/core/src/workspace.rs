//! The layout of a workspace: the folders flowgen reads flows and resources from.

/// The folder of a workspace that holds flows.
pub const FLOWS_DIR: &str = "flows";

/// The folder of a workspace that holds resources.
pub const RESOURCES_DIR: &str = "resources";

/// The folder of a workspace that holds the credentials its flows read.
pub const CREDENTIALS_DIR: &str = "credentials";

/// The folder of a workspace that holds local state, kept out of git.
pub const STATE_DIR: &str = ".flowgen";

/// The local cache file, in [`STATE_DIR`].
pub const CACHE_FILE: &str = "state.redb";

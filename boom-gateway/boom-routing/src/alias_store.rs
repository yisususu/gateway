use dashmap::DashMap;
use dashmap::DashSet;

/// In-memory store for model aliases.
/// Survives config reloads — updated incrementally via DB or YAML seed.
#[derive(Debug)]
pub struct AliasStore {
    /// alias_name → target_model_name.
    aliases: DashMap<String, String>,
    /// Alias names that should NOT appear in model list.
    hidden: DashSet<String>,
}

impl AliasStore {
    pub fn new() -> Self {
        Self {
            aliases: DashMap::new(),
            hidden: DashSet::new(),
        }
    }

    /// Set or update an alias mapping.
    pub fn set_alias(&self, alias: String, target: String, is_hidden: bool) {
        self.aliases.insert(alias.clone(), target);
        if is_hidden {
            self.hidden.insert(alias);
        } else {
            self.hidden.remove(&alias);
        }
    }

    /// Remove an alias. Returns true if it existed.
    pub fn remove_alias(&self, alias: &str) -> bool {
        self.hidden.remove(alias);
        self.aliases.remove(alias).is_some()
    }

    /// Resolve a model name through alias mapping.
    /// Returns the original name if not an alias.
    pub fn resolve(&self, name: &str) -> Option<String> {
        self.aliases.get(name).map(|r| r.value().clone())
    }

    /// Check if an alias name is hidden.
    pub fn is_hidden(&self, alias: &str) -> bool {
        self.hidden.contains(alias)
    }

    /// Get all alias → target mappings.
    pub fn all_aliases(&self) -> Vec<(String, String)> {
        self.aliases
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    /// Get all model names visible to clients (non-hidden aliases).
    pub fn visible_names(&self) -> Vec<String> {
        self.aliases
            .iter()
            .filter(|r| !self.hidden.contains(r.key()))
            .map(|r| r.key().clone())
            .collect()
    }

    /// Clear all aliases.
    pub fn clear(&self) {
        self.aliases.clear();
        self.hidden.clear();
    }

    /// Number of aliases.
    pub fn len(&self) -> usize {
        self.aliases.len()
    }

    /// Number of hidden aliases.
    pub fn hidden_count(&self) -> usize {
        self.hidden.len()
    }
}

impl Default for AliasStore {
    fn default() -> Self {
        Self::new()
    }
}

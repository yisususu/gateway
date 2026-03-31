use boom_core::provider::Provider;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// In-memory store for model deployments.
/// Survives config reloads — updated incrementally via DB or YAML seed.
pub struct DeploymentStore {
    /// model_name → list of provider deployments.
    deployments: DashMap<String, Vec<Arc<dyn Provider>>>,
    /// model_name → round-robin counter.
    rr_counters: DashMap<String, AtomicUsize>,
}

impl DeploymentStore {
    pub fn new() -> Self {
        Self {
            deployments: DashMap::new(),
            rr_counters: DashMap::new(),
        }
    }

    /// Replace all deployments for a model name.
    pub fn set_deployments(&self, model_name: String, providers: Vec<Arc<dyn Provider>>) {
        self.rr_counters.remove(&model_name);
        self.deployments.insert(model_name, providers);
    }

    /// Add a single deployment to an existing model group (or create it).
    pub fn add_deployment(&self, model_name: &str, provider: Arc<dyn Provider>) {
        self.deployments
            .entry(model_name.to_string())
            .or_default()
            .push(provider);
    }

    /// Remove all deployments for the given model name.
    /// Returns true if the model existed.
    pub fn remove_deployments(&self, model_name: &str) -> bool {
        self.rr_counters.remove(model_name);
        self.deployments.remove(model_name).is_some()
    }

    /// Clear all deployments (used before full reload).
    pub fn clear(&self) {
        self.rr_counters.clear();
        self.deployments.clear();
    }

    /// Select a provider via round-robin for the given model.
    /// Returns None if no deployments exist.
    pub fn select(&self, model_name: &str) -> Option<Arc<dyn Provider>> {
        let providers = self.deployments.get(model_name)?;
        if providers.is_empty() {
            return None;
        }
        if providers.len() == 1 {
            return Some(providers[0].clone());
        }

        let counter = self
            .rr_counters
            .entry(model_name.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        let idx = counter.fetch_add(1, Ordering::Relaxed);
        Some(providers[idx % providers.len()].clone())
    }

    /// Get all model names (deployment keys).
    pub fn model_names(&self) -> Vec<String> {
        self.deployments.iter().map(|r| r.key().clone()).collect()
    }

    /// Check if a model name has deployments.
    pub fn contains(&self, model_name: &str) -> bool {
        self.deployments.contains_key(model_name)
    }

    /// Get the number of deployments for a model.
    pub fn deployment_count(&self, model_name: &str) -> usize {
        self.deployments
            .get(model_name)
            .map(|r| r.value().len())
            .unwrap_or(0)
    }

    /// Total number of unique model names.
    pub fn len(&self) -> usize {
        self.deployments.len()
    }

    /// Total deployment count across all models.
    pub fn total_deployments(&self) -> usize {
        self.deployments
            .iter()
            .map(|r| r.value().len())
            .sum()
    }
}

impl Default for DeploymentStore {
    fn default() -> Self {
        Self::new()
    }
}

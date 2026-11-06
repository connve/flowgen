//! Token and amount budgets of `ai_completion`, counted in the system cache.

use flowgen_core::cache::{Cache, CacheError};
use rig_agent::agent::{
    AgentHook, CompletionCallAction, CompletionCallEvent, HookContext, ModelTurnAction,
    ModelTurnFinished,
};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

/// Limits on what every `ai_completion` uses, and the prices amount limits need.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetConfig {
    /// Limits, each counted across every model.
    #[serde(default)]
    pub limits: Vec<Limit>,
    /// Price of each model, by the name the completion calls it with.
    #[serde(default)]
    pub prices: HashMap<String, Price>,
}

/// A limit per calendar period in UTC, in tokens or in the prices' currency.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Limit {
    /// Whose use the limit counts.
    pub scope: Scope,
    /// The period after which the count starts over.
    pub period: Period,
    /// Tokens the scope uses per period.
    pub max_tokens: Option<u64>,
    /// Amount, at `prices`, the scope spends per period.
    pub max_amount: Option<f64>,
}

/// Whose use a limit counts.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Every flow of the workspace together.
    Workspace,
    /// Each signed-in user, across flows.
    User,
}

/// A calendar period in UTC.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Period {
    Day,
    Week,
    Month,
}

/// Price of a model per million tokens.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    /// Per million prompt tokens.
    pub input: f64,
    /// Per million completion tokens.
    pub output: f64,
}

/// Tokens a completion used.
#[derive(Clone, Copy, Debug)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

/// Errors of the budget.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Limit {index} of ai_budget must set exactly one of max_tokens and max_amount")]
    InvalidLimit { index: usize },
    #[error("Limit {index} of ai_budget counts what limit {first} counts")]
    DuplicateLimit { index: usize, first: usize },
    #[error("The {scope:?} {period:?} budget of {limit} is used up")]
    Spent {
        scope: Scope,
        period: Period,
        limit: String,
    },
    #[error("No price in ai_budget for model '{model}', which an amount limit needs")]
    NoPrice { model: String },
    #[error("Failed to read or count the budget: {source}")]
    Cache {
        #[source]
        source: CacheError,
    },
    #[error("Budget count under '{key}' is not a number")]
    Corrupt { key: String },
}

impl Error {
    /// Whether retrying might help: only when the count could not be read.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Cache { .. })
    }
}

impl BudgetConfig {
    /// Rejects a limit with neither or both maximums, and two limits sharing one count.
    pub fn validate(&self) -> Result<(), Error> {
        for (index, limit) in self.limits.iter().enumerate() {
            if limit.max_tokens.is_some() == limit.max_amount.is_some() {
                return Err(Error::InvalidLimit { index });
            }
            let first = self.limits[..index].iter().position(|other| {
                other.scope == limit.scope
                    && other.period == limit.period
                    && other.max_tokens.is_some() == limit.max_tokens.is_some()
            });
            if let Some(first) = first {
                return Err(Error::DuplicateLimit { index, first });
            }
        }
        Ok(())
    }
}

/// Amounts are counted in millionths, so counts stay integers.
const MICROS: f64 = 1_000_000.0;

/// Days a count is kept, past the longest period.
const KEEP_DAYS: i64 = 62;

/// Counts what every `ai_completion` uses against the limits.
#[derive(Debug)]
pub struct Budget {
    config: BudgetConfig,
    cache: Arc<dyn Cache>,
}

impl Budget {
    /// Counts in `cache`, which flow scripts must not reach.
    pub fn new(config: BudgetConfig, cache: Arc<dyn Cache>) -> Self {
        Self { config, cache }
    }

    /// Fails when a limit of the workspace or of `user` is used up, or `model` has no needed price.
    pub async fn check(&self, user: Option<&str>, model: &str) -> Result<(), Error> {
        let now = chrono::Utc::now();
        for limit in self.applying(user) {
            let used = self.used(&key(limit, user, now)).await?;
            let (spent, shown) = match (limit.max_tokens, limit.max_amount) {
                (Some(max), _) => (used >= max, format!("{max} tokens")),
                (None, Some(max)) => {
                    self.price(model)?;
                    (used as f64 >= max * MICROS, format!("{max}"))
                }
                (None, None) => (false, String::new()),
            };
            if spent {
                return Err(Error::Spent {
                    scope: limit.scope,
                    period: limit.period,
                    limit: shown,
                });
            }
        }
        Ok(())
    }

    /// Adds what a completion of `model` used to the counts of the workspace and of `user`.
    pub async fn spend(&self, user: Option<&str>, model: &str, usage: Usage) -> Result<(), Error> {
        let now = chrono::Utc::now();
        for limit in self.applying(user) {
            let added = match limit.max_tokens {
                Some(_) => usage.prompt_tokens + usage.completion_tokens,
                None => {
                    let price = self.price(model)?;
                    (usage.prompt_tokens as f64 * price.input
                        + usage.completion_tokens as f64 * price.output)
                        .round() as u64
                }
            };
            self.add(&key(limit, user, now), added).await?;
        }
        Ok(())
    }

    /// The limits that count this use: the workspace's, and the user's when one is signed in.
    fn applying<'a>(&'a self, user: Option<&'a str>) -> impl Iterator<Item = &'a Limit> {
        self.config
            .limits
            .iter()
            .filter(move |limit| limit.scope == Scope::Workspace || user.is_some())
    }

    fn price(&self, model: &str) -> Result<&Price, Error> {
        match self.config.prices.get(model) {
            Some(price) => Ok(price),
            None => Err(Error::NoPrice {
                model: model.to_string(),
            }),
        }
    }

    async fn used(&self, key: &str) -> Result<u64, Error> {
        match self.cache.get(key).await {
            Ok(Some(value)) => count(key, &value),
            Ok(None) => Ok(0),
            Err(source) => Err(Error::Cache { source }),
        }
    }

    /// Adds `added` to the count under `key`, again when another writer won the race.
    async fn add(&self, key: &str, added: u64) -> Result<(), Error> {
        let keep = u64::try_from(chrono::Duration::days(KEEP_DAYS).num_seconds()).ok();
        loop {
            let written = match self.cache.get_with_revision(key).await {
                Ok(Some((value, revision))) => {
                    let used = count(key, &value)?.saturating_add(added);
                    self.cache
                        .update(key, used.to_string().into(), revision, keep)
                        .await
                }
                Ok(None) => match self.cache.get_revision(key).await {
                    Ok(Some(deleted)) => {
                        self.cache
                            .update(key, added.to_string().into(), deleted, keep)
                            .await
                    }
                    Ok(None) => self.cache.create(key, added.to_string().into(), keep).await,
                    Err(source) => return Err(Error::Cache { source }),
                },
                Err(source) => return Err(Error::Cache { source }),
            };
            match written {
                Ok(_) => return Ok(()),
                Err(
                    CacheError::RevisionMismatch { .. }
                    | CacheError::AlreadyExists
                    | CacheError::NotFound,
                ) => continue,
                Err(source) => return Err(Error::Cache { source }),
            }
        }
    }
}

/// The key of the count of `limit` in the period of `now`.
fn key(limit: &Limit, user: Option<&str>, now: chrono::DateTime<chrono::Utc>) -> String {
    let period = match limit.period {
        Period::Day => now.format("day.%Y-%m-%d"),
        Period::Week => now.format("week.%G-W%V"),
        Period::Month => now.format("month.%Y-%m"),
    };
    let scope = match (limit.scope, user) {
        (Scope::User, Some(user)) => format!("user.{:x}", sha2::Sha256::digest(user.as_bytes())),
        _ => "workspace".to_string(),
    };
    let kind = match limit.max_tokens {
        Some(_) => "tokens",
        None => "amount",
    };
    format!("ai.budget.{period}.{scope}.{kind}")
}

fn count(key: &str, value: &[u8]) -> Result<u64, Error> {
    match std::str::from_utf8(value).map(str::parse::<u64>) {
        Ok(Ok(count)) => Ok(count),
        _ => Err(Error::Corrupt {
            key: key.to_string(),
        }),
    }
}

/// Checks the budget before each model call of a run and counts what each call used.
pub struct Spending {
    budget: Arc<Budget>,
    user: Option<String>,
    model: String,
}

impl Spending {
    pub fn new(budget: Arc<Budget>, user: Option<&str>, model: &str) -> Self {
        Self {
            budget,
            user: user.map(str::to_string),
            model: model.to_string(),
        }
    }
}

impl AgentHook for Spending {
    async fn on_completion_call(
        &self,
        _ctx: &HookContext,
        _event: CompletionCallEvent<'_>,
    ) -> CompletionCallAction {
        match self.budget.check(self.user.as_deref(), &self.model).await {
            Ok(()) => CompletionCallAction::Continue,
            Err(e) => CompletionCallAction::Stop(e.to_string()),
        }
    }

    async fn on_model_turn_finished(
        &self,
        _ctx: &HookContext,
        event: ModelTurnFinished<'_>,
    ) -> ModelTurnAction {
        let usage = Usage {
            prompt_tokens: event.usage.input_tokens.unwrap_or_default(),
            completion_tokens: event.usage.output_tokens.unwrap_or_default(),
        };
        if let Err(e) = self
            .budget
            .spend(self.user.as_deref(), &self.model, usage)
            .await
        {
            warn!(error = %e, "Failed to count a model call against the budget");
        }
        ModelTurnAction::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(
        scope: Scope,
        period: Period,
        max_tokens: Option<u64>,
        max_amount: Option<f64>,
    ) -> Limit {
        Limit {
            scope,
            period,
            max_tokens,
            max_amount,
        }
    }

    fn budget(limits: Vec<Limit>) -> Arc<Budget> {
        Arc::new(Budget::new(
            BudgetConfig {
                limits,
                prices: HashMap::from([
                    (
                        "cheap".to_string(),
                        Price {
                            input: 1.0,
                            output: 2.0,
                        },
                    ),
                    (
                        "dear".to_string(),
                        Price {
                            input: 10.0,
                            output: 20.0,
                        },
                    ),
                ]),
            },
            Arc::new(flowgen_core::cache::MemoryCache::new()),
        ))
    }

    fn usage(prompt_tokens: u64, completion_tokens: u64) -> Usage {
        Usage {
            prompt_tokens,
            completion_tokens,
        }
    }

    #[tokio::test]
    async fn the_workspace_is_cut_off_once_it_used_its_tokens() {
        let budget = budget(vec![limit(Scope::Workspace, Period::Day, Some(100), None)]);

        let before = budget.check(None, "cheap").await;
        budget
            .spend(Some("a"), "cheap", usage(30, 30))
            .await
            .unwrap();
        let within = budget.check(Some("b"), "dear").await;
        budget.spend(None, "dear", usage(20, 20)).await.unwrap();
        let spent = budget.check(Some("b"), "cheap").await;

        assert!(before.is_ok());
        assert!(within.is_ok());
        assert!(matches!(
            spent,
            Err(Error::Spent {
                scope: Scope::Workspace,
                period: Period::Day,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn an_amount_limit_counts_every_model_at_its_price() {
        let budget = budget(vec![limit(
            Scope::Workspace,
            Period::Month,
            None,
            Some(0.001),
        )]);

        budget.spend(None, "cheap", usage(100, 100)).await.unwrap();
        let after_cheap = budget.check(None, "cheap").await;
        budget.spend(None, "dear", usage(30, 20)).await.unwrap();
        let after_dear = budget.check(None, "cheap").await;

        assert!(after_cheap.is_ok());
        assert!(matches!(after_dear, Err(Error::Spent { .. })));
    }

    #[tokio::test]
    async fn a_model_without_a_price_fails_under_an_amount_limit() {
        let budget = budget(vec![limit(
            Scope::Workspace,
            Period::Month,
            None,
            Some(1.0),
        )]);

        let checked = budget.check(None, "unknown").await;

        assert!(matches!(checked, Err(Error::NoPrice { .. })));
    }

    #[tokio::test]
    async fn a_user_is_cut_off_without_cutting_off_the_others() {
        let budget = budget(vec![limit(Scope::User, Period::Week, Some(50), None)]);

        budget
            .spend(Some("a"), "cheap", usage(25, 25))
            .await
            .unwrap();
        let a = budget.check(Some("a"), "cheap").await;
        let b = budget.check(Some("b"), "cheap").await;
        let anonymous = budget.check(None, "cheap").await;

        assert!(matches!(
            a,
            Err(Error::Spent {
                scope: Scope::User,
                ..
            })
        ));
        assert!(b.is_ok());
        assert!(anonymous.is_ok());
    }

    #[test]
    fn a_limit_sets_exactly_one_maximum() {
        let config = |max_tokens, max_amount| BudgetConfig {
            limits: vec![limit(Scope::Workspace, Period::Day, max_tokens, max_amount)],
            prices: HashMap::new(),
        };

        assert!(config(Some(1), None).validate().is_ok());
        assert!(config(None, Some(1.0)).validate().is_ok());
        assert!(matches!(
            config(None, None).validate(),
            Err(Error::InvalidLimit { index: 0 })
        ));
        assert!(config(Some(1), Some(1.0)).validate().is_err());
    }

    #[test]
    fn two_limits_of_one_scope_period_and_kind_are_rejected() {
        let config = BudgetConfig {
            limits: vec![
                limit(Scope::Workspace, Period::Day, Some(1), None),
                limit(Scope::Workspace, Period::Month, Some(1), None),
                limit(Scope::Workspace, Period::Day, Some(2), None),
            ],
            prices: HashMap::new(),
        };

        assert!(matches!(
            config.validate(),
            Err(Error::DuplicateLimit { index: 2, first: 0 })
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_spends_all_count() {
        let budget = budget(vec![limit(
            Scope::Workspace,
            Period::Day,
            Some(1_000),
            None,
        )]);

        let spends: Vec<_> = (0..50)
            .map(|_| {
                let budget = Arc::clone(&budget);
                tokio::spawn(async move { budget.spend(None, "cheap", usage(1, 1)).await })
            })
            .collect();
        for spend in spends {
            spend.await.unwrap().unwrap();
        }

        let key = key(&budget.config.limits[0], None, chrono::Utc::now());
        assert_eq!(budget.used(&key).await.unwrap(), 100);
    }
}

//! Binding `:name` parameters into a SOQL query as literals, so a value
//! can't change the query around it.

use salesforce_core::chrono;
use salesforce_core::soql;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Errors binding parameters into a query.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("The query uses :{name}, which isn't in `parameters`")]
    UnknownParameter { name: String },
    #[error("Parameter `{name}` isn't a date (YYYY-MM-DD): {source}")]
    InvalidDate {
        name: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("Parameter `{name}` isn't an RFC 3339 datetime: {source}")]
    InvalidDatetime {
        name: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("Parameter `{name}` is an empty list, which SOQL can't compare against")]
    EmptyList { name: String },
    #[error("Parameter `{name}` must be a string, number, boolean, null, or a list of them")]
    UnsupportedValue { name: String },
}

/// A value bound to `:name` in a SOQL query.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Parameter {
    /// A date such as `2024-01-15`, bound unquoted.
    Date { date: String },
    /// An RFC 3339 datetime, bound unquoted in UTC.
    Datetime { datetime: String },
    /// A `LIKE` pattern matching values that contain the string, with `%`
    /// and `_` in it matched literally.
    Contains { contains: String },
    /// A `LIKE` pattern matching values that start with the string.
    StartsWith { starts_with: String },
    /// A `LIKE` pattern matching values that end with the string.
    EndsWith { ends_with: String },
    /// A string (bound quoted and escaped), number, boolean, null, or a list
    /// of them for `IN`.
    Value(serde_json::Value),
}

/// Replaces every `:name` outside a string literal with its parameter as a
/// SOQL literal.
pub fn bind(query: &str, parameters: &HashMap<String, Parameter>) -> Result<String, Error> {
    let mut bound = String::with_capacity(query.len());
    let mut in_literal = false;
    let mut chars = query.char_indices();
    while let Some((at, ch)) = chars.next() {
        match (in_literal, ch) {
            (true, '\\') => {
                bound.push(ch);
                if let Some((_, escaped)) = chars.next() {
                    bound.push(escaped);
                }
            }
            (_, '\'') => {
                in_literal = !in_literal;
                bound.push(ch);
            }
            (false, ':') => match identifier(&query[at + 1..]) {
                Some(name) => {
                    let parameter =
                        parameters
                            .get(name)
                            .ok_or_else(|| Error::UnknownParameter {
                                name: name.to_string(),
                            })?;
                    bound.push_str(&literal(name, parameter)?);
                    chars.nth(name.len() - 1);
                }
                None => bound.push(ch),
            },
            _ => bound.push(ch),
        }
    }
    Ok(bound)
}

/// The parameter name at the start of `rest`: a letter or underscore, then
/// letters, digits, or underscores. `LAST_N_DAYS:7` has none.
fn identifier(rest: &str) -> Option<&str> {
    let end = rest
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .unwrap_or(rest.len());
    let name = &rest[..end];
    match name.chars().next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => Some(name),
        _ => None,
    }
}

fn literal(name: &str, parameter: &Parameter) -> Result<String, Error> {
    match parameter {
        Parameter::Date { date } => chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .map(soql::date)
            .map_err(|source| Error::InvalidDate {
                name: name.to_string(),
                source,
            }),
        Parameter::Datetime { datetime } => chrono::DateTime::parse_from_rfc3339(datetime)
            .map(|datetime| soql::datetime(datetime.to_utc()))
            .map_err(|source| Error::InvalidDatetime {
                name: name.to_string(),
                source,
            }),
        Parameter::Contains { contains } => Ok(soql::like_contains(contains)),
        Parameter::StartsWith { starts_with } => Ok(soql::like_starts_with(starts_with)),
        Parameter::EndsWith { ends_with } => Ok(soql::like_ends_with(ends_with)),
        Parameter::Value(serde_json::Value::Array(items)) if items.is_empty() => {
            Err(Error::EmptyList {
                name: name.to_string(),
            })
        }
        Parameter::Value(serde_json::Value::Array(items)) => {
            let items = items
                .iter()
                .map(|item| scalar(name, item))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", items.join(", ")))
        }
        Parameter::Value(value) => scalar(name, value),
    }
}

fn scalar(name: &str, value: &serde_json::Value) -> Result<String, Error> {
    match value {
        serde_json::Value::String(text) => Ok(soql::quote(text)),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::Bool(flag) => Ok(flag.to_string()),
        serde_json::Value::Null => Ok("null".to_string()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(Error::UnsupportedValue {
                name: name.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parameters(entries: &[(&str, Parameter)]) -> HashMap<String, Parameter> {
        entries
            .iter()
            .map(|(name, parameter)| (name.to_string(), parameter.clone()))
            .collect()
    }

    fn text(value: &str) -> Parameter {
        Parameter::Value(serde_json::Value::String(value.to_string()))
    }

    #[test]
    fn a_string_is_quoted_so_it_cannot_widen_the_query() {
        let bound = bind(
            "SELECT Id FROM Contact WHERE Email = :email",
            &parameters(&[("email", text("x' OR Email != '"))]),
        )
        .unwrap();
        assert_eq!(
            bound,
            r"SELECT Id FROM Contact WHERE Email = 'x\' OR Email != \''"
        );
    }

    #[test]
    fn like_patterns_match_wildcards_in_the_value_literally() {
        let bound = bind(
            "WHERE Name LIKE :name AND Code LIKE :code AND Email LIKE :domain",
            &parameters(&[
                (
                    "name",
                    Parameter::Contains {
                        contains: "50%_off".to_string(),
                    },
                ),
                (
                    "code",
                    Parameter::StartsWith {
                        starts_with: "A'B".to_string(),
                    },
                ),
                (
                    "domain",
                    Parameter::EndsWith {
                        ends_with: "@example.com".to_string(),
                    },
                ),
            ]),
        )
        .unwrap();
        assert_eq!(
            bound,
            r"WHERE Name LIKE '%50\%\_off%' AND Code LIKE 'A\'B%' AND Email LIKE '%@example.com'"
        );
    }

    #[test]
    fn numbers_booleans_and_null_are_bound_unquoted() {
        let bound = bind(
            "WHERE Amount > :amount AND IsWon = :won AND CloseDate = :none",
            &parameters(&[
                ("amount", Parameter::Value(serde_json::Value::from(1500.5))),
                ("won", Parameter::Value(serde_json::Value::Bool(true))),
                ("none", Parameter::Value(serde_json::Value::Null)),
            ]),
        )
        .unwrap();
        assert_eq!(
            bound,
            "WHERE Amount > 1500.5 AND IsWon = true AND CloseDate = null"
        );
    }

    #[test]
    fn a_list_is_bound_for_in() {
        let ids = Parameter::Value(serde_json::Value::Array(vec![
            serde_json::Value::from("001A"),
            serde_json::Value::from("001'B"),
        ]));
        assert_eq!(
            bind("WHERE Id IN :ids", &parameters(&[("ids", ids)])).unwrap(),
            r"WHERE Id IN ('001A', '001\'B')"
        );
    }

    #[test]
    fn dates_and_datetimes_are_validated_and_bound_unquoted() {
        let bound = bind(
            "WHERE CloseDate = :day AND LastModifiedDate > :since",
            &parameters(&[
                (
                    "day",
                    Parameter::Date {
                        date: "2024-01-15".to_string(),
                    },
                ),
                (
                    "since",
                    Parameter::Datetime {
                        datetime: "2024-01-15T10:30:00.250+02:00".to_string(),
                    },
                ),
            ]),
        )
        .unwrap();
        assert_eq!(
            bound,
            "WHERE CloseDate = 2024-01-15 AND LastModifiedDate > 2024-01-15T08:30:00Z"
        );
        let injected = Parameter::Datetime {
            datetime: "2024-01-15T10:30:00Z OR Id != null".to_string(),
        };
        assert!(matches!(
            bind("WHERE X > :since", &parameters(&[("since", injected)])),
            Err(Error::InvalidDatetime { .. })
        ));
    }

    #[test]
    fn colons_in_literals_and_date_literals_are_left_alone() {
        let query = "WHERE Name = 'a:b' AND Note = 'it\\'s :x' AND CreatedDate = LAST_N_DAYS:7";
        assert_eq!(bind(query, &HashMap::new()).unwrap(), query);
    }

    #[test]
    fn an_unknown_parameter_fails_before_the_query_is_sent() {
        assert!(matches!(
            bind("WHERE Id = :id", &HashMap::new()),
            Err(Error::UnknownParameter { name }) if name == "id"
        ));
    }

    #[test]
    fn an_empty_list_or_an_object_is_rejected() {
        let empty = Parameter::Value(serde_json::Value::Array(Vec::new()));
        let object = Parameter::Value(serde_json::Value::Object(serde_json::Map::new()));
        assert!(matches!(
            bind("IN :ids", &parameters(&[("ids", empty)])),
            Err(Error::EmptyList { .. })
        ));
        assert!(matches!(
            bind("= :x", &parameters(&[("x", object)])),
            Err(Error::UnsupportedValue { .. })
        ));
    }

    #[test]
    fn parameters_deserialize_from_yaml_shapes() {
        let parsed: HashMap<String, Parameter> = serde_json::from_str(
            r#"{"email": "a@b.c", "day": {"date": "2024-01-15"}, "since": {"datetime": "2024-01-15T00:00:00Z"}, "name": {"contains": "a"}, "code": {"starts_with": "a"}, "domain": {"ends_with": "a"}, "ids": ["a"]}"#,
        )
        .unwrap();
        assert_eq!(parsed["email"], text("a@b.c"));
        assert!(matches!(parsed["day"], Parameter::Date { .. }));
        assert!(matches!(parsed["since"], Parameter::Datetime { .. }));
        assert!(matches!(parsed["name"], Parameter::Contains { .. }));
        assert!(matches!(parsed["code"], Parameter::StartsWith { .. }));
        assert!(matches!(parsed["domain"], Parameter::EndsWith { .. }));
        assert!(matches!(
            parsed["ids"],
            Parameter::Value(serde_json::Value::Array(_))
        ));
    }
}

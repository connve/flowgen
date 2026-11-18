//! Runs a Rhai script on sample events the way the `script` task runs it,
//! with its limits, helpers, module imports and an in-memory `ctx.cache`, so
//! a script is checked before it reaches an environment.

use super::{ProcessorBuilder, Script};
use crate::event::{EventBuilder, EventData};
use crate::task::runner::{Handler, Runner};
use crate::workspace::RESOURCES_DIR;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The extension of the scripts `flowgen test` runs.
const SCRIPT_EXTENSION: &str = "rhai";

/// Sample files holding one event per line.
const LINE_EXTENSIONS: [&str; 2] = ["ndjson", "jsonl"];

/// Sample files holding one event.
const EVENT_EXTENSIONS: [&str; 3] = ["json", "yaml", "yml"];

/// The task name and type the sample events and the script run under.
const TEST_NAME: &str = "test";

/// Failure to set up a test run.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Only Rhai scripts can be tested yet, and {path} is not a .rhai file")]
    NotScript { path: String },
    #[error("Failed to read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not valid JSON or YAML: {reason}")]
    Parse { path: String, reason: String },
    #[error("No sample events in {path}; use .json, .yaml, .yml, .ndjson or .jsonl files")]
    NoSamples { path: String },
    #[error("Failed to start the script: {0}")]
    Start(String),
}

/// One sample event and what the script made of it.
pub struct Outcome {
    /// The sample file, with `:<line>` for a file of one event per line.
    pub sample: String,
    /// The events the script emitted, or why it failed.
    pub result: Result<Vec<serde_json::Value>, String>,
}

/// Runs the script at `script` on every sample event in `data`, a file or a
/// folder of `.json`, `.yaml` or `.yml` files (one event each) and `.ndjson`
/// or `.jsonl` files (one event per line). Each sample is `event.data`.
pub async fn run(script: &Path, data: &Path) -> Result<Vec<Outcome>, Error> {
    let extension = script.extension().and_then(|extension| extension.to_str());
    if extension != Some(SCRIPT_EXTENSION) {
        return Err(Error::NotScript {
            path: script.display().to_string(),
        });
    }
    let samples = samples(data)?;
    if samples.is_empty() {
        return Err(Error::NoSamples {
            path: data.display().to_string(),
        });
    }
    let (resources, key) = locate(script);
    run_on(
        crate::resource::Source::Resource { resource: key },
        samples,
        crate::resource::ResourceLoader::new(Some(resources)),
    )
    .await
}

/// Runs `code` on every sample, a name and its `event.data`, with `resources`
/// for the scripts it imports.
pub async fn run_on(
    code: crate::resource::Source,
    samples: Vec<(String, serde_json::Value)>,
    resources: crate::resource::ResourceLoader,
) -> Result<Vec<Outcome>, Error> {
    let task_manager = crate::task::manager::TaskManagerBuilder::new()
        .build()
        .map_err(|source| Error::Start(source.to_string()))?;
    let cache = Arc::new(crate::cache::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
    let context = crate::task::context::TaskContextBuilder::new()
        .flow_name(TEST_NAME.to_string())
        .task_manager(Arc::new(task_manager))
        .cache(cache)
        .resource_loader(Some(resources))
        .build()
        .map_err(|source| Error::Start(source.to_string()))?;
    let (tx, mut emitted) = tokio::sync::mpsc::channel(1024);
    let (_input, rx) = tokio::sync::mpsc::channel(1);
    let processor = ProcessorBuilder::new()
        .config(Arc::new(Script {
            name: TEST_NAME.to_string(),
            code,
            ..Script::default()
        }))
        .receiver(rx)
        .sender(tx)
        .task_type(TEST_NAME)
        .task_context(Arc::new(context))
        .build()
        .await
        .map_err(|source| Error::Start(source.to_string()))?;
    let handler = processor
        .init()
        .await
        .map_err(|source| Error::Start(source.to_string()))?;

    let mut outcomes = Vec::with_capacity(samples.len());
    for (sample, data) in samples {
        let event = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(TEST_NAME.to_string())
            .task_id(0)
            .task_type(TEST_NAME)
            .build()
            .map_err(|source| Error::Start(source.to_string()))?;
        let handled = handler.handle(event);
        tokio::pin!(handled);
        let mut events = Vec::new();
        let result = loop {
            tokio::select! {
                Some(event) = emitted.recv() => events.push(event),
                result = &mut handled => break result,
            }
        };
        while let Ok(event) = emitted.try_recv() {
            events.push(event);
        }
        let result = match result {
            Ok(()) => events
                .iter()
                .map(|event| event.data_as_json().map_err(|source| source.to_string()))
                .collect(),
            Err(error) => Err(error.to_string()),
        };
        outcomes.push(Outcome { sample, result });
    }
    Ok(outcomes)
}

/// The resources folder the script's imports resolve in, and the script's key
/// in it: the nearest `resources/` above the script, else its own folder.
fn locate(script: &Path) -> (PathBuf, String) {
    let found = script
        .ancestors()
        .skip(1)
        .find(|folder| folder.file_name().and_then(|name| name.to_str()) == Some(RESOURCES_DIR));
    let base = match found {
        Some(folder) => folder.to_path_buf(),
        None => match script.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        },
    };
    let key = match script.strip_prefix(&base) {
        Ok(key) => key.to_string_lossy().replace('\\', "/"),
        Err(_) => script.to_string_lossy().into_owned(),
    };
    (base, key)
}

/// The sample events in `data`, a file or a folder, in file name order.
fn samples(data: &Path) -> Result<Vec<(String, serde_json::Value)>, Error> {
    let read = |path: &Path| {
        std::fs::read_to_string(path).map_err(|source| Error::Read {
            path: path.display().to_string(),
            source,
        })
    };
    let files = match data.is_dir() {
        true => {
            let mut files: Vec<PathBuf> = std::fs::read_dir(data)
                .map_err(|source| Error::Read {
                    path: data.display().to_string(),
                    source,
                })?
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.is_file())
                .collect();
            files.sort();
            files
        }
        false => vec![data.to_path_buf()],
    };
    let mut samples = Vec::new();
    for file in files {
        let name = file.display().to_string();
        match file.extension().and_then(|extension| extension.to_str()) {
            Some(extension) if LINE_EXTENSIONS.contains(&extension) => {
                for (index, line) in read(&file)?.lines().enumerate() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    samples.push((format!("{name}:{}", index + 1), parse(&name, line)?));
                }
            }
            Some(extension) if EVENT_EXTENSIONS.contains(&extension) => {
                samples.push((name.clone(), parse(&name, &read(&file)?)?));
            }
            _ => {}
        }
    }
    Ok(samples)
}

/// `content` as JSON, or as YAML when it is not JSON.
fn parse(path: &str, content: &str) -> Result<serde_json::Value, Error> {
    match serde_json::from_str(content) {
        Ok(value) => Ok(value),
        Err(_) => serde_yaml::from_str(content).map_err(|source| Error::Parse {
            path: path.to_string(),
            reason: source.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_script_runs_on_each_sample_with_the_imports_of_its_resources() {
        let dir = tempfile::tempdir().unwrap();
        let resources = dir.path().join(RESOURCES_DIR);
        std::fs::create_dir_all(resources.join("scripts")).unwrap();
        std::fs::write(
            resources.join("scripts/double.rhai"),
            "fn double(x) { x * 2 }",
        )
        .unwrap();
        let script = resources.join("scripts/a.rhai");
        std::fs::write(
            &script,
            "import \"scripts/double\" as d;\nif event.data.n < 0 { throw \"negative\"; }\nevent.data.n = d::double(event.data.n);\nevent",
        )
        .unwrap();
        let samples = dir.path().join("samples");
        std::fs::create_dir_all(&samples).unwrap();
        std::fs::write(samples.join("a.json"), r#"{"n": 2}"#).unwrap();
        std::fs::write(samples.join("b.ndjson"), "{\"n\": 3}\n\n{\"n\": -1}\n").unwrap();
        std::fs::write(samples.join("c.yaml"), "n: 5\n").unwrap();
        std::fs::write(samples.join("notes.txt"), "skipped").unwrap();

        let outcomes = run(&script, &samples).await.unwrap();

        let results: Vec<_> = outcomes
            .iter()
            .map(|outcome| match &outcome.result {
                Ok(events) => events
                    .iter()
                    .map(|event| event["n"].to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                Err(error) => error.clone(),
            })
            .collect();
        assert_eq!(results, vec!["4", "6", "negative", "10"]);
        assert!(outcomes[2].sample.ends_with("b.ndjson:3"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sample_may_emit_more_events_than_the_channel_holds() {
        let code = "let out = [];\nfor n in 0..event.data.count { out.push(#{ n: n }); }\nout";
        let sample = |count: i64| {
            let mut data = serde_json::Map::new();
            data.insert("count".to_string(), count.into());
            ("a".to_string(), serde_json::Value::Object(data))
        };

        let outcomes = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_on(
                crate::resource::Source::Inline(code.to_string()),
                vec![sample(3000), sample(2)],
                crate::resource::ResourceLoader::new(None),
            ),
        )
        .await
        .expect("The run finishes")
        .unwrap();

        let counts: Vec<usize> = outcomes
            .iter()
            .map(|outcome| outcome.result.as_ref().unwrap().len())
            .collect();
        assert_eq!(
            counts,
            vec![3000, 2],
            "Each sample gets only its own events"
        );
    }

    #[tokio::test]
    async fn only_rhai_scripts_and_real_samples_are_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let flow = dir.path().join("a.yaml");
        std::fs::write(&flow, "flow: {}").unwrap();
        let script = dir.path().join("a.rhai");
        std::fs::write(&script, "event").unwrap();
        let empty = dir.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();

        assert!(matches!(
            run(&flow, &empty).await,
            Err(Error::NotScript { .. })
        ));
        assert!(matches!(
            run(&script, &empty).await,
            Err(Error::NoSamples { .. })
        ));
    }
}

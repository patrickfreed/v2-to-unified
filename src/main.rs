use anyhow::Result;
use regex::Regex;

use std::{fs::File, io::Write, path::PathBuf};

use bson::{doc, Document};
use serde::Deserialize;

use crate::{
    crud_v2::TestData,
    unified::{
        ClientEntity, CollectionEntity, CreateEntity, DatabaseEntity, ExpectEvent, InitialData,
        Test,
    },
};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Serverless {
    Require,
    Forbid,
    Allow,
}

mod crud_v2 {
    use crate::unified::POOL_READY;

    use super::Serverless;
    use bson::{from_document, Bson, Document};
    use serde::{Deserialize, Deserializer};
    use std::collections::{HashMap, HashSet};

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct TestFile {
        #[serde(rename = "runOn")]
        pub(crate) run_on: Option<Vec<RunOn>>,
        pub(crate) database_name: Option<String>,
        pub(crate) collection_name: String,
        pub(crate) bucket_name: Option<String>,
        pub(crate) data: TestData,
        pub(crate) tests: Vec<Test>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    pub(crate) struct RunOn {
        pub(crate) min_server_version: Option<String>,
        pub(crate) max_server_version: Option<String>,
        pub(crate) topology: Option<Vec<String>>,
        pub(crate) serverless: Option<Serverless>,
        pub(crate) auth_enabled: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    pub(crate) enum TestData {
        Single(Vec<Document>),
        Many(HashMap<String, Vec<Document>>),
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct Test {
        pub(crate) description: String,
        pub(crate) skip_reason: Option<String>,
        pub(crate) use_multiple_mongoses: Option<bool>,
        #[serde(default, rename = "clientOptions")]
        pub(crate) client_uri: Option<Document>,
        pub(crate) fail_point: Option<Document>,
        pub(crate) session_options: Option<HashMap<String, Document>>,
        pub(crate) operations: Vec<Operation>,
        #[serde(default, deserialize_with = "deserialize_command_started_events")]
        pub(crate) expectations: Option<Vec<CommandStartedEvent>>,
        pub(crate) outcome: Option<Outcome>,
    }

    impl Test {
        pub(crate) fn observed_events(&self) -> HashSet<&'static str> {
            let mut observe_events = HashSet::new();
            if self.expectations.is_some() {
                observe_events.insert("commandStartedEvent");
            }

            for op in self.operations.iter() {
                if matches!(op.name.as_str(), "waitForEvent" | "assertEventCount") {
                    let event_name = op.arguments.as_ref().unwrap().get_str("event").unwrap();
                    let observe = match event_name {
                        "PoolClearedEvent" => crate::unified::POOL_CLEARED,
                        "PoolReadyEvent" => crate::unified::POOL_READY,
                        "ServerMarkedUnknownEvent" => crate::unified::SERVER_DESCRIPTION_CHANGED,
                        other => panic!("unexpected event: {}", other),
                    };
                    observe_events.insert(observe);
                }
            }
            observe_events
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct CommandStartedEvent {
        pub command_name: Option<String>,
        pub database_name: Option<String>,
        pub command: Document,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct Outcome {
        pub(crate) collection: CollectionOutcome,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct CollectionOutcome {
        pub(crate) name: Option<String>,
        pub(crate) data: Vec<Document>,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    pub struct Operation {
        pub name: String,
        pub object: String,
        // this field is required only for the runAdminCommand operation
        #[serde(rename = "command_name")]
        pub command_name: Option<String>,
        pub arguments: Option<Document>,
        pub error: Option<bool>,
        pub result: Option<OperationResult>,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(untagged)]
    pub enum OperationResult {
        Error(OperationError),
        Success(Bson),
    }

    #[derive(Clone, Debug, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    pub struct OperationError {
        pub error_contains: Option<String>,
        pub error_code_name: Option<String>,
        pub error_code: Option<i32>,
        pub error_labels_contain: Option<Vec<String>>,
        pub error_labels_omit: Option<Vec<String>>,
    }

    fn deserialize_command_started_events<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<Vec<CommandStartedEvent>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let docs = Vec::<Document>::deserialize(deserializer)?;
        Ok(Some(
            docs.iter()
                .map(|doc| {
                    let event = doc.get_document("command_started_event").unwrap();
                    from_document(event.clone()).unwrap()
                })
                .collect(),
        ))
    }
}

mod unified {
    use std::collections::HashSet;

    use bson::{doc, Bson, Document};
    use serde::Serialize;
    use serde_yaml::Value;

    use crate::{ADMIN_DATABASE_DEREF_PLACEHOLDER, CLIENT_DEFINITION_PLACEHOLDER, CLIENT_DEREF_PLACEHOLDER, COLLECTION_DEFINITION_PLACEHOLDER, COLLECTION_DEREF_PLACEHOLDER, COLLECTION_NAME_DEFINITION_PLACEHOLDER, COLLECTION_NAME_DEREF_PLACEHOLDER, DATABASE_DEFINITION_PLACEHOLDER, DATABASE_DEREF_PLACEHOLDER, DATABASE_NAME_DEFINITION_PLACEHOLDER, DATABASE_NAME_DEREF_PLACEHOLDER, SETUP_CLIENT_DEREF_PLACEHOLDER, TOPOLOGY_DESCRIPTION_DEFINITION_PLACEHOLDER, TOPOLOGY_DESCRIPTION_DEREF_PLACEHOLDER, crud_v2::{self, OperationResult}, thread_definition_placeholder, thread_deref_placeholder};

    pub static SERVER_DESCRIPTION_CHANGED: &'static str = "serverDescriptionChangedEvent";
    pub static POOL_CLEARED: &'static str = "poolClearedEvent";
    pub static POOL_READY: &'static str = "poolReadyEvent";

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    #[serde_with::skip_serializing_none]
    pub struct TestFile {
        pub description: String,
        pub schema_version: String,
        pub run_on_requirements: Option<Vec<RunOnRequirements>>,
        pub create_entities: Option<Vec<CreateEntity>>,
        pub initial_data: Option<Vec<InitialData>>,
        pub tests: Vec<Test>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct InitialData {
        pub collection_name: String,
        pub database_name: String,
        pub documents: Vec<Document>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub enum CreateEntity {
        Client(ClientEntity),
        Database(DatabaseEntity),
        Collection(CollectionEntity),
        Thread { id: String },
    }

    #[serde_with::skip_serializing_none]
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ClientEntity {
        pub id: String,
        pub observe_events: Option<HashSet<&'static str>>,
        pub uri_options: Option<Document>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DatabaseEntity {
        pub id: String,
        pub client: String,
        pub database_name: String,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct CollectionEntity {
        pub id: String,
        pub database: String,
        pub collection_name: String,
    }

    #[serde_with::skip_serializing_none]
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Test {
        pub description: String,
        pub run_on_requirements: Option<Vec<RunOnRequirements>>,
        pub operations: Vec<Operation>,
        pub expect_events: Option<Vec<ExpectEvents>>,
        pub outcome: Option<Vec<InitialData>>,
    }

    impl Test {
        pub(crate) fn from_crud_v2(old: crud_v2::Test, test_number: usize) -> Self {
            let mut operations = Vec::new();
            let observed_events = old.observed_events();
            if let Some(fp) = old.fail_point {
                operations.push(Operation {
                    name: "failPoint".to_string(),
                    object: "testRunner".to_string(),
                    arguments: Some(doc! {
                        "client": SETUP_CLIENT_DEREF_PLACEHOLDER,
                        "failPoint": fp,
                    }),
                    save_result_as_entity: None,
                    expect_result: None,
                    expect_error: None,
                });
            }

            let mut ents = vec![];
            ents.push(CreateEntity::Client(ClientEntity {
                id: CLIENT_DEFINITION_PLACEHOLDER.to_string(),
                observe_events: Some(observed_events),
                uri_options: old.client_uri.clone(),
            }));
            ents.push(CreateEntity::Database(DatabaseEntity {
                id: DATABASE_DEFINITION_PLACEHOLDER.to_string(),
                client: CLIENT_DEREF_PLACEHOLDER.to_string(),
                database_name: DATABASE_NAME_DEREF_PLACEHOLDER.to_string(),
            }));
            ents.push(CreateEntity::Collection(CollectionEntity {
                id: COLLECTION_DEFINITION_PLACEHOLDER.to_string(),
                database: DATABASE_DEREF_PLACEHOLDER.to_string(),
                collection_name: COLLECTION_NAME_DEREF_PLACEHOLDER.to_string(),
            }));

            if !ents.is_empty() {
                operations.push(Operation {
                    name: "createEntities".to_string(),
                    object: "testRunner".to_string(),
                    arguments: Some(doc! {
                        "entities": bson::to_bson(&ents).unwrap(),
                    }),
                    expect_error: None,
                    expect_result: None,
                    save_result_as_entity: None,
                });
            }

            for old_op in old.operations {
                operations.push(Operation::from_crud_v2(old_op, test_number));
            }

            let expect_events = old.expectations.map(|old_events| {
                vec![ExpectEvents {
                    client: CLIENT_DEREF_PLACEHOLDER.to_string(),
                    event_type: "command".to_string(),
                    events: old_events
                        .into_iter()
                        .map(|event| ExpectEvent::CommandStartedEvent {
                            command: event.command,
                            command_name: event.command_name,
                            database_name: Some(DATABASE_NAME_DEREF_PLACEHOLDER.to_string()),
                        })
                        .collect(),
                }]
            });

            let outcome = old.outcome.map(|old_outcome| {
                vec![InitialData {
                    database_name: DATABASE_NAME_DEREF_PLACEHOLDER.to_string(),
                    collection_name: COLLECTION_NAME_DEREF_PLACEHOLDER.to_string(),
                    documents: old_outcome.collection.data,
                }]
            });

            Self {
                description: old.description,
                run_on_requirements: None,
                operations,
                expect_events,
                outcome,
            }
        }
    }

    #[serde_with::skip_serializing_none]
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct RunOnRequirements {
        min_server_version: Option<String>,
        max_server_version: Option<String>,
        topologies: Option<Vec<String>>,
        auth: Option<bool>,
    }

    impl From<crud_v2::RunOn> for RunOnRequirements {
        fn from(old: crud_v2::RunOn) -> Self {
            Self {
                min_server_version: old.min_server_version,
                max_server_version: old.max_server_version,
                topologies: old.topology,
                auth: old.auth_enabled,
            }
        }
    }

    #[serde_with::skip_serializing_none]
    #[derive(Debug, Default, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Operation {
        name: String,
        object: String,
        arguments: Option<Document>,
        save_result_as_entity: Option<String>,
        expect_result: Option<Bson>,
        expect_error: Option<ExpectError>,
    }

    impl Operation {
        pub(crate) fn from_crud_v2(old_op: crud_v2::Operation, test_number: usize) -> Self {
            let mut name = old_op.name;
            let mut arguments = old_op.arguments;
            let mut object = match old_op.object.as_str() {
                "collection" => COLLECTION_DEREF_PLACEHOLDER.to_string(),
                _ => old_op.object,
            };

            match name.as_str() {
                "waitForEvent" | "assertEventCount" => {
                    let old_arguments = arguments.as_ref().unwrap();

                    let event = match old_arguments.get_str("event").unwrap() {
                        "ServerMarkedUnknownEvent" => doc! {
                            SERVER_DESCRIPTION_CHANGED: {
                                "newDescription": { "type": "Unknown" }
                            }
                        },
                        "PoolClearedEvent" => doc! {
                            POOL_CLEARED: { }
                        },
                        "PoolReadyEvent" => doc! { POOL_READY: { } },
                        e => panic!("unrecognized event: {}", e),
                    };

                    arguments = doc! {
                        "client": CLIENT_DEREF_PLACEHOLDER.to_string(),
                        "event": event,
                        "count": old_arguments.get("count").unwrap()
                    }
                    .into();
                }
                "recordPrimary" => {
                    arguments = doc! {
                        "client": CLIENT_DEREF_PLACEHOLDER.to_string(),
                        "id": TOPOLOGY_DESCRIPTION_DEFINITION_PLACEHOLDER,
                    }
                    .into();
                    name = "recordTopologyDescription".to_string();
                }
                "waitForPrimaryChange" => {
                    let mut new_arguments = doc! {
                        "client": CLIENT_DEREF_PLACEHOLDER.to_string(),
                        "priorTopologyDescription": TOPOLOGY_DESCRIPTION_DEREF_PLACEHOLDER,
                    };
                    if let Some(timeout) = arguments.as_ref().and_then(|a| a.get("timeoutMS")) {
                        new_arguments.insert("timeoutMS", timeout);
                    }
                    arguments = Some(new_arguments);
                }
                "runAdminCommand" => {
                    arguments
                        .as_mut()
                        .unwrap()
                        .insert("commandName", old_op.command_name.unwrap());
                    object = ADMIN_DATABASE_DEREF_PLACEHOLDER.to_string();
                    name = "runCommand".to_string();
                }
                "runCommand" => {
                    arguments.as_mut().unwrap().insert("commandName", old_op.command_name.unwrap());
                }
                "startThread" => {
                    let thread_name = arguments.as_ref().unwrap().get_str("name").unwrap();
                    let thread_number = Operation::thread_number(thread_name);
                    let thread_entity = CreateEntity::Thread {
                        id: thread_definition_placeholder(thread_number),
                    };
                    name = "createEntities".to_string();
                    object = "testRunner".to_string();
                    arguments = doc! {
                        "entities": [
                            bson::to_bson(&thread_entity).unwrap()
                        ]
                    }
                    .into();
                }
                "runOnThread" => {
                    let old_arguments = arguments.as_ref().unwrap();
                    let thread_name = old_arguments.get_str("name").unwrap();
                    let thread_number = Operation::thread_number(thread_name);

                    let old_operation: crud_v2::Operation =
                        bson::from_bson(old_arguments.get("operation").unwrap().clone()).unwrap();
                    let new_op = Operation::from_crud_v2(old_operation, test_number);

                    arguments = doc! {
                        "thread": thread_deref_placeholder(thread_number),
                        "operation": bson::to_bson(&new_op).unwrap()
                    }
                    .into();
                }
                "waitForThread" => {
                    let thread_name = arguments.as_ref().unwrap().get_str("name").unwrap();
                    let thread_number = Operation::thread_number(thread_name);
                    arguments = doc! {
                        "thread": thread_deref_placeholder(thread_number)
                    }
                    .into();
                }
                "configureFailPoint" => {
                    object = "testRunner".to_string();
                    name = "failPoint".to_string();
                    arguments.as_mut().unwrap().insert("client", SETUP_CLIENT_DEREF_PLACEHOLDER);
                }
                _ => {}
            };

            let (expect_result, expect_error) = match old_op.result {
                Some(OperationResult::Success(b)) => (Some(b), None),
                Some(OperationResult::Error(e)) => (
                    None,
                    ExpectError {
                        is_error: None,
                        error_contains: e.error_contains,
                        error_code: e.error_code,
                        error_code_name: None,
                        error_labels_contain: e.error_labels_contain,
                        error_labels_omit: e.error_labels_omit,
                    }
                    .into(),
                ),
                None if old_op.error.unwrap_or(false) => (
                    None,
                    Some(ExpectError {
                        is_error: Some(true),
                        ..Default::default()
                    }),
                ),
                _ => (None, None),
            };

            Self {
                name,
                object,
                arguments,
                save_result_as_entity: None,
                expect_result,
                expect_error,
            }
        }

        fn thread_number(v2_name: impl AsRef<str>) -> usize {
            v2_name
                .as_ref()
                .strip_prefix("thread")
                .unwrap()
                .parse::<usize>()
                .unwrap()
                - 1
        }
    }

    #[derive(Debug, Default, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ExpectEvents {
        client: String,
        event_type: String,
        events: Vec<ExpectEvent>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub enum ExpectEvent {
        #[serde(rename_all = "camelCase")]
        CommandStartedEvent {
            command: Document,
            command_name: Option<String>,
            database_name: Option<String>,
        },
    }

    #[serde_with::skip_serializing_none]
    #[derive(Debug, Default, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ExpectError {
        is_error: Option<bool>,
        error_contains: Option<String>,
        error_code: Option<i32>,
        error_code_name: Option<String>,
        error_labels_contain: Option<Vec<String>>,
        error_labels_omit: Option<Vec<String>>,
    }
}

static CLIENT_DEFINITION_PLACEHOLDER: &'static str = "xCLIENT_DEFINITION_PLACEHOLDER";
static CLIENT_DEREF_PLACEHOLDER: &'static str = "xCLIENT_DEREF_PLACEHOLDER";

static DATABASE_DEFINITION_PLACEHOLDER: &'static str = "xDATABASE_DEFINITION_PLACEHOLDER";
static DATABASE_DEREF_PLACEHOLDER: &'static str = "xDATABASE_DEREF_PLACEHOLDER";
static DATABASE_NAME_DEFINITION_PLACEHOLDER: &'static str = "xDATABASE_NAME_DEFINITION_PLACEHOLDER";
static DATABASE_NAME_DEREF_PLACEHOLDER: &'static str = "xDATABASE_NAME_DEREF_PLACEHOLDER";

static COLLECTION_DEFINITION_PLACEHOLDER: &'static str = "xCOLLECTION_DEFINITION_PLACEHOLDER";
static COLLECTION_DEREF_PLACEHOLDER: &'static str = "xCOLLECTION_DEREF_PLACEHOLDER";
static COLLECTION_NAME_DEFINITION_PLACEHOLDER: &'static str =
    "COLLECTION_NAME_DEFINITION_PLACEHOLDER";
static COLLECTION_NAME_DEREF_PLACEHOLDER: &'static str = "xCOLLECTION_NAME_DEREF_PLACEHOLDER";

static SETUP_CLIENT_DEFINITION_PLACEHOLDER: &'static str = "xSETUP_CLIENT_DEFINITION_PLACEHOLDER";
static SETUP_CLIENT_DEREF_PLACEHOLDER: &'static str = "xSETUP_CLIENT_DEREF_PLACEHOLDER";

static ADMIN_DATABASE_DEFINITION_PLACEHOLDER: &'static str =
    "xADMIN_DATABASE_DEFINITION_PLACEHOLDER";
static ADMIN_DATABASE_DEREF_PLACEHOLDER: &'static str = "xADMIN_DATABASE_DEREF_PLACEHOLDER";

static TOPOLOGY_DESCRIPTION_DEFINITION_PLACEHOLDER: &'static str = "xTDESC_DEFINITION_PLACEHOLDER";
static TOPOLOGY_DESCRIPTION_DEREF_PLACEHOLDER: &'static str = "xTDESC_DEREF_PLACEHOLDER";

static REGEX_PLACEHOLDER_REPLACEMENTS: &'static [(&'static str, &'static str)] = &[
    (CLIENT_DEFINITION_PLACEHOLDER, "&client client"),
    (CLIENT_DEREF_PLACEHOLDER, "*client"),
    (DATABASE_DEFINITION_PLACEHOLDER, "&database database"),
    (DATABASE_DEREF_PLACEHOLDER, "*database"),
    (
        DATABASE_NAME_DEFINITION_PLACEHOLDER,
        "&databaseName sdam-tests",
    ),
    (DATABASE_NAME_DEREF_PLACEHOLDER, "*databaseName"),
    (COLLECTION_DEFINITION_PLACEHOLDER, "&collection collection"),
    (COLLECTION_DEREF_PLACEHOLDER, "*collection"),
    (COLLECTION_NAME_DEREF_PLACEHOLDER, "*collectionName"),
    ("initialData:", "initialData: &initialData"),
    (
        SETUP_CLIENT_DEFINITION_PLACEHOLDER,
        "&setupClient setupClient",
    ),
    (SETUP_CLIENT_DEREF_PLACEHOLDER, "*setupClient"),
    (
        ADMIN_DATABASE_DEFINITION_PLACEHOLDER,
        "&adminDatabase adminDatabase",
    ),
    (ADMIN_DATABASE_DEREF_PLACEHOLDER, "*adminDatabase"),
    ("THREAD_(\\d+)_DEFINITION_PLACEHOLDER", "&thread$1 thread$1"),
    ("THREAD_(\\d+)_DEREF_PLACEHOLDER", "*thread$1"),
    (TOPOLOGY_DESCRIPTION_DEFINITION_PLACEHOLDER, "&topologyDescription topologyDescription"),
    (TOPOLOGY_DESCRIPTION_DEREF_PLACEHOLDER, "*topologyDescription"),
];

fn thread_definition_placeholder(i: usize) -> String {
    format!("THREAD_{}_DEFINITION_PLACEHOLDER", i)
}

fn thread_deref_placeholder(i: usize) -> String {
    format!("THREAD_{}_DEREF_PLACEHOLDER", i)
}

fn convert(file_name: impl AsRef<str>, old: crud_v2::TestFile) -> Result<String> {
    let mut ents = Vec::new();
    let mut tests = Vec::new();
    let contains_admin_command = old.tests.iter().any(|old_test| {
        old_test
            .operations
            .iter()
            .any(|op| op.name.as_str() == "runAdminCommand")
    });
    let contains_fail_point = old.tests.iter().any(|t| {
        t.fail_point.is_some()
            || t.operations
                .iter()
                .any(|op| op.name.as_str() == "configureFailPoint")
    });

    for (i, old_test) in old.tests.into_iter().enumerate() {
        // if !create_entities_in_tests {
        //     ents.push(CreateEntity::Client(ClientEntity {
        //         id: format!("$CLIENT_{}_DEFINITION_PLACEHOLDER$", i),
        //         observe_events: Some(old_test.observed_events()),
        //         uri_options: old_test.client_uri.clone(),
        //     }));

        //     ents.push(CreateEntity::Database(DatabaseEntity {
        //         id: format!("$DATABASE_{}_DEFINITION_PLACEHOLDER$", i),
        //         client: format!("$CLIENT_{}_DEREF_PLACEHOLDER$", i),
        //         database_name: format!("$DATABASE_{}_NAME_DEFINITION_PLACEHOLDER$", i),
        //     }));

        //     ents.push(CreateEntity::Collection(CollectionEntity {
        //         id: format!("$COLLECTION_{}_DEFINITION_PLACEHOLDER$", i),
        //         database: format!("$DATABASE_{}_DEREF_PLACEHOLDER$", i),
        //         collection_name: format!("$COLLECTION_{}_NAME_DEFINITION_PLACEHOLDER$", i),
        //     }));
        // }

        tests.push(Test::from_crud_v2(old_test, i));
    }

    let initial_data = match old.data {
        TestData::Single(docs) => {
            vec![InitialData {
                collection_name: COLLECTION_NAME_DEFINITION_PLACEHOLDER.to_string(),
                database_name: DATABASE_NAME_DEFINITION_PLACEHOLDER.to_string(),
                documents: docs,
            }]
        }
        _ => panic!("got map of data"),
    };

    if contains_fail_point || contains_admin_command {
        ents.push(CreateEntity::Client(ClientEntity {
            id: SETUP_CLIENT_DEFINITION_PLACEHOLDER.to_string(),
            observe_events: None,
            uri_options: None,
        }));

        if contains_admin_command {
            ents.push(CreateEntity::Database(DatabaseEntity {
                id: ADMIN_DATABASE_DEFINITION_PLACEHOLDER.to_string(),
                client: SETUP_CLIENT_DEREF_PLACEHOLDER.to_string(),
                database_name: "admin".to_string(),
            }))
        }
    }

    let test_file = unified::TestFile {
        description: file_name.as_ref().to_string(),
        schema_version: "1.9".to_string(),
        run_on_requirements: old
            .run_on
            .map(|run_on| run_on.into_iter().map(From::from).collect()),
        create_entities: Some(ents),
        initial_data: Some(initial_data),
        tests,
    };

    let mut raw_string = serde_yaml::to_string(&test_file)?;

    for (regex_str, replacement) in REGEX_PLACEHOLDER_REPLACEMENTS {
        let regex = Regex::new(regex_str).unwrap();
        raw_string = regex.replace_all(&raw_string, *replacement).to_string();
    }

    let regex = Regex::new(COLLECTION_NAME_DEFINITION_PLACEHOLDER).unwrap();
    raw_string = regex
        .replace_all(
            &raw_string,
            format!("&collectionName {}", old.collection_name).as_str(),
        )
        .to_string();

    Ok(raw_string)
}

fn main() -> Result<()> {
    // let file =
    // File::open("/home/patrick/specifications/source/server-discovery-and-monitoring/tests/
    // integration/auth-error.yml")?; multiple tests
    // let file =
    // File::open("/home/patrick/specifications/source/server-discovery-and-monitoring/tests/
    // integration/hello-timeout.yml")?;
    // let file = File::open(
    //     "/home/patrick/specifications/source/server-discovery-and-monitoring/tests/integration/\
    //      rediscover-quickly-after-step-down.yml",
    // )?;

    // threads
    // let file = File::open(
    //     "/home/patrick/specifications/source/server-discovery-and-monitoring/tests/integration/
    // find-shutdown-error.yml", )?;

    let tests_dir =
        PathBuf::from("/home/patrick/specifications/source/server-discovery-and-monitoring/tests/");
    let integration = tests_dir.join("integration");
    let unified = tests_dir.join("unified");

    let paths = std::fs::read_dir(integration)?;

    for path in paths {
        let path = path?;
        if path.path().extension().unwrap() != "yml" {
            continue;
        }
        let filename = path
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        println!("converting {}", filename);
        let out = unified.join(filename.as_str());
        let old_file = File::open(path.path())?;
        let old: crud_v2::TestFile = serde_yaml::from_reader(old_file)?;
        let new = convert(filename.strip_suffix(".yml").unwrap(), old)?;
        let mut new_file = File::create(out)?;
        new_file.write_all(new.as_bytes())?;
        // println!("{}", new);
        // break;
    }
    // println!("{}", new);

    Ok(())
}

use anyhow::Result;

use std::fs::File;

use bson::{doc, Document};
use serde::Deserialize;

use crate::{
    crud_v2::TestData,
    unified::{ClientEntity, CollectionEntity, CreateEntity, DatabaseEntity, InitialData},
};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Serverless {
    Require,
    Forbid,
    Allow,
}

mod crud_v2 {
    use super::Serverless;
    use bson::{from_document, Document};
    use serde::{Deserialize, Deserializer};
    use std::collections::HashMap;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct TestFile {
        #[serde(rename = "runOn")]
        pub(crate) run_on: Option<Vec<RunOn>>,
        pub(crate) database_name: Option<String>,
        pub(crate) collection_name: Option<String>,
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
        pub arguments: Document,
        pub error: Option<bool>,
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
    use bson::{doc, Document};
    use serde::Serialize;
    use serde_yaml::Value;

    use crate::crud_v2;

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
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
        pub todo: String,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub enum CreateEntity {
        Client(ClientEntity),
        Database(DatabaseEntity),
        Collection(CollectionEntity),
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ClientEntity {
        pub id: String,
        pub observe_events: Option<Vec<String>>,
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
    }

    impl From<crud_v2::Test> for Test {
        fn from(old: crud_v2::Test) -> Self {
            let mut operations = Vec::new();
            if let Some(fp) = old.fail_point {
                operations.push(Operation {
                    name: "configureFailPoint".to_string(),
                    object: "testRunner".to_string(),
                    arguments: doc! {
                        "client": "*client0",
                        "failPoint": fp,
                    },
                    save_result_as_entity: None,
                    expect_result: None,
                    expect_error: None,
                });
            }

            for old_op in old.operations {
                operations.push(old_op.into());
            }

            let expect_events = old.expectations.map(|old_events| {
                vec![ExpectEvents {
                    client: "*client0".to_string(),
                    event_type: "command".to_string(),
                    events: old_events
                        .into_iter()
                        .map(|event| ExpectEvent::CommandStartedEvent {
                            command: event.command,
                            command_name: event.command_name,
                            database_name: Some("*database0Name".to_string()),
                        })
                        .collect(),
                }]
            });

            Self {
                description: old.description,
                run_on_requirements: None,
                operations,
                expect_events,
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
        arguments: Document,
        save_result_as_entity: Option<String>,
        expect_result: Option<serde_yaml::Mapping>,
        expect_error: Option<ExpectError>,
    }

    impl From<crud_v2::Operation> for Operation {
        fn from(old_op: crud_v2::Operation) -> Self {
            let arguments = match old_op.name.as_str() {
                "waitForEvent" | "assertEventCount" => {
                    let event = match old_op.arguments.get_str("event").unwrap() {
                        "ServerMarkedUnknownEvent" => doc! {
                            "serverDescriptionChanged": {
                                "newDescription": { "type": "Unknown" }
                            }
                        },
                        "PoolClearedEvent" => doc! {
                            "poolClearedEvent": { }
                        },
                        e => panic!("unrecognized event: {}", e)
                    };

                    doc! {
                        "client": "*client0",
                        "event": event,
                        "count": old_op.arguments.get("count").unwrap()
                    }
                },
                
                _ => old_op.arguments,
            };

            Self {
                name: old_op.name,
                object: match old_op.object.as_str() {
                    "collection" => "*collection0".to_string(),
                    _ => old_op.object,
                },
                arguments,
                save_result_as_entity: None,
                expect_result: None,
                expect_error: match old_op.error {
                    Some(true) => Some(ExpectError { is_error: true }),
                    _ => None,
                },
            }
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

    #[derive(Debug, Default, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct ExpectError {
        is_error: bool,
    }
}

fn convert(file_name: impl AsRef<str>, old: crud_v2::TestFile) -> unified::TestFile {
    let create_entities = Some(vec![
        CreateEntity::Client(ClientEntity {
            id: "&client0 client0".to_string(),
            observe_events: Some(vec!["TODO".to_string()]),
            uri_options: old.tests[0].client_uri.clone(),
        }),
        CreateEntity::Database(DatabaseEntity {
            id: "&database0 database0".to_string(),
            client: "*client0".to_string(),
            database_name: "&database0Name sdam-tests".to_string(),
        }),
        CreateEntity::Collection(CollectionEntity {
            id: "&collection0 collection0".to_string(),
            database: "*database0".to_string(),
            collection_name: format!("&collection0Name {}", file_name.as_ref()),
        }),
    ]);

    let initial_data = match old.data {
        TestData::Single(docs) => {
            vec![InitialData {
                collection_name: "*collection0Name".to_string(),
                database_name: "*database0Name".to_string(),
                documents: docs,
                todo: "TODO: add &initialData".to_string(),
            }]
        }
        _ => panic!("got map of data"),
    };

    unified::TestFile {
        description: file_name.as_ref().to_string(),
        schema_version: "1.9".to_string(),
        run_on_requirements: old
            .run_on
            .map(|run_on| run_on.into_iter().map(From::from).collect()),
        create_entities,
        initial_data: Some(initial_data),
        tests: old.tests.into_iter().map(From::from).collect(),
    }
}

fn main() -> Result<()> {
    let file = File::open("/home/patrick/specifications/source/server-discovery-and-monitoring/tests/integration/auth-error.yml")?;

    let old: crud_v2::TestFile = serde_yaml::from_reader(file)?;
    let new = convert("auth-error", old);
    let yaml = serde_yaml::to_string(&new)?;
    println!("{}", yaml);
    
    Ok(())
}

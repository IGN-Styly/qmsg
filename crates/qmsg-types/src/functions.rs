//! Provider-defined actions, lookups, and input helpers.

use crate::{ChannelRef, Content, Message};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Arguments = BTreeMap<String, Value>;

/// Files can use blobs inside `Content`. Custom records need no new variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Record(Arguments),
    Content(Vec<Content>),
    Channel(ChannelRef),
    Message(Message),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueType {
    Any,
    Null,
    Boolean,
    Integer,
    Text,
    Bytes,
    List(Box<ValueType>),
    Record,
    Content,
    Channel,
    Message,
}

impl ValueType {
    pub fn accepts(&self, value: &Value) -> bool {
        match (self, value) {
            (Self::Any, _) => true,
            (Self::Null, Value::Null) => true,
            (Self::Boolean, Value::Boolean(_))
            | (Self::Integer, Value::Integer(_))
            | (Self::Text, Value::Text(_))
            | (Self::Bytes, Value::Bytes(_))
            | (Self::Record, Value::Record(_))
            | (Self::Content, Value::Content(_))
            | (Self::Channel, Value::Channel(_))
            | (Self::Message, Value::Message(_)) => true,
            (Self::List(kind), Value::List(values)) => values.iter().all(|v| kind.accepts(v)),
            _ => false,
        }
    }
}

/// Providers must check scope, existence and permissions. A message names
/// its channel, so it cannot name a conflicting organization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionContext {
    #[default]
    Provider,
    Organization(String),
    Channel(ChannelRef),
    Message {
        channel: ChannelRef,
        id: String,
    },
}

/// Common actions let clients recognize edit/delete without guessing ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKind {
    SendMessage,
    EditMessage,
    DeleteMessage,
    OpenChannel,
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionKind {
    Action {
        action: ActionKind,
        result: ValueType,
    },
    Lookup(ValueType),
    Verification,
    Completion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionInput {
    pub id: String,
    pub label: String,
    pub description: String,
    pub value_type: ValueType,
    pub required: bool,
    /// Id of a `Verification` function in the same provider.
    pub verification: Option<String>,
    /// Id of a `Completion` function in the same provider.
    pub completion: Option<String>,
}

/// Available in the requested context. Discovery is a snapshot; providers
/// must still check permissions and inputs when called.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Function {
    pub id: String,
    pub label: String,
    pub description: String,
    pub kind: FunctionKind,
    pub inputs: Vec<FunctionInput>,
}

impl Function {
    /// Checks required inputs, types (including list items), and unknown
    /// inputs locally. This does not run provider verification functions.
    /// Providers should also check before carrying out a call.
    pub fn check(&self, arguments: &Arguments) -> Result<(), Vec<InputIssue>> {
        let mut issues = Vec::new();
        for input in &self.inputs {
            match arguments.get(&input.id) {
                None if input.required => issues.push(InputIssue {
                    input: Some(input.id.clone()),
                    code: "required".into(),
                    message: "This input is required".into(),
                }),
                Some(value) if !input.value_type.accepts(value) => issues.push(InputIssue {
                    input: Some(input.id.clone()),
                    code: "wrong_type".into(),
                    message: format!("Expected {:?}", input.value_type),
                }),
                _ => {}
            }
        }
        for id in arguments.keys() {
            if !self.inputs.iter().any(|input| &input.id == id) {
                issues.push(InputIssue {
                    input: Some(id.clone()),
                    code: "unknown_input".into(),
                    message: "Unknown input".into(),
                });
            }
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionCall {
    pub function: String,
    pub context: FunctionContext,
    pub arguments: Arguments,
}

/// Identifies an input on its parent action/lookup. Helper functions can be
/// shared by several functions without losing which input requested them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionInputRef {
    pub function: String,
    pub input: String,
}

/// `call.arguments` holds other inputs, which can still be incomplete.
/// `input` names the target input, or is `None` for a standalone check.
/// Verification must not carry out the action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationRequest {
    pub call: FunctionCall,
    pub input: Option<FunctionInputRef>,
    pub value: Value,
}

/// Completion must not carry out actions. Cursors are opaque; use the same
/// context, arguments, input and query for subsequent pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionRequest {
    pub call: FunctionCall,
    pub input: Option<FunctionInputRef>,
    pub query: String,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputIssue {
    /// `None` means the issue applies to the whole input set.
    pub input: Option<String>,
    /// Provider-defined stable code; clients display `message`.
    pub code: String,
    pub message: String,
}

/// Invalid input is a normal result. Request failures such as network errors
/// or missing permissions use `CommandError` instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verification {
    Valid,
    Invalid(Vec<InputIssue>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionItem {
    pub label: String,
    pub description: Option<String>,
    /// The value to use, which can differ from its display label.
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionPage {
    /// At most the requested limit. An empty list means no matches.
    pub items: Vec<CompletionItem>,
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit() -> Function {
        Function {
            id: "edit".into(),
            label: "Edit message".into(),
            description: "Change text".into(),
            kind: FunctionKind::Action {
                action: ActionKind::EditMessage,
                result: ValueType::Null,
            },
            inputs: vec![
                FunctionInput {
                    id: "text".into(),
                    label: "Text".into(),
                    description: "New text".into(),
                    value_type: ValueType::Text,
                    required: true,
                    verification: Some("check-text".into()),
                    completion: None,
                },
                FunctionInput {
                    id: "tags".into(),
                    label: "Tags".into(),
                    description: "Optional tags".into(),
                    value_type: ValueType::List(Box::new(ValueType::Text)),
                    required: false,
                    verification: None,
                    completion: None,
                },
            ],
        }
    }

    #[test]
    fn input_checks_collect_missing_wrong_and_unknown_inputs() {
        let function = edit();
        let issues = function
            .check(
                &[
                    ("tags".into(), Value::List(vec![Value::Integer(1)])),
                    ("extra".into(), Value::Boolean(true)),
                ]
                .into(),
            )
            .unwrap_err();
        assert_eq!(
            issues
                .iter()
                .map(|i| (i.input.as_deref(), i.code.as_str()))
                .collect::<Vec<_>>(),
            [
                (Some("text"), "required"),
                (Some("tags"), "wrong_type"),
                (Some("extra"), "unknown_input")
            ]
        );
        assert!(
            function
                .check(&[("text".into(), Value::Text("hello".into()))].into())
                .is_ok()
        );
        assert_eq!(
            function
                .check(&[("text".into(), Value::Null)].into())
                .unwrap_err()[0]
                .code,
            "wrong_type"
        );
        assert!(ValueType::Null.accepts(&Value::Null));
        assert!(!ValueType::Null.accepts(&Value::Text(String::new())));
    }

    #[test]
    fn declarations_and_requests_keep_context_and_typed_values_on_the_wire() {
        use crate::{Command, HostMessage, ProviderMessage, Reply, decode, encode};
        let functions = ProviderMessage::Reply {
            request: 1,
            result: Ok(Reply::Functions(vec![
                edit(),
                Function {
                    id: "delete".into(),
                    label: "Delete".into(),
                    description: "Delete message".into(),
                    kind: FunctionKind::Action {
                        action: ActionKind::DeleteMessage,
                        result: ValueType::Null,
                    },
                    inputs: vec![],
                },
            ])),
        };
        assert_eq!(
            decode::<ProviderMessage>(&encode(&functions).unwrap()).unwrap(),
            functions
        );
        let call = FunctionCall {
            function: "check-text".into(),
            context: FunctionContext::Message {
                channel: ChannelRef::new(Some("org"), "channel"),
                id: "message".into(),
            },
            arguments: [("tags".into(), Value::List(vec![Value::Text("tag".into())]))].into(),
        };
        let message = HostMessage::Command(Command::Verify {
            request: 2,
            verification: Box::new(VerificationRequest {
                call,
                input: Some(FunctionInputRef {
                    function: "edit".into(),
                    input: "text".into(),
                }),
                value: Value::Content(vec![Content::Text("hello".into())]),
            }),
        });
        assert_eq!(
            decode::<HostMessage>(&encode(&message).unwrap()).unwrap(),
            message
        );
        let failure = ProviderMessage::Reply {
            request: 3,
            result: Err(crate::CommandError::Provider {
                code: "partial_send".into(),
                message: "Only some parts reached the server".into(),
                details: Some(Box::new(Value::Record(
                    [("confirmed".into(), Value::Integer(2))].into(),
                ))),
            }),
        };
        assert_eq!(
            decode::<ProviderMessage>(&encode(&failure).unwrap()).unwrap(),
            failure
        );
    }
}

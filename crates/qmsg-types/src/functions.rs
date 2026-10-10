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
    /// A record with these fields. Empty takes any record; otherwise fields
    /// that aren't listed are refused.
    Record(Vec<Field>),
    Content,
    Channel,
    Message,
}

/// One field of a [`ValueType::Record`], so clients can show typed results
/// and forms without guessing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub id: String,
    pub label: String,
    pub value_type: ValueType,
    pub required: bool,
}

impl Field {
    pub fn new(id: impl Into<String>, label: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            value_type,
            required: true,
        }
    }

    pub fn optional(self) -> Self {
        Self {
            required: false,
            ..self
        }
    }
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
            | (Self::Content, Value::Content(_))
            | (Self::Channel, Value::Channel(_))
            | (Self::Message, Value::Message(_)) => true,
            (Self::List(kind), Value::List(values)) => values.iter().all(|v| kind.accepts(v)),
            (Self::Record(fields), Value::Record(values)) => {
                fields.is_empty()
                    || (fields.iter().all(|f| match values.get(&f.id) {
                        None => !f.required,
                        Some(v) => f.value_type.accepts(v),
                    }) && values.keys().all(|k| fields.iter().any(|f| &f.id == k)))
            }
            _ => false,
        }
    }

    /// Whether every value this type accepts, `other` accepts too.
    fn fits(&self, other: &ValueType) -> bool {
        match (self, other) {
            (_, Self::Any) => true,
            (Self::List(a), Self::List(b)) => a.fits(b),
            (a, b) => a == b,
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

/// The ids of the inputs of standard actions; see [`ActionKind::contract`].
pub mod inputs {
    /// [`Content`](crate::Content) to send, or a message's new content.
    pub const CONTENT: &str = "content";
    /// The id of the message a new one replies to.
    pub const REPLY_TO: &str = "reply_to";
    /// The client's id for a send, repeated in [`Message::nonce`](crate::Message::nonce).
    pub const NONCE: &str = "nonce";
    /// User ids or addresses to open a conversation with.
    pub const MEMBERS: &str = "members";
    /// A reaction key, such as `👍` or a provider's custom emoji id.
    pub const REACTION: &str = "reaction";
}

/// Standard actions have fixed inputs and results, so one client form works
/// with every provider. Custom actions describe their own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionKind {
    SendMessage,
    EditMessage,
    DeleteMessage,
    OpenChannel,
    AddReaction,
    RemoveReaction,
    /// Marks the message, and everything before it, read.
    MarkRead,
    /// Marks the message, and everything after it, unread.
    MarkUnread,
    /// Tells the channel the account is typing.
    Typing,
    /// One login step. Its inputs are the provider's own, such as a
    /// password or a code. See [`ProviderStatus::LoginRequired`](crate::ProviderStatus).
    Login,
    Logout,
    Custom(String),
}

/// Where a standard action can be offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextKind {
    /// The provider or an organization.
    Account,
    Channel,
    Message,
}

impl ContextKind {
    pub fn of(context: &FunctionContext) -> Self {
        match context {
            FunctionContext::Provider | FunctionContext::Organization(_) => Self::Account,
            FunctionContext::Channel(_) => Self::Channel,
            FunctionContext::Message { .. } => Self::Message,
        }
    }
}

/// What every function for a standard action takes and returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contract {
    pub context: ContextKind,
    /// Functions may add optional inputs of their own, except for
    /// [`ActionKind::Login`], whose inputs are all the provider's.
    pub inputs: Vec<FunctionInput>,
    pub result: ValueType,
}

impl ActionKind {
    /// The shared contract, or `None` for custom actions.
    ///
    /// | Action | Context | Inputs | Result |
    /// | --- | --- | --- | --- |
    /// | `SendMessage` | channel | `content`, optional `reply_to` and `nonce` | message id |
    /// | `EditMessage` | message | `content` | null |
    /// | `DeleteMessage` | message | | null |
    /// | `OpenChannel` | provider or organization | `members`, a list of text | channel |
    /// | `AddReaction`, `RemoveReaction` | message | `reaction` | null |
    /// | `MarkRead`, `MarkUnread` | message | | null |
    /// | `Typing` | channel | | null |
    /// | `Login`, `Logout` | provider or organization | the provider's own for `Login` | null |
    pub fn contract(&self) -> Option<Contract> {
        use ValueType::{List, Null, Text};
        let input = |id: &str, label: &str, value_type| FunctionInput {
            required: true,
            ..FunctionInput::new(id, label, value_type)
        };
        let content = || input(inputs::CONTENT, "Content", ValueType::Content);
        let reaction = || input(inputs::REACTION, "Reaction", Text);
        let (context, inputs, result) = match self {
            Self::SendMessage => (
                ContextKind::Channel,
                vec![
                    content(),
                    FunctionInput::new(inputs::REPLY_TO, "Reply to", Text),
                    FunctionInput::new(inputs::NONCE, "Nonce", Text),
                ],
                Text,
            ),
            Self::EditMessage => (ContextKind::Message, vec![content()], Null),
            Self::DeleteMessage | Self::MarkRead | Self::MarkUnread => {
                (ContextKind::Message, vec![], Null)
            }
            Self::OpenChannel => (
                ContextKind::Account,
                vec![input(inputs::MEMBERS, "Members", List(Box::new(Text)))],
                ValueType::Channel,
            ),
            Self::AddReaction | Self::RemoveReaction => {
                (ContextKind::Message, vec![reaction()], Null)
            }
            Self::Typing => (ContextKind::Channel, vec![], Null),
            Self::Login | Self::Logout => (ContextKind::Account, vec![], Null),
            Self::Custom(_) => return None,
        };
        Some(Contract {
            context,
            inputs,
            result,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionKind {
    Action {
        action: ActionKind,
        result: ValueType,
    },
    Lookup(ValueType),
    /// Checks a value of this type.
    Verification(ValueType),
    /// Suggests values of this type.
    Completion(ValueType),
}

/// How a client should show an input.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputHint {
    #[default]
    Plain,
    /// Hide what is typed, such as a password. Don't keep or log it.
    Secret,
    /// Text that can span lines.
    Multiline,
    /// Only one of these values, or for a list, items from them.
    Choices(Vec<Choice>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Choice {
    pub label: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionInput {
    pub id: String,
    pub label: String,
    pub description: String,
    pub value_type: ValueType,
    pub required: bool,
    pub hint: InputHint,
    /// Id of a `Verification` function in the same provider. For a list
    /// input, it can check one item.
    pub verification: Option<String>,
    /// Id of a `Completion` function in the same provider. For a list
    /// input, it can suggest one item.
    pub completion: Option<String>,
}

impl FunctionInput {
    /// An optional, plain input with no description or helpers.
    pub fn new(id: impl Into<String>, label: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            description: String::new(),
            value_type,
            required: false,
            hint: InputHint::Plain,
            verification: None,
            completion: None,
        }
    }

    fn takes(&self, value: &Value) -> bool {
        let InputHint::Choices(choices) = &self.hint else {
            return true;
        };
        let is_choice = |v: &Value| choices.iter().any(|c| &c.value == v);
        match (&self.value_type, value) {
            (ValueType::List(_), Value::List(items)) => items.iter().all(is_choice),
            _ => is_choice(value),
        }
    }
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
    /// A standard action with its contract's inputs and result, or a custom
    /// one with no inputs and a null result.
    pub fn action(id: impl Into<String>, label: impl Into<String>, action: ActionKind) -> Self {
        let (inputs, result) = match action.contract() {
            Some(contract) => (contract.inputs, contract.result),
            None => (vec![], ValueType::Null),
        };
        Self {
            id: id.into(),
            label: label.into(),
            description: String::new(),
            kind: FunctionKind::Action { action, result },
            inputs,
        }
    }

    /// Checks required inputs, types (including list items and record
    /// fields), choices, and unknown inputs locally. This does not run
    /// provider verification functions. Providers should also check before
    /// carrying out a call.
    pub fn check(&self, arguments: &Arguments) -> Result<(), Vec<InputIssue>> {
        let mut issues = Vec::new();
        let issue = |input: &str, code: &str, message: String| InputIssue {
            input: Some(input.into()),
            code: code.into(),
            message,
        };
        for input in &self.inputs {
            match arguments.get(&input.id) {
                None if input.required => issues.push(issue(
                    &input.id,
                    "required",
                    "This input is required".into(),
                )),
                Some(value) if !input.value_type.accepts(value) => issues.push(issue(
                    &input.id,
                    "wrong_type",
                    format!("Expected {:?}", input.value_type),
                )),
                Some(value) if !input.takes(value) => issues.push(issue(
                    &input.id,
                    "not_a_choice",
                    "Pick one of the choices".into(),
                )),
                _ => {}
            }
        }
        for id in arguments.keys() {
            if !self.inputs.iter().any(|input| &input.id == id) {
                issues.push(issue(id, "unknown_input", "Unknown input".into()));
            }
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }

    /// The type of what calling this returns: an action's or lookup's
    /// result, a checked value for verification, or suggested values for
    /// completion.
    pub fn value_type(&self) -> &ValueType {
        match &self.kind {
            FunctionKind::Action { result, .. } => result,
            FunctionKind::Lookup(t)
            | FunctionKind::Verification(t)
            | FunctionKind::Completion(t) => t,
        }
    }
}

/// Checks a provider's answer to discovery in `context`: unique ids, helper
/// references to declared helpers whose type fits the input, choices of the
/// input's type, and standard actions that follow their [`Contract`].
pub fn check_declarations(context: &FunctionContext, functions: &[Function]) -> Result<(), String> {
    for (i, function) in functions.iter().enumerate() {
        let id = &function.id;
        if functions[..i].iter().any(|f| &f.id == id) {
            return Err(format!("function `{id}` is declared twice"));
        }
        for input in &function.inputs {
            let helpers = [
                (&input.verification, "verification"),
                (&input.completion, "completion"),
            ];
            for (helper, what) in helpers {
                let Some(helper) = helper else { continue };
                let found = functions.iter().find(|f| &f.id == helper);
                let helper_type = match found.map(|f| &f.kind) {
                    Some(FunctionKind::Verification(t)) if what == "verification" => t,
                    Some(FunctionKind::Completion(t)) if what == "completion" => t,
                    _ => return Err(format!("`{id}.{}` has no {what} `{helper}`", input.id)),
                };
                // A verification must take every value of the input, and a
                // completion must suggest only values the input takes. Helpers
                // of list inputs can work on one item at a time.
                let fits = |input: &ValueType| match what {
                    "verification" => input.fits(helper_type),
                    _ => helper_type.fits(input),
                };
                let fits = match &input.value_type {
                    ValueType::List(item) => fits(item),
                    _ => false,
                } || fits(&input.value_type);
                if !fits {
                    return Err(format!("{what} `{helper}` doesn't fit `{id}.{}`", input.id));
                }
            }
            if let InputHint::Choices(choices) = &input.hint {
                let item_type = match &input.value_type {
                    ValueType::List(item) => item,
                    other => other,
                };
                if choices.iter().any(|c| !item_type.accepts(&c.value)) {
                    return Err(format!(
                        "`{id}.{}` has a choice of the wrong type",
                        input.id
                    ));
                }
            }
        }
        function.check_contract(context)?;
    }
    Ok(())
}

impl Function {
    /// Checks a standard action follows its [`Contract`] in `context`,
    /// including where it is offered. Other functions always pass.
    pub fn check_contract(&self, context: &FunctionContext) -> Result<(), String> {
        let FunctionKind::Action { action, result } = &self.kind else {
            return Ok(());
        };
        check_contract(self, action, result, context)
            .map_err(|e| format!("`{}` is not a standard {action:?}: {e}", self.id))
    }
}

fn check_contract(
    function: &Function,
    action: &ActionKind,
    result: &ValueType,
    context: &FunctionContext,
) -> Result<(), String> {
    let Some(contract) = action.contract() else {
        return Ok(());
    };
    if contract.context != ContextKind::of(context) {
        return Err(format!(
            "offered in a {:?} context",
            ContextKind::of(context)
        ));
    }
    if *result != contract.result {
        return Err(format!("returns {result:?}, not {:?}", contract.result));
    }
    for expected in &contract.inputs {
        match function.inputs.iter().find(|i| i.id == expected.id) {
            Some(i) if i.value_type == expected.value_type && i.required == expected.required => {}
            _ => return Err(format!("input `{}` differs", expected.id)),
        }
    }
    if *action != ActionKind::Login
        && let Some(extra) = function
            .inputs
            .iter()
            .find(|i| i.required && !contract.inputs.iter().any(|c| c.id == i.id))
    {
        return Err(format!("extra input `{}` is required", extra.id));
    }
    Ok(())
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
                action: ActionKind::Custom("edit-text".into()),
                result: ValueType::Null,
            },
            inputs: vec![
                FunctionInput {
                    required: true,
                    hint: InputHint::Multiline,
                    verification: Some("check-text".into()),
                    ..FunctionInput::new("text", "Text", ValueType::Text)
                },
                FunctionInput {
                    hint: InputHint::Choices(vec![
                        Choice {
                            label: "Urgent".into(),
                            value: Value::Text("urgent".into()),
                        },
                        Choice {
                            label: "Later".into(),
                            value: Value::Text("later".into()),
                        },
                    ]),
                    ..FunctionInput::new("tags", "Tags", ValueType::List(Box::new(ValueType::Text)))
                },
            ],
        }
    }

    fn check_text() -> Function {
        Function {
            id: "check-text".into(),
            label: "Check text".into(),
            description: String::new(),
            kind: FunctionKind::Verification(ValueType::Text),
            inputs: vec![],
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
    fn choices_limit_values_and_list_items() {
        let function = edit();
        let tags = |tags: &[&str]| -> Arguments {
            [
                ("text".into(), Value::Text("hi".into())),
                (
                    "tags".into(),
                    Value::List(tags.iter().map(|t| Value::Text((*t).into())).collect()),
                ),
            ]
            .into()
        };
        assert!(function.check(&tags(&["urgent", "later"])).is_ok());
        let issues = function.check(&tags(&["urgent", "soon"])).unwrap_err();
        assert_eq!(issues[0].code, "not_a_choice");
    }

    #[test]
    fn records_check_their_fields() {
        let user = ValueType::Record(vec![
            Field::new("id", "Id", ValueType::Text),
            Field::new("name", "Name", ValueType::Text).optional(),
        ]);
        let record = |fields: &[(&str, Value)]| {
            Value::Record(
                fields
                    .iter()
                    .map(|(k, v)| ((*k).into(), v.clone()))
                    .collect(),
            )
        };
        let id = ("id", Value::Text("a".into()));
        assert!(user.accepts(&record(std::slice::from_ref(&id))));
        assert!(user.accepts(&record(&[id.clone(), ("name", Value::Text("A".into()))])));
        // Missing, wrongly typed and unlisted fields are all refused.
        assert!(!user.accepts(&record(&[("name", Value::Text("A".into()))])));
        assert!(!user.accepts(&record(&[("id", Value::Integer(1))])));
        assert!(!user.accepts(&record(&[id.clone(), ("age", Value::Integer(3))])));
        // No fields takes any record.
        assert!(ValueType::Record(vec![]).accepts(&record(&[("age", Value::Integer(3))])));
    }

    #[test]
    fn standard_actions_follow_their_contract() {
        let channel = FunctionContext::Channel(ChannelRef::new(None, "c"));
        let message = FunctionContext::Message {
            channel: ChannelRef::new(None, "c"),
            id: "1".into(),
        };
        let send = Function::action("send", "Send", ActionKind::SendMessage);
        let edit = Function::action("edit", "Edit", ActionKind::EditMessage);
        assert_eq!(
            check_declarations(&channel, std::slice::from_ref(&send)),
            Ok(())
        );
        assert_eq!(
            check_declarations(&message, std::slice::from_ref(&edit)),
            Ok(())
        );
        // Optional extras are fine; required ones break the common form.
        let mut silent = send.clone();
        silent
            .inputs
            .push(FunctionInput::new("silent", "Silent", ValueType::Boolean));
        assert_eq!(check_declarations(&channel, &[silent.clone()]), Ok(()));
        silent.inputs.last_mut().unwrap().required = true;
        assert!(check_declarations(&channel, &[silent]).is_err());
        // Wrong context, result or input type.
        assert!(check_declarations(&channel, std::slice::from_ref(&edit)).is_err());
        let mut wrong = edit.clone();
        wrong.kind = FunctionKind::Action {
            action: ActionKind::EditMessage,
            result: ValueType::Message,
        };
        assert!(check_declarations(&message, &[wrong]).is_err());
        let mut wrong = edit;
        wrong.inputs[0].value_type = ValueType::Text;
        assert!(check_declarations(&message, &[wrong]).is_err());
        // Login inputs are the provider's own.
        let mut login = Function::action("login", "Log in", ActionKind::Login);
        login.inputs.push(FunctionInput {
            required: true,
            hint: InputHint::Secret,
            ..FunctionInput::new("password", "Password", ValueType::Text)
        });
        assert_eq!(
            check_declarations(&FunctionContext::Provider, &[login]),
            Ok(())
        );
    }

    #[test]
    fn helper_references_must_resolve_to_fitting_helpers() {
        let context = FunctionContext::Provider;
        assert_eq!(
            check_declarations(&context, &[edit(), check_text()]),
            Ok(())
        );
        assert!(check_declarations(&context, &[edit()]).is_err());
        let mut wrong = check_text();
        wrong.kind = FunctionKind::Verification(ValueType::Integer);
        assert!(check_declarations(&context, &[edit(), wrong]).is_err());
        // A list input's helper can take one item.
        let mut list = edit();
        list.inputs[1].completion = Some("suggest-tag".into());
        let mut suggest_tag = check_text();
        suggest_tag.id = "suggest-tag".into();
        suggest_tag.kind = FunctionKind::Completion(ValueType::Text);
        assert_eq!(
            check_declarations(&context, &[list, check_text(), suggest_tag]),
            Ok(())
        );
        let mut any = check_text();
        any.kind = FunctionKind::Verification(ValueType::Any);
        assert_eq!(check_declarations(&context, &[edit(), any]), Ok(()));
        // A verification of text can't check any value.
        let mut takes_any = edit();
        takes_any.inputs[0].value_type = ValueType::Any;
        assert!(check_declarations(&context, &[takes_any, check_text()]).is_err());
        let mut completion = check_text();
        completion.kind = FunctionKind::Completion(ValueType::Text);
        assert!(check_declarations(&context, &[edit(), completion]).is_err());
        assert!(check_declarations(&context, &[edit(), check_text(), check_text()]).is_err());
        // A completion must suggest only what the input takes, the other
        // way round from verification.
        let suggesting = |input: ValueType, suggests: ValueType| {
            let mut function = edit();
            function.inputs = vec![FunctionInput {
                completion: Some("suggest".into()),
                ..FunctionInput::new("input", "Input", input)
            }];
            let mut suggest = check_text();
            suggest.id = "suggest".into();
            suggest.kind = FunctionKind::Completion(suggests);
            check_declarations(&context, &[function, suggest])
        };
        let list = |item| ValueType::List(Box::new(item));
        assert!(suggesting(ValueType::Text, ValueType::Any).is_err());
        assert_eq!(suggesting(ValueType::Any, ValueType::Text), Ok(()));
        assert!(suggesting(list(ValueType::Text), ValueType::Any).is_err());
        assert_eq!(suggesting(list(ValueType::Any), ValueType::Text), Ok(()));
        assert_eq!(
            suggesting(list(ValueType::Text), list(ValueType::Text)),
            Ok(())
        );
        assert!(suggesting(list(ValueType::Text), list(ValueType::Any)).is_err());
        assert!(suggesting(ValueType::Text, ValueType::Integer).is_err());
        let mut bad_choice = edit();
        bad_choice.inputs[1].hint = InputHint::Choices(vec![Choice {
            label: "One".into(),
            value: Value::Integer(1),
        }]);
        assert!(check_declarations(&context, &[bad_choice, check_text()]).is_err());
    }

    #[test]
    fn declarations_and_requests_keep_context_and_typed_values_on_the_wire() {
        use crate::{Command, HostMessage, ProviderMessage, Reply, decode, encode};
        let functions = ProviderMessage::Reply {
            request: 1,
            result: Ok(Reply::Functions(vec![
                edit(),
                check_text(),
                Function::action("delete", "Delete", ActionKind::DeleteMessage),
                Function {
                    id: "users".into(),
                    label: "Users".into(),
                    description: String::new(),
                    kind: FunctionKind::Lookup(ValueType::List(Box::new(ValueType::Record(vec![
                        Field::new("id", "Id", ValueType::Text),
                    ])))),
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

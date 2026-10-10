//! Small example of discovery, calls, and input helpers. These functions use
//! the provider's existing account/channel and do not write to its TCP server.

use qmsg_sdk::{
    ActionKind, ChannelRef, Command, CommandError, CompletionItem, CompletionPage, Context,
    Function, FunctionContext, FunctionInput, FunctionInputRef, FunctionKind, InputIssue, Reply,
    Result, Value, ValueType, Verification,
};

pub fn declarations() -> Vec<Function> {
    let user = FunctionInput {
        id: "user".into(),
        label: "User".into(),
        description: "User id".into(),
        value_type: ValueType::Text,
        required: false,
        verification: Some("verify-user".into()),
        completion: Some("complete-user".into()),
    };
    vec![
        Function {
            id: "users".into(),
            label: "Find users".into(),
            description: "List users, or find one by id".into(),
            kind: FunctionKind::Lookup(ValueType::List(Box::new(ValueType::Record))),
            inputs: vec![user.clone()],
        },
        Function {
            id: "open-channel".into(),
            label: "Open channel".into(),
            description: "Find the channel with the server".into(),
            kind: FunctionKind::Action {
                action: ActionKind::OpenChannel,
                result: ValueType::Channel,
            },
            inputs: vec![FunctionInput {
                required: true,
                ..user
            }],
        },
        Function {
            id: "verify-user".into(),
            label: "Check user".into(),
            description: "Check whether a user id exists".into(),
            kind: FunctionKind::Verification,
            inputs: vec![],
        },
        Function {
            id: "complete-user".into(),
            label: "Suggest users".into(),
            description: "Find user ids by name or id prefix".into(),
            kind: FunctionKind::Completion,
            inputs: vec![],
        },
    ]
}

fn issue(input: Option<String>, code: &str, message: &str) -> InputIssue {
    InputIssue {
        input,
        code: code.into(),
        message: message.into(),
    }
}

fn check_user(value: &Value, input: Option<&FunctionInputRef>, server: &str) -> Verification {
    let field = input.map(|i| i.input.clone());
    let problem = match value {
        Value::Text(id)
            if id == super::ME && input.is_some_and(|i| i.function == "open-channel") =>
        {
            issue(
                field,
                "no_conversation",
                "Only the server has a conversation",
            )
        }
        Value::Text(id) if id == server || id == super::ME => return Verification::Valid,
        Value::Text(_) => issue(field, "unknown_user", "This user does not exist"),
        _ => issue(field, "wrong_type", "Expected a user id as text"),
    };
    Verification::Invalid(vec![problem])
}

fn check_helper(id: &str, expected: &str) -> std::result::Result<(), CommandError> {
    if id == expected {
        Ok(())
    } else if declarations().iter().any(|f| f.id == id) {
        Err(CommandError::Unsupported)
    } else {
        Err(CommandError::UnknownFunction(id.into()))
    }
}

fn check_context(
    context: &FunctionContext,
    channel: &ChannelRef,
) -> std::result::Result<(), CommandError> {
    match context {
        FunctionContext::Provider => Ok(()),
        FunctionContext::Organization(org) if Some(org) == channel.organization.as_ref() => Ok(()),
        FunctionContext::Organization(org) => Err(CommandError::UnknownOrganization(org.clone())),
        FunctionContext::Channel(to) if to == channel => Ok(()),
        FunctionContext::Channel(to) => Err(CommandError::UnknownChannel(to.clone())),
        FunctionContext::Message { .. } => Err(CommandError::Unsupported),
    }
}

fn check_input(input: &Option<FunctionInputRef>) -> std::result::Result<(), CommandError> {
    if let Some(input) = input
        && (input.input != "user" || !matches!(input.function.as_str(), "users" | "open-channel"))
    {
        return Err(CommandError::InvalidInput(vec![issue(
            Some(input.input.clone()),
            "unknown_input",
            "Unknown function input",
        )]));
    }
    Ok(())
}

pub fn handle(cx: &mut Context, command: Command, server: &str, channel: &ChannelRef) -> Result {
    let users = [(server, server), (super::ME, "qmsg")];
    let (request, result) = match command {
        Command::Functions { request, context } => (
            request,
            check_context(&context, channel).map(|()| Reply::Functions(declarations())),
        ),
        Command::Call { request, call } => {
            let result = (|| {
                check_context(&call.context, channel)?;
                let function = declarations()
                    .into_iter()
                    .find(|f| f.id == call.function)
                    .ok_or_else(|| CommandError::UnknownFunction(call.function.clone()))?;
                function
                    .check(&call.arguments)
                    .map_err(CommandError::InvalidInput)?;
                if !matches!(
                    function.kind,
                    FunctionKind::Lookup(_) | FunctionKind::Action { .. }
                ) {
                    return Err(CommandError::Unsupported);
                }
                let selected = call.arguments.get("user");
                let input = FunctionInputRef {
                    function: call.function.clone(),
                    input: "user".into(),
                };
                if let Some(value) = selected
                    && let Verification::Invalid(issues) = check_user(value, Some(&input), server)
                {
                    return Err(CommandError::InvalidInput(issues));
                }
                if call.function == "open-channel" {
                    return Ok(Reply::Value(Value::Channel(channel.clone())));
                }
                Ok(Reply::Value(Value::List(
                    users
                        .into_iter()
                        .filter(|(id, _)| selected.is_none_or(|v| v == &Value::Text((*id).into())))
                        .map(|(id, name)| {
                            Value::Record(
                                [
                                    ("id".into(), Value::Text(id.into())),
                                    ("name".into(), Value::Text(name.into())),
                                ]
                                .into(),
                            )
                        })
                        .collect(),
                )))
            })();
            (request, result)
        }
        Command::Verify {
            request,
            verification,
        } => {
            let result = (|| {
                check_context(&verification.call.context, channel)?;
                check_helper(&verification.call.function, "verify-user")?;
                check_input(&verification.input)?;
                Ok(Reply::Verified(check_user(
                    &verification.value,
                    verification.input.as_ref(),
                    server,
                )))
            })();
            (request, result)
        }
        Command::Complete {
            request,
            completion,
        } => {
            let result = (|| {
                check_context(&completion.call.context, channel)?;
                check_helper(&completion.call.function, "complete-user")?;
                check_input(&completion.input)?;
                let offset = match completion.cursor {
                    None => 0,
                    Some(cursor) => cursor.parse::<usize>().map_err(|_| {
                        CommandError::InvalidInput(vec![issue(
                            None,
                            "bad_cursor",
                            "Invalid completion cursor",
                        )])
                    })?,
                };
                let query = completion.query.to_lowercase();
                let matches: Vec<_> = users
                    .into_iter()
                    .filter(|(id, _)| {
                        *id != super::ME
                            || !completion
                                .input
                                .as_ref()
                                .is_some_and(|i| i.function == "open-channel")
                    })
                    .filter(|(id, name)| {
                        id.to_lowercase().starts_with(&query)
                            || name.to_lowercase().starts_with(&query)
                    })
                    .collect();
                if offset > matches.len() {
                    return Err(CommandError::InvalidInput(vec![issue(
                        None,
                        "bad_cursor",
                        "Invalid completion cursor",
                    )]));
                }
                let items: Vec<_> = matches
                    .iter()
                    .skip(offset)
                    .take(completion.limit as usize)
                    .map(|(id, name)| CompletionItem {
                        label: (*name).into(),
                        description: Some((*id).into()),
                        value: Value::Text((*id).into()),
                    })
                    .collect();
                let next = offset + items.len();
                let next_cursor =
                    (completion.limit > 0 && next < matches.len()).then(|| next.to_string());
                Ok(Reply::Completed(CompletionPage { items, next_cursor }))
            })();
            (request, result)
        }
        _ => unreachable!("only function commands go to this handler"),
    };
    cx.reply(request, result)
}

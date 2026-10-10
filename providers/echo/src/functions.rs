//! Echo's functions: login, a `users` lookup, standard actions, and input
//! helpers. Message actions change only the provider's copy of its history;
//! the TCP server has no edits, reactions or read markers.

use qmsg_sdk::{
    ActionKind, Arguments, ChannelRef, Command, CommandError, CompletionItem, CompletionPage,
    Content, Context, Field, Function, FunctionContext, FunctionInput, FunctionInputRef,
    FunctionKind, InputHint, InputIssue, LoginStep, MessageEvent, ProviderStatus, Reaction, Reply,
    Result, Value, ValueType, Verification, inputs,
};

use super::{Answer, End, ME, Outgoing, SESSION, Session, now};

type Answered = std::result::Result<Reply, CommandError>;

/// The id of the `SendMessage` function.
pub const SEND: &str = "send";

fn function(id: &str, label: &str, kind: FunctionKind, inputs: Vec<FunctionInput>) -> Function {
    Function {
        id: id.into(),
        label: label.into(),
        description: String::new(),
        kind,
        inputs,
    }
}

fn user_input(id: &str) -> FunctionInput {
    FunctionInput {
        description: "User id".into(),
        verification: Some("verify-user".into()),
        completion: Some("complete-user".into()),
        ..FunctionInput::new(id, "User", ValueType::Text)
    }
}

fn reaction_input() -> FunctionInput {
    FunctionInput {
        required: true,
        completion: Some("complete-reaction".into()),
        ..FunctionInput::new(inputs::REACTION, "Reaction", ValueType::Text)
    }
}

/// What a user record from `users` holds.
fn user_record() -> ValueType {
    ValueType::Record(vec![
        Field::new("id", "Id", ValueType::Text),
        Field::new("name", "Name", ValueType::Text),
    ])
}

fn login_step(code: bool) -> (LoginStep, Function) {
    let (id, message, input) = match code {
        false => (
            "login-password",
            "Enter the password",
            FunctionInput {
                required: true,
                hint: InputHint::Secret,
                ..FunctionInput::new("password", "Password", ValueType::Text)
            },
        ),
        true => (
            "login-code",
            "Enter the login code",
            FunctionInput {
                required: true,
                ..FunctionInput::new("code", "Code", ValueType::Text)
            },
        ),
    };
    let step = LoginStep {
        message: message.into(),
        function: Some(id.into()),
        link: None,
        qr_code: None,
    };
    let mut function = Function::action(id, "Log in", ActionKind::Login);
    function.inputs.push(input);
    (step, function)
}

fn issue(input: Option<String>, code: &str, message: &str) -> InputIssue {
    InputIssue {
        input,
        code: code.into(),
        message: message.into(),
    }
}

/// Asks for the password, then the code, answering only login commands
/// meanwhile. Returns `false` on shutdown.
pub fn log_in(cx: &mut Context, password: &str, code: &str) -> Result<bool> {
    let mut asking_code = false;
    let (step, _) = login_step(false);
    cx.status(ProviderStatus::LoginRequired(step))?;
    loop {
        let Some(command) = cx.next_command(None)? else {
            continue;
        };
        let (request, result) = match command {
            Command::Shutdown => return Ok(false),
            Command::ReleaseBlob { .. } | Command::Cancel { .. } => continue,
            Command::Functions {
                request,
                context: FunctionContext::Provider,
            } => (
                request,
                Ok(Reply::Functions(vec![login_step(asking_code).1])),
            ),
            Command::Call { request, call } if call.context == FunctionContext::Provider => {
                let (_, function) = login_step(asking_code);
                if call.function != function.id {
                    cx.reply(request, Err(CommandError::UnknownFunction(call.function)))?;
                    continue;
                }
                if let Err(issues) = function.check(&call.arguments) {
                    cx.reply(request, Err(CommandError::InvalidInput(issues)))?;
                    continue;
                }
                let (input, expected) = match asking_code {
                    false => ("password", password),
                    true => ("code", code),
                };
                if call.arguments[input] != Value::Text(expected.into()) {
                    let wrong = issue(Some(input.into()), "wrong", "That is not right");
                    cx.reply(request, Err(CommandError::InvalidInput(vec![wrong])))?;
                    continue;
                }
                if !asking_code {
                    asking_code = true;
                    // The next step comes before the answer.
                    cx.status(ProviderStatus::LoginRequired(login_step(true).0))?;
                    cx.reply(request, Ok(Reply::Value(Value::Null)))?;
                    continue;
                }
                cx.secret_set(SESSION, now().to_string())?;
                cx.status(ProviderStatus::Syncing)?;
                cx.reply(request, Ok(Reply::Value(Value::Null)))?;
                return Ok(true);
            }
            other => {
                super::refuse(cx, &other, CommandError::LoginRequired)?;
                continue;
            }
        };
        cx.reply(request, result)?;
    }
}

impl Session<'_> {
    /// The functions available in `context`.
    fn declarations(
        &self,
        context: &FunctionContext,
    ) -> std::result::Result<Vec<Function>, CommandError> {
        let users = function(
            "users",
            "Find users",
            FunctionKind::Lookup(ValueType::List(Box::new(user_record()))),
            vec![user_input("user")],
        );
        let verify_user = function(
            "verify-user",
            "Check user",
            FunctionKind::Verification(ValueType::Text),
            vec![],
        );
        let complete_user = function(
            "complete-user",
            "Suggest users",
            FunctionKind::Completion(ValueType::Text),
            vec![],
        );
        let mut functions = vec![users, verify_user, complete_user];
        match context {
            FunctionContext::Provider => {}
            FunctionContext::Organization(org) if *org == self.server => {}
            FunctionContext::Organization(org) => {
                return Err(CommandError::UnknownOrganization(org.clone()));
            }
            FunctionContext::Channel(channel) if *channel == self.history.channel => {
                functions.push(Function::action(SEND, "Send", ActionKind::SendMessage));
                return Ok(functions);
            }
            FunctionContext::Channel(channel) => {
                return Err(CommandError::UnknownChannel(channel.clone()));
            }
            FunctionContext::Message { channel, id } => {
                if *channel != self.history.channel {
                    return Err(CommandError::UnknownChannel(channel.clone()));
                }
                let message = self
                    .history
                    .get(id)
                    .ok_or_else(|| CommandError::UnknownMessage(id.clone()))?;
                let mut functions = Vec::new();
                if message.author == ME {
                    functions.push(Function::action("edit", "Edit", ActionKind::EditMessage));
                    functions.push(Function::action(
                        "delete",
                        "Delete",
                        ActionKind::DeleteMessage,
                    ));
                }
                let mut add = Function::action("react", "React", ActionKind::AddReaction);
                add.inputs = vec![reaction_input()];
                let mut remove =
                    Function::action("unreact", "Remove reaction", ActionKind::RemoveReaction);
                remove.inputs = vec![reaction_input()];
                functions.extend([
                    add,
                    remove,
                    function(
                        "complete-reaction",
                        "Suggest reactions",
                        FunctionKind::Completion(ValueType::Text),
                        vec![],
                    ),
                    Function::action("mark-read", "Mark read", ActionKind::MarkRead),
                    Function::action("mark-unread", "Mark unread", ActionKind::MarkUnread),
                ]);
                return Ok(functions);
            }
        }
        let mut open = Function::action("open-channel", "Open channel", ActionKind::OpenChannel);
        // The standard input, with helpers that check and suggest one user.
        let members = &mut open.inputs[0];
        members.verification = Some("verify-user".into());
        members.completion = Some("complete-user".into());
        functions.push(open);
        if self.login {
            functions.push(Function::action("logout", "Log out", ActionKind::Logout));
        }
        Ok(functions)
    }

    /// Finds a function declared in `context`, of the kind `kind` matches.
    fn find(
        &self,
        context: &FunctionContext,
        id: &str,
        kind: fn(&FunctionKind) -> bool,
    ) -> std::result::Result<Function, CommandError> {
        let function = self
            .declarations(context)?
            .into_iter()
            .find(|f| f.id == id)
            .ok_or_else(|| CommandError::UnknownFunction(id.into()))?;
        match kind(&function.kind) {
            true => Ok(function),
            false => Err(CommandError::Unsupported),
        }
    }

    fn check_user(&self, value: &Value, input: Option<&FunctionInputRef>) -> Verification {
        let field = input.map(|i| i.input.clone());
        let problem = match value {
            Value::Text(id) if id == ME && input.is_some_and(|i| i.function == "open-channel") => {
                issue(
                    field,
                    "no_conversation",
                    "Only the server has a conversation",
                )
            }
            Value::Text(id) if *id == self.server || id == ME => return Verification::Valid,
            Value::Text(_) => issue(field, "unknown_user", "This user does not exist"),
            _ => issue(field, "wrong_type", "Expected a user id as text"),
        };
        Verification::Invalid(vec![problem])
    }

    /// Calls a lookup or action, returning how the session ends, if it does.
    fn call(
        &mut self,
        cx: &mut Context,
        request: u64,
        context: FunctionContext,
        id: &str,
        mut arguments: Arguments,
    ) -> Result<Option<End>> {
        let callable =
            |k: &FunctionKind| matches!(k, FunctionKind::Lookup(_) | FunctionKind::Action { .. });
        let function = match self.find(&context, id, callable) {
            Ok(function) => function,
            Err(e) => return cx.reply(request, Err(e)).map(|()| None),
        };
        if let Err(issues) = function.check(&arguments) {
            let error = CommandError::InvalidInput(issues);
            // A send that never reached the server, like a refused `Send`.
            if let (
                FunctionKind::Action {
                    action: ActionKind::SendMessage,
                    ..
                },
                FunctionContext::Channel(channel),
                Some(Value::Text(nonce)),
            ) = (&function.kind, &context, arguments.get(inputs::NONCE))
            {
                cx.emit(MessageEvent::NotSent {
                    channel: channel.clone(),
                    nonce: nonce.clone(),
                    error: error.clone(),
                })?;
            }
            cx.reply(request, Err(error))?;
            return Ok(None);
        }
        let FunctionKind::Action { action, .. } = function.kind else {
            let result = self.users(arguments.get("user"));
            return cx.reply(request, result).map(|()| None);
        };
        let text = |arguments: &mut Arguments, id: &str| match arguments.remove(id) {
            Some(Value::Text(text)) => Some(text),
            _ => None,
        };
        let result = match (action, context) {
            (ActionKind::SendMessage, FunctionContext::Channel(channel)) => {
                let Some(Value::Content(content)) = arguments.remove(inputs::CONTENT) else {
                    unreachable!("checked above");
                };
                let send = Outgoing {
                    request,
                    answer: Answer::Value,
                    channel,
                    reply_to: text(&mut arguments, inputs::REPLY_TO),
                    content,
                    nonce: text(&mut arguments, inputs::NONCE),
                };
                return self.send(cx, send).map(|()| None);
            }
            (ActionKind::OpenChannel, context) => {
                let Some(Value::List(members)) = arguments.remove(inputs::MEMBERS) else {
                    unreachable!("checked above");
                };
                let members: Vec<_> = members
                    .into_iter()
                    .filter_map(|m| match m {
                        Value::Text(m) => Some(m),
                        _ => None,
                    })
                    .collect();
                for member in &members {
                    let input = FunctionInputRef {
                        function: id.into(),
                        input: inputs::MEMBERS.into(),
                    };
                    if let Verification::Invalid(issues) =
                        self.check_user(&Value::Text(member.clone()), Some(&input))
                    {
                        return cx
                            .reply(request, Err(CommandError::InvalidInput(issues)))
                            .map(|()| None);
                    }
                }
                let organization = match context {
                    FunctionContext::Organization(org) => org,
                    _ => self.server.clone(),
                };
                self.open_channel(Some(&organization), &members)
                    .map(|c| Reply::Value(Value::Channel(c)))
            }
            (ActionKind::Logout, _) => {
                cx.secret_delete(SESSION)?;
                self.fail_sends(cx, "logged out")?;
                // What follows the answer: the account is gone, and the
                // provider starts over.
                cx.directory(qmsg_sdk::DirectoryUpdate::Reset)?;
                cx.status(ProviderStatus::Connecting)?;
                cx.reply(request, Ok(Reply::Value(Value::Null)))?;
                return Ok(Some(End::LoggedOut));
            }
            (action, FunctionContext::Message { channel, id }) => {
                let reaction = text(&mut arguments, inputs::REACTION);
                self.message_action(cx, action, channel, id, reaction, arguments)?
            }
            _ => Err(CommandError::Unsupported),
        };
        cx.reply(request, result)?;
        Ok(None)
    }

    fn users(&self, selected: Option<&Value>) -> Answered {
        if let Some(value) = selected
            && let Verification::Invalid(issues) = self.check_user(value, None)
        {
            return Err(CommandError::InvalidInput(issues));
        }
        let users = [(self.server.as_str(), self.server.as_str()), (ME, "qmsg")];
        Ok(Reply::Value(Value::List(
            users
                .into_iter()
                .filter(|(id, _)| selected.is_none_or(|v| *v == Value::Text((*id).into())))
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
    }

    /// Changes the provider's copy of a message, emitting the change before
    /// the answer.
    fn message_action(
        &mut self,
        cx: &mut Context,
        action: ActionKind,
        channel: ChannelRef,
        id: String,
        reaction: Option<String>,
        mut arguments: Arguments,
    ) -> Result<Answered> {
        let position = self.history.position(&id).expect("declared for it");
        let event = match action {
            ActionKind::EditMessage => {
                let Some(Value::Content(content)) = arguments.remove(inputs::CONTENT) else {
                    unreachable!("checked before");
                };
                if let Err(violation) = self.channel.check(&content) {
                    return Ok(Err(violation.into()));
                }
                if content
                    .iter()
                    .any(|c| !matches!(c, Content::Text(_) | Content::Formatted(_)))
                {
                    return Ok(Err(CommandError::Unsupported));
                }
                let edited_at = now();
                self.history.set_text(position, content.clone());
                self.history.message_mut(position).edited_at = Some(edited_at);
                MessageEvent::Edited {
                    channel,
                    id,
                    content,
                    edited_at,
                }
            }
            ActionKind::DeleteMessage => {
                self.history.remove(position);
                MessageEvent::Deleted { channel, id }
            }
            ActionKind::AddReaction | ActionKind::RemoveReaction => {
                let key = reaction.expect("checked before");
                let added = action == ActionKind::AddReaction;
                let reactions = &mut self.history.message_mut(position).reactions;
                let found = reactions.iter().position(|r| r.key == key);
                match (found, added) {
                    (Some(i), true) if reactions[i].me => {}
                    (Some(i), true) => {
                        reactions[i].count += 1;
                        reactions[i].me = true;
                    }
                    (None, true) => reactions.push(Reaction {
                        key: key.clone(),
                        count: 1,
                        me: true,
                    }),
                    (Some(i), false) if reactions[i].me => {
                        reactions[i].count -= 1;
                        reactions[i].me = false;
                        if reactions[i].count == 0 {
                            reactions.remove(i);
                        }
                    }
                    _ => {
                        let input = Some(inputs::REACTION.into());
                        let issue = issue(input, "not_reacted", "You haven't reacted with this");
                        return Ok(Err(CommandError::InvalidInput(vec![issue])));
                    }
                }
                MessageEvent::Reacted {
                    channel,
                    id,
                    user: ME.into(),
                    key,
                    added,
                }
            }
            ActionKind::MarkRead => MessageEvent::Read {
                channel,
                user: ME.into(),
                up_to: Some(id),
            },
            ActionKind::MarkUnread => MessageEvent::Read {
                channel,
                user: ME.into(),
                up_to: position
                    .checked_sub(1)
                    .map(|i| self.history.messages[i].message.id.clone()),
            },
            _ => return Ok(Err(CommandError::Unsupported)),
        };
        cx.emit(event)?;
        Ok(Ok(Reply::Value(Value::Null)))
    }

    fn complete(
        &self,
        id: &str,
        input: Option<&FunctionInputRef>,
        query: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Answered {
        let bad_cursor = || {
            CommandError::InvalidInput(vec![issue(None, "bad_cursor", "Invalid completion cursor")])
        };
        let offset = match cursor {
            None => 0,
            Some(cursor) => cursor.parse::<usize>().map_err(|_| bad_cursor())?,
        };
        let options: Vec<(&str, &str)> = match id {
            "complete-reaction" => vec![("👍", "Thumbs up"), ("❤️", "Heart"), ("😂", "Laughing")],
            _ => vec![(self.server.as_str(), self.server.as_str()), (ME, "qmsg")]
                .into_iter()
                .filter(|(id, _)| *id != ME || !input.is_some_and(|i| i.function == "open-channel"))
                .collect(),
        };
        let query = query.to_lowercase();
        let matches: Vec<_> = options
            .into_iter()
            .filter(|(value, label)| {
                value.to_lowercase().starts_with(&query) || label.to_lowercase().starts_with(&query)
            })
            .collect();
        if offset > matches.len() {
            return Err(bad_cursor());
        }
        let items: Vec<_> = matches
            .iter()
            .skip(offset)
            .take(limit as usize)
            .map(|(value, label)| CompletionItem {
                label: (*label).into(),
                description: Some((*value).into()),
                value: Value::Text((*value).into()),
            })
            .collect();
        let next = offset + items.len();
        let next_cursor = (limit > 0 && next < matches.len()).then(|| next.to_string());
        Ok(Reply::Completed(CompletionPage { items, next_cursor }))
    }

    /// Checks a helper's input reference names one of its inputs.
    fn check_input(
        &self,
        context: &FunctionContext,
        helper: &str,
        input: Option<&FunctionInputRef>,
    ) -> std::result::Result<(), CommandError> {
        let Some(input) = input else { return Ok(()) };
        let linked = self.declarations(context)?.into_iter().any(|f| {
            f.id == input.function
                && f.inputs.iter().any(|i| {
                    i.id == input.input
                        && (i.verification.as_deref() == Some(helper)
                            || i.completion.as_deref() == Some(helper))
                })
        });
        match linked {
            true => Ok(()),
            false => Err(CommandError::InvalidInput(vec![issue(
                Some(input.input.clone()),
                "unknown_input",
                "Unknown function input",
            )])),
        }
    }
}

/// Handles a function command, returning how the session ends, if it does.
pub fn handle(session: &mut Session, cx: &mut Context, command: Command) -> Result<Option<End>> {
    let (request, result) = match command {
        Command::Functions { request, context } => (
            request,
            session.declarations(&context).map(Reply::Functions),
        ),
        Command::Call { request, call } => {
            return session.call(cx, request, call.context, &call.function, call.arguments);
        }
        Command::Verify {
            request,
            verification,
        } => {
            let call = &verification.call;
            let result = session
                .find(&call.context, &call.function, |k| {
                    matches!(k, FunctionKind::Verification(_))
                })
                .and_then(|_| {
                    let input = verification.input.as_ref();
                    session.check_input(&call.context, &call.function, input)?;
                    Ok(Reply::Verified(
                        session.check_user(&verification.value, input),
                    ))
                });
            (request, result)
        }
        Command::Complete {
            request,
            completion,
        } => {
            let call = &completion.call;
            let result = session
                .find(&call.context, &call.function, |k| {
                    matches!(k, FunctionKind::Completion(_))
                })
                .and_then(|_| {
                    let input = completion.input.as_ref();
                    session.check_input(&call.context, &call.function, input)?;
                    session.complete(
                        &call.function,
                        input,
                        &completion.query,
                        completion.cursor.as_deref(),
                        completion.limit,
                    )
                });
            (request, result)
        }
        _ => unreachable!("only function commands go to this handler"),
    };
    cx.reply(request, result)?;
    Ok(None)
}

//! Runtime-owned, non-blocking Init state machine.
//!
//! [`RuntimeInitMachine`] drives the same one-time spine as the test-only
//! linear `run_init_sequence`, but as a poll-based state machine that yields
//! back to the runtime loop between steps instead of blocking. The per-step
//! Engine API helpers it calls live in [`super::steps`].

use super::*;

pub(crate) enum RuntimeInitPoll {
    Pending { changed: bool },
    Ready(InitResult),
    Failed(ConnectError),
}

pub(crate) struct RuntimeInitMachine {
    cfg: InitConfig,
    started: Instant,
    init_started_at: Instant,
    connect_timeout: Duration,
    step_timeout: Duration,
    phase: RuntimeInitPhase,
    result: InitResult,
    waiting_update: bool,
    base_errors_before: usize,
    auth_block_errors_before: usize,
    base_status: CriticalInitStatus,
    auth_status: CriticalInitStatus,
    strategy_schema: Option<PendingStrategySchemaStep>,
}

enum RuntimeInitPhase {
    WaitAuthorized,
    ServerUpdateAuthWait {
        waits_done: u8,
        next_at: Instant,
    },
    SendBaseCheck {
        attempt: BaseAttempt,
    },
    WaitBaseCheck {
        attempt: BaseAttempt,
        pending: PendingEngineInit,
    },
    BaseUpdateRetryPause {
        next_retry: u8,
        next_at: Instant,
    },
    SendAuthCheck {
        attempt: AuthAttempt,
    },
    WaitAuthCheck {
        attempt: AuthAttempt,
        pending: PendingEngineInit,
    },
    InitAuthRetryPause {
        next_at: Instant,
    },
    SendGetMarketsList,
    WaitGetMarketsList {
        pending: PendingEngineInit,
    },
    SendUpdateMarketsList,
    WaitUpdateMarketsList {
        pending: PendingEngineInit,
    },
    WaitStrategySchema,
    PostInit,
    PostInitFlush {
        until: Instant,
    },
    Done,
}

#[derive(Clone, Copy)]
enum BaseAttempt {
    First,
    UpdateRetry { retry_no: u8 },
    InitRetry,
}

#[derive(Clone, Copy)]
enum AuthAttempt {
    First,
    InitRetry,
}

impl RuntimeInitMachine {
    pub(crate) fn new(cfg: ConnectConfig, dispatcher: &mut crate::events::EventDispatcher) -> Self {
        if let Some(initial) = cfg.init.initial_strategies.as_ref() {
            dispatcher.set_local_strategy_epoch(initial.epoch);
            dispatcher.set_local_strategies(&initial.strategies);
        }
        let now = Instant::now();
        let step_timeout = cfg.init.step_timeout.unwrap_or(Duration::from_millis(
            crate::api_pending::DEFAULT_PENDING_TIMEOUT_MS as u64,
        ));
        Self {
            connect_timeout: cfg.connect_timeout,
            step_timeout,
            cfg: cfg.init,
            started: now,
            init_started_at: now,
            phase: RuntimeInitPhase::WaitAuthorized,
            result: InitResult::default(),
            waiting_update: false,
            base_errors_before: 0,
            auth_block_errors_before: 0,
            base_status: CriticalInitStatus::Skipped,
            auth_status: CriticalInitStatus::Skipped,
            strategy_schema: None,
        }
    }

    pub(crate) fn poll(
        &mut self,
        client: &mut Client,
        dispatcher: &mut crate::events::EventDispatcher,
    ) -> RuntimeInitPoll {
        if client.shutdown_requested() {
            client.disconnect();
            return RuntimeInitPoll::Failed(ConnectError::Canceled);
        }

        let mut changed = false;
        loop {
            let phase = std::mem::replace(&mut self.phase, RuntimeInitPhase::Done);
            match phase {
                RuntimeInitPhase::WaitAuthorized => {
                    if client.is_authorized() {
                        self.init_started_at = Instant::now();
                        self.waiting_update = client.take_server_update_sent();
                        self.auth_block_errors_before = self.result.errors.len();
                        self.base_errors_before = self.result.errors.len();
                        self.phase = if self.waiting_update {
                            RuntimeInitPhase::ServerUpdateAuthWait {
                                waits_done: 0,
                                next_at: Instant::now(),
                            }
                        } else {
                            RuntimeInitPhase::SendBaseCheck {
                                attempt: BaseAttempt::First,
                            }
                        };
                        continue;
                    }
                    if timeout_remaining(self.started, self.connect_timeout).is_none() {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::ConnectTimedOut {
                            timeout: self.connect_timeout,
                        });
                    }
                    self.phase = RuntimeInitPhase::WaitAuthorized;
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::ServerUpdateAuthWait {
                    mut waits_done,
                    mut next_at,
                } => {
                    if client.is_authorized()
                        || waits_done >= DELPHI_BASE_CHECK_UPDATE_AUTH_WAITS as u8
                    {
                        self.phase = RuntimeInitPhase::SendBaseCheck {
                            attempt: BaseAttempt::First,
                        };
                        continue;
                    }
                    let now = Instant::now();
                    if now >= next_at {
                        waits_done = waits_done.saturating_add(1);
                        next_at =
                            now + Duration::from_millis(DELPHI_BASE_CHECK_UPDATE_AUTH_WAIT_MS);
                    }
                    self.phase = RuntimeInitPhase::ServerUpdateAuthWait {
                        waits_done,
                        next_at,
                    };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::SendBaseCheck { attempt } => {
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::base_check(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitBaseCheck { attempt, pending };
                    continue;
                }
                RuntimeInitPhase::WaitBaseCheck {
                    attempt,
                    mut pending,
                } => match poll_engine_init_step(client, &mut pending) {
                    PendingEnginePoll::Pending => {
                        self.phase = RuntimeInitPhase::WaitBaseCheck { attempt, pending };
                        return RuntimeInitPoll::Pending { changed };
                    }
                    PendingEnginePoll::Response(resp) => {
                        let status = self.apply_base_check_response(client, resp);
                        if status.is_ok() {
                            fire_init_step(client, "BaseCheck", self.init_started_at);
                            self.ensure_strategy_schema_started(client, dispatcher);
                        }
                        match attempt {
                            BaseAttempt::First => {
                                self.base_status = status;
                                if self.waiting_update && !self.base_status.is_ok() {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else if self.base_status.is_ok() {
                                    self.phase = RuntimeInitPhase::SendAuthCheck {
                                        attempt: AuthAttempt::First,
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::UpdateRetry { retry_no } => {
                                self.base_status = status;
                                if self.base_status.is_ok() {
                                    self.result.errors.truncate(self.base_errors_before);
                                    self.phase = RuntimeInitPhase::SendAuthCheck {
                                        attempt: AuthAttempt::First,
                                    };
                                } else if retry_no < DELPHI_BASE_CHECK_UPDATE_RETRIES as u8 {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: retry_no + 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::InitRetry => {
                                self.base_status = status;
                                self.phase = RuntimeInitPhase::SendAuthCheck {
                                    attempt: AuthAttempt::InitRetry,
                                };
                                continue;
                            }
                        }
                    }
                    PendingEnginePoll::Timeout => {
                        self.result.errors.push("BaseCheck timeout".to_string());
                        let status = CriticalInitStatus::TimedOut;
                        match attempt {
                            BaseAttempt::First => {
                                self.base_status = status;
                                if self.waiting_update {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::UpdateRetry { retry_no } => {
                                self.base_status = status;
                                if retry_no < DELPHI_BASE_CHECK_UPDATE_RETRIES as u8 {
                                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                                        next_retry: retry_no + 1,
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_BASE_CHECK_UPDATE_RETRY_PAUSE_MS,
                                            ),
                                    };
                                } else {
                                    self.auth_status = CriticalInitStatus::Skipped;
                                    self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                        next_at: Instant::now()
                                            + Duration::from_millis(
                                                DELPHI_INIT_AUTH_RETRY_PAUSE_MS,
                                            ),
                                    };
                                }
                                continue;
                            }
                            BaseAttempt::InitRetry => {
                                self.base_status = status;
                                self.phase = RuntimeInitPhase::SendAuthCheck {
                                    attempt: AuthAttempt::InitRetry,
                                };
                                continue;
                            }
                        }
                    }
                    PendingEnginePoll::Disconnected => {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::from(
                            InitError::SendChannelClosed,
                        ));
                    }
                },
                RuntimeInitPhase::BaseUpdateRetryPause {
                    next_retry,
                    next_at,
                } => {
                    if Instant::now() >= next_at {
                        self.phase = RuntimeInitPhase::SendBaseCheck {
                            attempt: BaseAttempt::UpdateRetry {
                                retry_no: next_retry,
                            },
                        };
                        continue;
                    }
                    self.phase = RuntimeInitPhase::BaseUpdateRetryPause {
                        next_retry,
                        next_at,
                    };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::SendAuthCheck { attempt } => {
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::auth_check(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitAuthCheck { attempt, pending };
                    continue;
                }
                RuntimeInitPhase::WaitAuthCheck {
                    attempt,
                    mut pending,
                } => {
                    let status = match poll_engine_init_step(client, &mut pending) {
                        PendingEnginePoll::Pending => {
                            self.phase = RuntimeInitPhase::WaitAuthCheck { attempt, pending };
                            return RuntimeInitPoll::Pending { changed };
                        }
                        PendingEnginePoll::Response(resp) => {
                            self.apply_auth_check_response(client, resp)
                        }
                        PendingEnginePoll::Timeout => {
                            self.result.errors.push("AuthCheck timeout".to_string());
                            CriticalInitStatus::TimedOut
                        }
                        PendingEnginePoll::Disconnected => {
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(
                                InitError::SendChannelClosed,
                            ));
                        }
                    };
                    if status.is_ok() {
                        fire_init_step(client, "AuthCheck", self.init_started_at);
                    }
                    match attempt {
                        AuthAttempt::First => {
                            self.auth_status = status;
                            if !self.base_status.is_ok() || !self.auth_status.is_ok() {
                                self.phase = RuntimeInitPhase::InitAuthRetryPause {
                                    next_at: Instant::now()
                                        + Duration::from_millis(DELPHI_INIT_AUTH_RETRY_PAUSE_MS),
                                };
                            } else {
                                self.phase = RuntimeInitPhase::SendGetMarketsList;
                            }
                            continue;
                        }
                        AuthAttempt::InitRetry => {
                            self.auth_status = status;
                            if self.auth_status.is_ok() {
                                self.result.errors.truncate(self.auth_block_errors_before);
                                if self.strategy_schema.is_none() {
                                    self.ensure_strategy_schema_started(client, dispatcher);
                                }
                                self.phase = RuntimeInitPhase::SendGetMarketsList;
                                continue;
                            }
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(
                                self.auth_status
                                    .final_error("AuthCheck")
                                    .unwrap_or(InitError::CriticalStepTimedOut("AuthCheck")),
                            ));
                        }
                    }
                }
                RuntimeInitPhase::InitAuthRetryPause { next_at } => {
                    if Instant::now() >= next_at {
                        self.phase = RuntimeInitPhase::SendBaseCheck {
                            attempt: BaseAttempt::InitRetry,
                        };
                        continue;
                    }
                    self.phase = RuntimeInitPhase::InitAuthRetryPause { next_at };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::SendGetMarketsList => {
                    if self.strategy_schema.is_none() {
                        self.ensure_strategy_schema_started(client, dispatcher);
                    }
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::get_markets_list(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitGetMarketsList { pending };
                    continue;
                }
                RuntimeInitPhase::WaitGetMarketsList { mut pending } => {
                    let resp = match self.poll_required_engine_response(
                        client,
                        &mut pending,
                        "GetMarketsList",
                    ) {
                        Ok(Some(resp)) => resp,
                        Ok(None) => {
                            self.phase = RuntimeInitPhase::WaitGetMarketsList { pending };
                            return RuntimeInitPoll::Pending { changed };
                        }
                        Err(err) => {
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(err));
                        }
                    };
                    if let Err(err) = apply_required_get_markets_list_response(
                        dispatcher,
                        &resp,
                        &mut self.result,
                    ) {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::from(err));
                    }
                    self.result.markets_response_bytes = resp.data.len();
                    fire_init_step(client, "GetMarketsList", self.init_started_at);
                    client.reconnect.tracked_indexes_peer_app_token = client.peer_app_token;
                    self.phase = RuntimeInitPhase::SendUpdateMarketsList;
                    changed = true;
                    continue;
                }
                RuntimeInitPhase::SendUpdateMarketsList => {
                    let pending = begin_engine_init_step(
                        client,
                        crate::commands::engine_request::update_markets_list(),
                        self.step_timeout,
                    );
                    self.phase = RuntimeInitPhase::WaitUpdateMarketsList { pending };
                    continue;
                }
                RuntimeInitPhase::WaitUpdateMarketsList { mut pending } => {
                    let resp = match self.poll_required_engine_response(
                        client,
                        &mut pending,
                        "UpdateMarketsList",
                    ) {
                        Ok(Some(resp)) => resp,
                        Ok(None) => {
                            self.phase = RuntimeInitPhase::WaitUpdateMarketsList { pending };
                            return RuntimeInitPoll::Pending { changed };
                        }
                        Err(err) => {
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(err));
                        }
                    };
                    if let Err(err) = apply_required_update_markets_list_response(
                        dispatcher,
                        &resp,
                        &mut self.result,
                    ) {
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Failed(ConnectError::from(err));
                    }
                    self.result.update_markets_response_bytes = resp.data.len();
                    fire_init_step(client, "UpdateMarketsList", self.init_started_at);
                    client.subscriptions.domain_restore = DomainRestoreIntent {
                        fetch_indexes: true,
                    };
                    client.set_domain_ready(true);
                    self.phase = RuntimeInitPhase::WaitStrategySchema;
                    changed = true;
                    continue;
                }
                RuntimeInitPhase::WaitStrategySchema => {
                    let timeout = self.step_timeout;
                    let Some(pending) = self.strategy_schema.as_mut() else {
                        self.ensure_strategy_schema_started(client, dispatcher);
                        self.phase = RuntimeInitPhase::WaitStrategySchema;
                        continue;
                    };
                    match poll_required_strategy_schema_step(
                        client,
                        dispatcher,
                        &mut self.result,
                        pending,
                        timeout,
                    ) {
                        StrategySchemaPoll::Ready => {
                            fire_init_step(client, "StrategySchema", self.init_started_at);
                            self.phase = RuntimeInitPhase::PostInit;
                            changed = true;
                            continue;
                        }
                        StrategySchemaPoll::Pending => {
                            self.phase = RuntimeInitPhase::WaitStrategySchema;
                            return RuntimeInitPoll::Pending { changed };
                        }
                        StrategySchemaPoll::Failed(err) => {
                            client.set_domain_ready(false);
                            self.phase = RuntimeInitPhase::Done;
                            return RuntimeInitPoll::Failed(ConnectError::from(err));
                        }
                    }
                }
                RuntimeInitPhase::PostInit => {
                    send_post_init_resync(client, dispatcher, &self.cfg, &mut self.result);
                    client.send_registry_subscriptions_after_init();
                    if let Some(mode) = self.cfg.subscribe_trades {
                        client.subscribe_all_trades(mode.want_market_makers());
                        self.result.trades_subscribed = true;
                    }
                    for name in &self.cfg.subscribe_orderbooks {
                        client.subscribe_orderbook(name);
                        self.result.orderbooks_subscribed += 1;
                    }
                    self.phase = RuntimeInitPhase::PostInitFlush {
                        until: Instant::now() + Duration::from_millis(100),
                    };
                    changed = true;
                    continue;
                }
                RuntimeInitPhase::PostInitFlush { until } => {
                    if Instant::now() >= until {
                        fire_init_step(client, "PostInitFlush", self.init_started_at);
                        self.phase = RuntimeInitPhase::Done;
                        return RuntimeInitPoll::Ready(std::mem::take(&mut self.result));
                    }
                    self.phase = RuntimeInitPhase::PostInitFlush { until };
                    return RuntimeInitPoll::Pending { changed };
                }
                RuntimeInitPhase::Done => {
                    self.phase = RuntimeInitPhase::Done;
                    return RuntimeInitPoll::Pending { changed };
                }
            }
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn profile_source(&self) -> (u8, u8) {
        use crate::commands::engine_api::EngineMethod;

        match self.phase {
            RuntimeInitPhase::SendBaseCheck { .. }
            | RuntimeInitPhase::WaitBaseCheck { .. }
            | RuntimeInitPhase::BaseUpdateRetryPause { .. } => {
                (Command::API.to_byte(), EngineMethod::BaseCheck.to_byte())
            }
            RuntimeInitPhase::SendAuthCheck { .. }
            | RuntimeInitPhase::WaitAuthCheck { .. }
            | RuntimeInitPhase::InitAuthRetryPause { .. } => {
                (Command::API.to_byte(), EngineMethod::AuthCheck.to_byte())
            }
            RuntimeInitPhase::SendGetMarketsList | RuntimeInitPhase::WaitGetMarketsList { .. } => (
                Command::API.to_byte(),
                EngineMethod::GetMarketsList.to_byte(),
            ),
            RuntimeInitPhase::SendUpdateMarketsList
            | RuntimeInitPhase::WaitUpdateMarketsList { .. } => (
                Command::API.to_byte(),
                EngineMethod::UpdateMarketsList.to_byte(),
            ),
            RuntimeInitPhase::WaitStrategySchema => (Command::Strat.to_byte(), u8::MAX),
            RuntimeInitPhase::PostInit | RuntimeInitPhase::PostInitFlush { .. } => {
                (Command::Grouped.to_byte(), u8::MAX)
            }
            RuntimeInitPhase::WaitAuthorized
            | RuntimeInitPhase::ServerUpdateAuthWait { .. }
            | RuntimeInitPhase::Done => (u8::MAX, u8::MAX),
        }
    }

    fn ensure_strategy_schema_started(
        &mut self,
        client: &mut Client,
        dispatcher: &crate::events::EventDispatcher,
    ) {
        if self.strategy_schema.is_none() {
            self.strategy_schema = Some(begin_required_strategy_schema_step(client, dispatcher));
        }
    }

    fn apply_base_check_response(
        &mut self,
        client: &mut Client,
        resp: EngineResponse,
    ) -> CriticalInitStatus {
        if resp.success {
            self.result.base_check_ok = true;
            let info = parse_base_check_response(&resp.data);
            client.set_server_info(info);
            CriticalInitStatus::Ok
        } else {
            let message = response_error_message(&resp);
            self.result
                .errors
                .push(format!("BaseCheck error: {message}"));
            CriticalInitStatus::Failed(message)
        }
    }

    fn apply_auth_check_response(
        &mut self,
        client: &mut Client,
        resp: EngineResponse,
    ) -> CriticalInitStatus {
        if resp.success {
            let len = resp.data.len();
            match parse_auth_check_response(&resp.data) {
                Some(auth) => {
                    client.set_auth_info(auth.clone());
                    self.result.auth_info = Some(auth);
                }
                None => {
                    self.result
                        .errors
                        .push(format!("AuthCheck parse: malformed payload ({len} bytes)"));
                }
            }
            self.result.auth_check_ok = true;
            CriticalInitStatus::Ok
        } else {
            let message = response_error_message(&resp);
            self.result
                .errors
                .push(format!("AuthCheck error: {message}"));
            CriticalInitStatus::Failed(message)
        }
    }

    fn poll_required_engine_response(
        &mut self,
        client: &mut Client,
        pending: &mut PendingEngineInit,
        step: &'static str,
    ) -> Result<Option<EngineResponse>, InitError> {
        match poll_engine_init_step(client, pending) {
            PendingEnginePoll::Pending => Ok(None),
            PendingEnginePoll::Response(resp) if resp.success => Ok(Some(resp)),
            PendingEnginePoll::Response(resp) => {
                let message = response_error_message(&resp);
                self.result.errors.push(format!("{step} error: {message}"));
                Err(InitError::CriticalStepFailed { step, message })
            }
            PendingEnginePoll::Timeout => {
                self.result.errors.push(format!("{step}: timeout"));
                Err(InitError::CriticalStepTimedOut(step))
            }
            PendingEnginePoll::Disconnected => Err(InitError::SendChannelClosed),
        }
    }
}

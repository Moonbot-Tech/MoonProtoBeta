//! Account-level state maintained by Active Lib.
//!
//! Delphi keeps account-mode/API-expiration checks in worker paths and then
//! updates application state/UI. Normal Rust applications should use async
//! refresh intents and read this state, not block UI code on scalar Engine API
//! requests.

use crate::commands::engine_api::{
    parse_api_expiration_time_response, parse_query_hedge_mode_response, ApiExpirationTime,
    EngineMethod, EngineResponse,
};

#[derive(Debug, Clone, Default)]
pub struct AccountState {
    hedge_mode: Option<bool>,
    hedge_mode_request_uid: Option<u64>,
    api_expiration: Option<ApiExpirationTime>,
    api_expiration_request_uid: Option<u64>,
    revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AccountEvent {
    HedgeModeUpdated {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        request_uid: u64,
        hedge_mode: bool,
        revision: u64,
    },
    HedgeModeUpdateFailed {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        request_uid: Option<u64>,
        error: String,
    },
    ApiExpirationUpdated {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        request_uid: u64,
        expiration: ApiExpirationTime,
        revision: u64,
    },
    ApiExpirationUpdateFailed {
        #[cfg(any(test, feature = "diagnostics"))]
        #[doc(hidden)]
        request_uid: Option<u64>,
        error: String,
    },
}

impl AccountState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hedge_mode(&self) -> Option<bool> {
        self.hedge_mode
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "diagnostics"))]
    pub fn hedge_mode_request_uid(&self) -> Option<u64> {
        self.hedge_mode_request_uid
    }

    pub fn api_expiration(&self) -> Option<ApiExpirationTime> {
        self.api_expiration
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "diagnostics"))]
    pub fn api_expiration_request_uid(&self) -> Option<u64> {
        self.api_expiration_request_uid
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn apply_hedge_mode_response(&mut self, resp: EngineResponse) -> AccountEvent {
        if resp.method != EngineMethod::QueryHedgeMode {
            return AccountEvent::HedgeModeUpdateFailed {
                #[cfg(any(test, feature = "diagnostics"))]
                request_uid: Some(resp.request_uid),
                error: format!("unexpected EngineMethod {:?}", resp.method),
            };
        }
        if !resp.success {
            return AccountEvent::HedgeModeUpdateFailed {
                #[cfg(any(test, feature = "diagnostics"))]
                request_uid: Some(resp.request_uid),
                error: format!("server error {} {}", resp.error_code, resp.error_msg.trim()),
            };
        }
        let Some(hedge_mode) = parse_query_hedge_mode_response(&resp.data) else {
            return AccountEvent::HedgeModeUpdateFailed {
                #[cfg(any(test, feature = "diagnostics"))]
                request_uid: Some(resp.request_uid),
                error: format!("parse failed data_len={}", resp.data.len()),
            };
        };
        self.hedge_mode = Some(hedge_mode);
        self.hedge_mode_request_uid = Some(resp.request_uid);
        self.revision = self.revision.wrapping_add(1);
        AccountEvent::HedgeModeUpdated {
            #[cfg(any(test, feature = "diagnostics"))]
            request_uid: resp.request_uid,
            hedge_mode,
            revision: self.revision,
        }
    }

    pub(crate) fn apply_api_expiration_response(&mut self, resp: EngineResponse) -> AccountEvent {
        if resp.method != EngineMethod::CheckAPIExpirationTime {
            return AccountEvent::ApiExpirationUpdateFailed {
                #[cfg(any(test, feature = "diagnostics"))]
                request_uid: Some(resp.request_uid),
                error: format!("unexpected EngineMethod {:?}", resp.method),
            };
        }
        if !resp.success {
            return AccountEvent::ApiExpirationUpdateFailed {
                #[cfg(any(test, feature = "diagnostics"))]
                request_uid: Some(resp.request_uid),
                error: format!("server error {} {}", resp.error_code, resp.error_msg.trim()),
            };
        }
        let Some(expiration) = parse_api_expiration_time_response(&resp.data) else {
            return AccountEvent::ApiExpirationUpdateFailed {
                #[cfg(any(test, feature = "diagnostics"))]
                request_uid: Some(resp.request_uid),
                error: format!("parse failed data_len={}", resp.data.len()),
            };
        };
        self.api_expiration = Some(expiration);
        self.api_expiration_request_uid = Some(resp.request_uid);
        self.revision = self.revision.wrapping_add(1);
        AccountEvent::ApiExpirationUpdated {
            #[cfg(any(test, feature = "diagnostics"))]
            request_uid: resp.request_uid,
            expiration,
            revision: self.revision,
        }
    }

    pub(crate) fn hedge_mode_request_failed(
        &mut self,
        request_uid: Option<u64>,
        error: impl Into<String>,
    ) -> AccountEvent {
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = request_uid;
        AccountEvent::HedgeModeUpdateFailed {
            #[cfg(any(test, feature = "diagnostics"))]
            request_uid,
            error: error.into(),
        }
    }

    pub(crate) fn api_expiration_request_failed(
        &mut self,
        request_uid: Option<u64>,
        error: impl Into<String>,
    ) -> AccountEvent {
        #[cfg(not(any(test, feature = "diagnostics")))]
        let _ = request_uid;
        AccountEvent::ApiExpirationUpdateFailed {
            #[cfg(any(test, feature = "diagnostics"))]
            request_uid,
            error: error.into(),
        }
    }
}

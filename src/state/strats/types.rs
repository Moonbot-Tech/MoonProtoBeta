/// Cached serialized `TStrategySerializer` payload for replying to
/// `TStratSnapshotRequest`.
#[derive(Debug, Clone)]
pub(crate) struct StrategySnapshotPayloadCache {
    pub client_max_last_date: u64,
    pub data: Vec<u8>,
}

/// Информация по одной стратегии — то что хранится клиентом.
#[derive(Debug, Clone)]
pub struct StrategyInfo {
    /// Уникальный идентификатор стратегии (от сервера). 0 = не валидный.
    pub strategy_id: u64,
    /// Версия стратегии из `TStrategySerializer` header.
    pub strategy_ver: i32,
    /// Время последнего апдейта (TDateTime f64 packed как UInt64).
    pub last_date: u64,
    /// Цена продажи из decoded snapshot field `SellPrice`, если это поле есть.
    pub sell_price: f64,
    /// Checked-state (для UI start/stop).
    pub checked: bool,
    /// Last server-acknowledged checked-state (`TStrategy.PrevChecked`).
    pub prev_checked: bool,
    /// Folder path в дереве стратегий (из последнего TStratDelete / Snapshot).
    pub folder_path: String,
}

impl StrategyInfo {
    pub(super) fn new(strategy_id: u64) -> Self {
        Self {
            strategy_id,
            strategy_ver: 0,
            last_date: 0,
            sell_price: 0.0,
            checked: false,
            prev_checked: false,
            folder_path: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StratEvent {
    /// Полный snapshot (`Full=true`) успешно применён dispatcher'ом.
    SnapshotFull {
        server_epoch: u64,
        raw_data: Vec<u8>,
    },
    /// Частичный snapshot (`Full=false`) успешно применён dispatcher'ом.
    SnapshotPartial {
        server_epoch: u64,
        raw_data: Vec<u8>,
    },
    /// Результат `TStratDelete`: Delphi сначала пробует удалить StrategyID,
    /// затем FolderPath. Событие приходит только если хотя бы одна операция
    /// реально изменила state.
    Deleted {
        strategy_id: u64,
        folder_path: String,
        strategy_deleted: bool,
        folder_deleted: bool,
    },
    /// Checked-флаги синхронизированы (полная замена или delta).
    CheckedSynced { changed: usize, is_delta: bool },
    /// Эхо checked-state от сервера (после нашего sync).
    CheckedEcho { count: usize },
    /// **Сервер просит у нас snapshot стратегий**.
    /// Это `TStratSnapshotRequest` от сервера. Delphi отвечает fresh rebuild'ом
    /// из живого `Strats`; Rust dispatcher делает то же из `StratsState`.
    /// Если приложение ещё не дало стратегий и серверный snapshot ещё не пришёл,
    /// ответом будет корректный пустой `TStratSnapshot`.
    SnapshotRequested { uid: u64 },
    /// Получена и распарсена schema стратегий (`TStratSchema`, CmdId=8).
    SchemaApplied {
        raw_len: usize,
        format_version: u8,
        kind_count: usize,
        field_count: usize,
    },
    /// Сервер прислал `TStratSchema`, но raw-deflate/body не распарсились.
    SchemaParseFailed { raw_len: usize },
    /// Диагностический вариант для raw parser/users. Client receive path does
    /// not emit it because Delphi client ignores incoming `TStratSchemaRequest`.
    SchemaRequested { uid: u64 },
    /// Low-level diagnostic for commands that the client state does not apply.
    /// The active dispatcher does not emit this for Delphi-inapplicable
    /// incoming command classes such as unknown/skipped, schema request, or
    /// sell-price update.
    Ignored,
}

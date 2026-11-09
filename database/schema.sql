-- database/schema.sql (SQLite, Raspberry Pi)
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS medicoes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    data_hora   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, -- UTC, instante da leitura
    gateway_id  TEXT NOT NULL,
    sensor_id   INTEGER NOT NULL,
    temp_ds1    REAL,
    temp_ds2    REAL,
    temp_ds3    REAL,
    temp_ds4    REAL,
    temp_ds5    REAL,
    temp_ds6    REAL,
    rssi        REAL,
    mac         TEXT,     -- MAC do gateway (vem do tópico rssf/<mac>/...)
    lote_id     INTEGER,  -- id do lote gerado no gateway (uint16)
    enviado_em  TEXT      -- UTC do envio à nuvem; NULL = Pendente; 'descartado' = recusado(400)
);

CREATE INDEX IF NOT EXISTS idx_medicoes_latest_point
    ON medicoes (sensor_id, gateway_id, data_hora, id);
CREATE INDEX IF NOT EXISTS idx_medicoes_dedup
    ON medicoes (mac, lote_id, data_hora);
CREATE INDEX IF NOT EXISTS idx_medicoes_pendentes
    ON medicoes (mac, lote_id) WHERE enviado_em IS NULL;
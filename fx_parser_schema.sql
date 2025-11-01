-- FX PARSER DATABASE SCHEMA (адаптовано під твою структуру)
CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL,
    message_id INTEGER,
    version TEXT,
    published TIMESTAMP,
    edited TIMESTAMP,
    currency_a TEXT,
    currency_b TEXT,
    buy REAL,
    sell REAL,
    comment TEXT,
    UNIQUE(channel_id, message_id, version, currency_a, currency_b, comment),
    FOREIGN KEY(channel_id) REFERENCES channels(id)
);

CREATE TABLE IF NOT EXISTS parse_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT,
    found_count INTEGER,
    new_count INTEGER,
    skipped_count INTEGER,
    FOREIGN KEY(channel_id) REFERENCES channels(id)
);

CREATE VIEW IF NOT EXISTS latest_rates AS
SELECT c.name AS channel,
       r.currency_a, r.currency_b, r.buy, r.sell, r.comment, r.edited
FROM rates r
JOIN channels c ON c.id = r.channel_id
WHERE r.id IN (
    SELECT MAX(id)
    FROM rates
    GROUP BY channel_id, currency_a, currency_b, comment
);

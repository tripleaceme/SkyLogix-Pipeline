CREATE TABLE IF NOT EXISTS insert_history (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    mongo_id TEXT,
    status TEXT NOT NULL,
    error_message TEXT
);

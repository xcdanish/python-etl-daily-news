CREATE TABLE IF NOT EXISTS api_ingest (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    by VARCHAR(100),
    score INTEGER DEFAULT 0,
    time BIGINT,
    type VARCHAR(50),
    url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed records for initial testing
INSERT INTO api_ingest (id, title, by, score, time, type, url)
VALUES 
    (1, 'Show HN: My New Dagster Docker Project', 'admin', 150, 1600000000, 'story', 'https://example.com'),
    (2, 'Postgres Connected to Docker & DBeaver', 'danish', 250, 1600003600, 'story', 'https://dbeaver.io')
ON CONFLICT (id) DO NOTHING;

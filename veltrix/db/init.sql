-- db/init.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE teams (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name       TEXT NOT NULL UNIQUE,
    email      TEXT NOT NULL UNIQUE,
    api_key    TEXT NOT NULL UNIQUE DEFAULT encode(gen_random_bytes(32), 'hex'),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- PENDING | BUILDING | READY | FAILED_LOGIC | FAILED_RESOURCE | FAILED_STARTUP | FAILED_SYSTEM
CREATE TABLE submissions (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    team_id       UUID REFERENCES teams(id) ON DELETE CASCADE,
    language      TEXT NOT NULL,        
    status        TEXT NOT NULL DEFAULT 'PENDING' 
                  CHECK (status IN ('PENDING', 'BUILDING', 'READY', 'SUCCESS', 'FAILED_LOGIC', 'FAILED_RESOURCE', 'FAILED_STARTUP', 'FAILED_SYSTEM')),
    storage_key   TEXT,                 
    container_id  TEXT,                 
    endpoint_url  TEXT,                 
    error_message TEXT,
    exit_code     INT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE benchmark_jobs (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    submission_id UUID REFERENCES submissions(id) ON DELETE CASCADE,
    status        TEXT NOT NULL DEFAULT 'QUEUED',  -- QUEUED|RUNNING|COMPLETED|FAILED
    num_bots      INT DEFAULT 100,
    duration_secs INT DEFAULT 60,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Seed a test team so you can test immediately
INSERT INTO teams (name, email, api_key)
VALUES ('Test Team', 'test@iicpc.dev', 'test-api-key-1234')
ON CONFLICT DO NOTHING;

-- Index to optimize the Sandbox Manager's polling query
CREATE INDEX idx_submissions_status ON submissions(status);
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) NOT NULL UNIQUE,
  display_name VARCHAR(255),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

CREATE TABLE IF NOT EXISTS api_keys (
  id SERIAL PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  api_key_hash BYTEA NOT NULL,
  api_key_partial VARCHAR(6) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  deleted_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(api_key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_created_at ON api_keys(created_at);
CREATE INDEX IF NOT EXISTS idx_api_keys_updated_at ON api_keys(updated_at);
CREATE INDEX IF NOT EXISTS idx_api_keys_deleted_at ON api_keys(deleted_at);

CREATE TABLE IF NOT EXISTS auth_providers (
  id SERIAL PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider VARCHAR(50) NOT NULL,
  provider_user_id VARCHAR(255) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE (provider, provider_user_id)
);
CREATE INDEX IF NOT EXISTS idx_auth_providers_user_id ON auth_providers(user_id);

INSERT INTO users (id, email, display_name)
VALUES ('123e4567-e89b-12d3-a456-426614174000', 'user@example.com', 'Example User');

-- BLAKE3 keyed hash for API key: 123e4567-e89b-12d3-a456-426614174001 and secret from config
INSERT INTO api_keys (user_id, api_key_hash, api_key_partial)
VALUES (
  '123e4567-e89b-12d3-a456-426614174000',
  decode('4021bfe31dd2dc7ac3b6991ecaed73eaa82b56eec2f53ec861ac43214614d5b3','hex'),
  '123e45'
);

-- Companies table
CREATE TABLE IF NOT EXISTS companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL UNIQUE,
  email VARCHAR(255),
  rate_limit_hourly INT NOT NULL DEFAULT 60,
  rate_limit_daily INT NOT NULL DEFAULT 600,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  created_by UUID REFERENCES users(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_companies_updated_at ON companies(updated_at);
CREATE INDEX IF NOT EXISTS idx_companies_created_at ON companies(created_at);

-- Company API keys table
CREATE TABLE IF NOT EXISTS company_api_keys (
  id SERIAL PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  api_key_hash BYTEA NOT NULL,
  api_key_partial VARCHAR(6) NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  deleted_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_company_id ON company_api_keys(company_id);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_hash ON company_api_keys(api_key_hash);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_created_at ON company_api_keys(created_at);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_updated_at ON company_api_keys(updated_at);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_deleted_at ON company_api_keys(deleted_at);

-- Optional composite indexes that can help delta queries and joins
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id_updated ON api_keys(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id_created ON api_keys(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_company_id_updated ON company_api_keys(company_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_company_api_keys_company_id_created ON company_api_keys(company_id, created_at DESC);

-- Raw activity events for client API traffic (one row per request).
-- Tracks: user/company, action, tool, task kind, gateway, timestamps.
-- Optimized for: company_id/user_id + week/month/year buckets (optionally + action),
--                tool + week/month/year buckets, and direct task_id lookup.
-- Daily/weekly/monthly/yearly grouping is fast via generated date buckets.
CREATE TABLE IF NOT EXISTS activity_events (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID REFERENCES users(id) ON DELETE SET NULL,
  user_email VARCHAR(255),
  company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
  company_name VARCHAR(255),
  action VARCHAR(64) NOT NULL CHECK (char_length(action) > 0),
  tool VARCHAR(64) NOT NULL DEFAULT 'api',
  task_kind VARCHAR(16) NOT NULL,
  model VARCHAR(128),
  gateway_name VARCHAR(255) NOT NULL,
  task_id UUID,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
  request_date DATE GENERATED ALWAYS AS (created_at::date) STORED,
  request_week DATE GENERATED ALWAYS AS (date_trunc('week', created_at)::date) STORED,
  request_month DATE GENERATED ALWAYS AS (date_trunc('month', created_at)::date) STORED,
  request_year DATE GENERATED ALWAYS AS (date_trunc('year', created_at)::date) STORED
);
CREATE INDEX IF NOT EXISTS idx_activity_events_task_id ON activity_events(task_id);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_month ON activity_events(company_id, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_month ON activity_events(user_id, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_week ON activity_events(company_id, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_year ON activity_events(company_id, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_week ON activity_events(user_id, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_year ON activity_events(user_id, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_tool_week ON activity_events(tool, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_tool_month ON activity_events(tool, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_tool_year ON activity_events(tool, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_model_week ON activity_events(model, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_model_month ON activity_events(model, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_model_year ON activity_events(model, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_model_day ON activity_events(model, request_date);
-- Composite indexes for tool filtering within company/user buckets
CREATE INDEX IF NOT EXISTS idx_activity_events_company_tool_week ON activity_events(company_id, tool, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_tool_month ON activity_events(company_id, tool, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_tool_year ON activity_events(company_id, tool, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_tool_week ON activity_events(user_id, tool, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_tool_month ON activity_events(user_id, tool, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_tool_year ON activity_events(user_id, tool, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_action_week ON activity_events(company_id, action, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_action_month ON activity_events(company_id, action, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_action_year ON activity_events(company_id, action, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_company_action_day ON activity_events(company_id, action, request_date);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_action_week ON activity_events(user_id, action, request_week);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_action_month ON activity_events(user_id, action, request_month);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_action_year ON activity_events(user_id, action, request_year);
CREATE INDEX IF NOT EXISTS idx_activity_events_user_action_day ON activity_events(user_id, action, request_date);

-- Raw worker events (per assignment/result). Use this for worker stats and averages later.
-- Optimized for: worker_id + week/month/year buckets (optionally + action),
--                and direct task_id lookup.
-- Daily/weekly/monthly/yearly grouping is fast via generated date buckets.
CREATE TABLE IF NOT EXISTS worker_events (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID,
  worker_id VARCHAR(128),
  action VARCHAR(64) NOT NULL CHECK (char_length(action) > 0), -- task_assigned/result_success/result_failure/timeout
  task_kind VARCHAR(16) NOT NULL,
  reason TEXT,
  gateway_name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC'),
  bucket DATE GENERATED ALWAYS AS (created_at::date) STORED,
  bucket_week DATE GENERATED ALWAYS AS (date_trunc('week', created_at)::date) STORED,
  bucket_month DATE GENERATED ALWAYS AS (date_trunc('month', created_at)::date) STORED,
  bucket_year DATE GENERATED ALWAYS AS (date_trunc('year', created_at)::date) STORED
);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_time ON worker_events(worker_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_events_task ON worker_events(task_id);
CREATE INDEX IF NOT EXISTS idx_worker_events_gateway_time ON worker_events(gateway_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_month ON worker_events(worker_id, bucket_month);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_week ON worker_events(worker_id, bucket_week);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_year ON worker_events(worker_id, bucket_year);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_action_week ON worker_events(worker_id, action, bucket_week);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_action_month ON worker_events(worker_id, action, bucket_month);
CREATE INDEX IF NOT EXISTS idx_worker_events_worker_action_year ON worker_events(worker_id, action, bucket_year);

-- Batched rate-limit violations (aggregated per flush window + gateway).
-- "details" stores a JSON object with per-client counters.
CREATE TABLE IF NOT EXISTS rate_limit_violations (
  id BIGSERIAL PRIMARY KEY,
  gateway_name VARCHAR(255) NOT NULL,
  window_start TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  window_end TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  total_count BIGINT NOT NULL CHECK (total_count >= 0),
  details JSONB NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')
);
CREATE INDEX IF NOT EXISTS idx_rate_limit_violations_gateway_window
  ON rate_limit_violations(gateway_name, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_rate_limit_violations_window
  ON rate_limit_violations(window_start DESC);

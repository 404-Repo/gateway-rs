CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) NOT NULL UNIQUE,
  display_name VARCHAR(255),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

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

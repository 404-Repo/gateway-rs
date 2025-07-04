CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) NOT NULL UNIQUE,
  display_name VARCHAR(255),
  created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

CREATE TABLE IF NOT EXISTS api_keys (
  id SERIAL PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  api_key_hash VARCHAR(255) NOT NULL,
  api_key_partial VARCHAR(6) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(api_key_hash);

CREATE TABLE IF NOT EXISTS auth_providers (
  id SERIAL PRIMARY KEY,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider VARCHAR(50) NOT NULL,
  provider_user_id VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (provider, provider_user_id)
);
CREATE INDEX IF NOT EXISTS idx_auth_providers_user_id ON auth_providers(user_id);

INSERT INTO users (id, email, display_name)
VALUES ('123e4567-e89b-12d3-a456-426614174000', 'user@example.com', 'Example User');

-- Insert hashed API keys - hashes computed using Argon2d with memoryCost: 9216, timeCost: 4, parallelism: 1
-- Plain text: 123e4567-e89b-12d3-a456-426614174001
INSERT INTO api_keys (user_id, api_key_hash, api_key_partial)
VALUES ('123e4567-e89b-12d3-a456-426614174000', '$argon2d$v=19$m=9216,t=4,p=1$a3Z6ZDdLbDloN2tsLzZtVQ$Z0EC3GytVX2TNA/Vp9/VTII3h3s+EXQVoaKM7+KsOyc', '123e45');

-- Plain text: 123e4567-e89b-12d3-a456-426614174002
INSERT INTO api_keys (user_id, api_key_hash, api_key_partial)
VALUES ('123e4567-e89b-12d3-a456-426614174000', '$argon2d$v=19$m=9216,t=4,p=1$RWJ1akVHY1pCTGtLQ3VWag$8FMv75EwUfq944H8ZgIOOgYccDKduX57443QQGgJc0E', '123e45');

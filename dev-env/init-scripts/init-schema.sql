CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE OR REPLACE FUNCTION pg_raise(p_message TEXT)
RETURNS TEXT
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
  RAISE EXCEPTION '%', p_message;
END;
$$;

CREATE OR REPLACE FUNCTION gen_advisory_xact_lock(p_namespace TEXT, p_key TEXT)
RETURNS VOID
LANGUAGE sql
SET search_path = public
AS $$
  SELECT pg_advisory_xact_lock(
    hashtext(COALESCE(p_namespace, '')),
    hashtext(COALESCE(p_key, ''))
  );
$$;

-- Keep timestamp bookkeeping inside PostgreSQL so delta-based cache refreshes
-- remain correct even if a writer forgets to touch updated_at explicitly.
CREATE OR REPLACE FUNCTION gen_manage_row_timestamps()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = public
AS $$
DECLARE
  v_now BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.created_at IS NULL THEN
      NEW.created_at := v_now;
    END IF;

    IF NEW.updated_at IS NULL OR NEW.updated_at < NEW.created_at THEN
      NEW.updated_at := NEW.created_at;
    END IF;

    RETURN NEW;
  END IF;

  NEW.created_at := OLD.created_at;
  NEW.updated_at := GREATEST(
    COALESCE(OLD.updated_at, 0),
    COALESCE(NEW.updated_at, 0),
    v_now
  );
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS app_settings (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  guest_generation_limit INTEGER NOT NULL CHECK (guest_generation_limit >= 0),
  guest_window_ms BIGINT NOT NULL CHECK (guest_window_ms > 0),
  registered_generation_limit INTEGER NOT NULL CHECK (registered_generation_limit >= 0),
  registered_window_ms BIGINT NOT NULL CHECK (registered_window_ms > 0),
  gateway_generic_key UUID NOT NULL,
  gateway_generic_global_daily_limit INTEGER NOT NULL CHECK (gateway_generic_global_daily_limit >= 0),
  gateway_generic_per_ip_daily_limit INTEGER NOT NULL CHECK (gateway_generic_per_ip_daily_limit >= 0),
  gateway_generic_window_ms BIGINT NOT NULL CHECK (gateway_generic_window_ms > 0),
  personal_api_key_create_limit_per_day INTEGER NOT NULL DEFAULT 10 CHECK (personal_api_key_create_limit_per_day >= 0),
  add_task_unauthorized_per_ip_daily_rate_limit INTEGER NOT NULL CHECK (add_task_unauthorized_per_ip_daily_rate_limit >= 0),
  rate_limit_whitelist TEXT[] NOT NULL,
  max_task_queue_len INTEGER NOT NULL CHECK (max_task_queue_len >= 0),
  request_file_size_limit BIGINT NOT NULL CHECK (request_file_size_limit >= 0),
  stripe_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

DO $$
DECLARE
  v_now BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
  INSERT INTO app_settings (
    id,
    guest_generation_limit,
    guest_window_ms,
    registered_generation_limit,
    registered_window_ms,
    gateway_generic_key,
    gateway_generic_global_daily_limit,
    gateway_generic_per_ip_daily_limit,
    gateway_generic_window_ms,
    personal_api_key_create_limit_per_day,
    add_task_unauthorized_per_ip_daily_rate_limit,
    rate_limit_whitelist,
    max_task_queue_len,
    request_file_size_limit,
    stripe_enabled,
    created_at,
    updated_at
  )
  VALUES (
    1,
    1,
    86400000,
    0,
    86400000,
    '6eca4068-3be6-4d30-b828-f63cda3bc35b'::UUID,
    250,
    10,
    86400000,
    10,
    1200,
    ARRAY[]::TEXT[],
    500,
    157286400,
    TRUE,
    v_now,
    v_now
  )
  ON CONFLICT (id) DO NOTHING;
END;
$$;

DROP TRIGGER IF EXISTS app_settings_timestamps ON app_settings;

CREATE TRIGGER app_settings_timestamps
BEFORE INSERT OR UPDATE
ON app_settings
FOR EACH ROW
EXECUTE FUNCTION gen_manage_row_timestamps();

-- Auth

CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT,
  display_name TEXT,
  -- Rate-limit convention for task_limit_* columns below: 0 means unlimited.
  task_limit_concurrent INTEGER NOT NULL DEFAULT 1 CHECK (task_limit_concurrent >= 0),
  task_limit_daily INTEGER NOT NULL DEFAULT 50 CHECK (task_limit_daily >= 0),
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  last_login_at BIGINT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique
  ON users ((LOWER(email)))
  WHERE email IS NOT NULL;

CREATE INDEX IF NOT EXISTS users_email_trgm_idx
  ON users USING GIN (LOWER(email) gin_trgm_ops)
  WHERE email IS NOT NULL;

CREATE INDEX IF NOT EXISTS users_display_name_trgm_idx
  ON users USING GIN (LOWER(display_name) gin_trgm_ops)
  WHERE display_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS users_last_login_idx
  ON users (last_login_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS users_created_at_idx
  ON users (created_at);

CREATE INDEX IF NOT EXISTS users_updated_at_idx
  ON users (updated_at);

DROP TRIGGER IF EXISTS users_timestamps ON users;

CREATE TRIGGER users_timestamps
BEFORE INSERT OR UPDATE
ON users
FOR EACH ROW
EXECUTE FUNCTION gen_manage_row_timestamps();

CREATE TABLE IF NOT EXISTS auth_identities (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id),
  provider TEXT NOT NULL,
  provider_user_id TEXT NOT NULL,
  email TEXT,
  display_name TEXT,
  created_at BIGINT NOT NULL,
  last_login_at BIGINT NOT NULL,
  UNIQUE (provider, provider_user_id)
);

CREATE INDEX IF NOT EXISTS auth_identities_user_id_idx
  ON auth_identities (user_id);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  active_identity_id BIGINT REFERENCES auth_identities(id) ON DELETE SET NULL,
  data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  last_seen_at BIGINT NOT NULL,
  expires_at BIGINT NOT NULL,
  revoked_at BIGINT,
  ip TEXT,
  user_agent TEXT
);

CREATE INDEX IF NOT EXISTS sessions_user_id_idx
  ON sessions (user_id);

CREATE INDEX IF NOT EXISTS sessions_expires_at_idx
  ON sessions (expires_at);

CREATE INDEX IF NOT EXISTS sessions_revoked_at_idx
  ON sessions (revoked_at)
  WHERE revoked_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS sessions_active_expires_at_idx
  ON sessions (expires_at, id)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS sessions_revoked_cleanup_idx
  ON sessions (revoked_at, id)
  WHERE revoked_at IS NOT NULL;

UPDATE sessions
SET data_json = data_json - 'apiKey'
WHERE data_json ? 'apiKey';

CREATE TABLE IF NOT EXISTS admins (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL UNIQUE REFERENCES users(id),
  created_by_admin_id BIGINT REFERENCES admins(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS admins_created_at_idx
  ON admins (created_at);

-- Ownership and billing

CREATE TABLE IF NOT EXISTS companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  owner_email TEXT,
  vat VARCHAR(30),
  -- Rate-limit convention for task_limit_* columns below: 0 means unlimited.
  task_limit_concurrent INTEGER NOT NULL DEFAULT 1 CHECK (task_limit_concurrent >= 0),
  task_limit_daily INTEGER NOT NULL CHECK (task_limit_daily >= 0),
  ownership_state TEXT NOT NULL DEFAULT 'unassigned' CHECK (ownership_state IN ('unassigned', 'owned')),
  created_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS companies_created_at_idx
  ON companies (created_at);

CREATE INDEX IF NOT EXISTS companies_updated_at_idx
  ON companies (updated_at);

DROP TRIGGER IF EXISTS companies_timestamps ON companies;

CREATE TRIGGER companies_timestamps
BEFORE INSERT OR UPDATE
ON companies
FOR EACH ROW
EXECUTE FUNCTION gen_manage_row_timestamps();

CREATE INDEX IF NOT EXISTS companies_name_lower_created_idx
  ON companies ((LOWER(name)), created_at DESC, id);

CREATE TABLE IF NOT EXISTS company_memberships (
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('owner', 'viewer')),
  added_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (company_id, user_id)
);

CREATE INDEX IF NOT EXISTS company_memberships_user_id_idx
  ON company_memberships (user_id, company_id);

CREATE INDEX IF NOT EXISTS company_memberships_company_role_idx
  ON company_memberships (company_id, role, user_id);

CREATE TABLE IF NOT EXISTS company_invitations (
  id BIGSERIAL PRIMARY KEY,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('owner', 'viewer')),
  invited_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  expires_at BIGINT,
  accepted_at BIGINT,
  accepted_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  revoked_at BIGINT
);

CREATE INDEX IF NOT EXISTS company_invitations_email_lookup_idx
  ON company_invitations ((LOWER(email)), company_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS company_invitations_active_unique
  ON company_invitations (company_id, (LOWER(email)))
  WHERE accepted_at IS NULL AND revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS accounts (
  id BIGSERIAL PRIMARY KEY,
  owner_kind TEXT NOT NULL CHECK (owner_kind IN ('user', 'company')),
  user_id BIGINT UNIQUE REFERENCES users(id),
  company_id UUID UNIQUE REFERENCES companies(id),
  balance_cents BIGINT NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
  currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
  stripe_customer_id TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  CONSTRAINT accounts_id_user_id_unique UNIQUE (id, user_id),
  CONSTRAINT accounts_id_company_id_unique UNIQUE (id, company_id),
  CONSTRAINT accounts_owner_check
    CHECK (
      (owner_kind = 'user' AND user_id IS NOT NULL AND company_id IS NULL)
      OR
      (owner_kind = 'company' AND company_id IS NOT NULL AND user_id IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS accounts_stripe_customer_id_unique
  ON accounts (stripe_customer_id)
  WHERE stripe_customer_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS accounts_owner_kind_idx
  ON accounts (owner_kind, id);

CREATE TABLE IF NOT EXISTS api_keys (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id),
  key_scope TEXT NOT NULL CHECK (key_scope IN ('personal', 'company')),
  user_id BIGINT REFERENCES users(id),
  company_id UUID REFERENCES companies(id),
  name TEXT NOT NULL,
  api_key_hash BYTEA NOT NULL,
  api_key_encrypted BYTEA,
  api_key_partial TEXT NOT NULL CHECK (char_length(api_key_partial) > 0),
  is_primary BOOLEAN NOT NULL DEFAULT FALSE,
  created_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  last_used_at BIGINT,
  revoked_at BIGINT,
  CONSTRAINT api_keys_id_account_id_unique UNIQUE (id, account_id),
  CONSTRAINT api_keys_id_user_id_unique UNIQUE (id, user_id),
  CONSTRAINT api_keys_id_company_id_unique UNIQUE (id, company_id),
  CONSTRAINT api_keys_owner_check
    CHECK (
      (key_scope = 'personal' AND user_id IS NOT NULL AND company_id IS NULL)
      OR
      (key_scope = 'company' AND company_id IS NOT NULL AND user_id IS NULL)
    ),
  CONSTRAINT api_keys_primary_scope_check
    CHECK (NOT is_primary OR key_scope = 'personal'),
  CONSTRAINT api_keys_primary_encrypted_check
    CHECK (NOT is_primary OR api_key_encrypted IS NOT NULL),
  CONSTRAINT api_keys_account_user_match_fkey
    FOREIGN KEY (account_id, user_id) REFERENCES accounts(id, user_id),
  CONSTRAINT api_keys_account_company_match_fkey
    FOREIGN KEY (account_id, company_id) REFERENCES accounts(id, company_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS api_keys_hash_unique
  ON api_keys (api_key_hash);

CREATE INDEX IF NOT EXISTS api_keys_account_id_idx
  ON api_keys (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS api_keys_user_id_idx
  ON api_keys (user_id, created_at DESC)
  WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS api_keys_company_id_idx
  ON api_keys (company_id, created_at DESC)
  WHERE company_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS api_keys_active_personal_user_idx
  ON api_keys (user_id)
  WHERE key_scope = 'personal' AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS api_keys_active_company_idx
  ON api_keys (company_id)
  WHERE key_scope = 'company' AND revoked_at IS NULL AND company_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS api_keys_personal_primary_unique
  ON api_keys (user_id)
  WHERE user_id IS NOT NULL AND is_primary = TRUE AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS api_keys_created_at_idx
  ON api_keys (created_at);

CREATE INDEX IF NOT EXISTS api_keys_updated_at_idx
  ON api_keys (updated_at);

DROP TRIGGER IF EXISTS api_keys_timestamps ON api_keys;

CREATE TRIGGER api_keys_timestamps
BEFORE INSERT OR UPDATE
ON api_keys
FOR EACH ROW
EXECUTE FUNCTION gen_manage_row_timestamps();

CREATE OR REPLACE FUNCTION gen_guard_primary_personal_api_key()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.key_scope = 'personal'
       AND OLD.is_primary = TRUE
       AND OLD.revoked_at IS NULL THEN
      RAISE EXCEPTION
        'Primary personal API key % cannot be deleted.',
        OLD.id;
    END IF;
    RETURN OLD;
  END IF;

  IF OLD.key_scope = 'personal'
     AND OLD.is_primary = TRUE
     AND OLD.revoked_at IS NULL
     AND (
       NEW.key_scope <> 'personal'
       OR NEW.user_id IS DISTINCT FROM OLD.user_id
       OR NEW.is_primary IS DISTINCT FROM TRUE
       OR NEW.revoked_at IS NOT NULL
     ) THEN
    RAISE EXCEPTION
      'Primary personal API key % cannot be revoked, moved, or demoted.',
      OLD.id;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS api_keys_primary_personal_guard ON api_keys;

CREATE TRIGGER api_keys_primary_personal_guard
BEFORE UPDATE OF key_scope, user_id, is_primary, revoked_at OR DELETE
ON api_keys
FOR EACH ROW
EXECUTE FUNCTION gen_guard_primary_personal_api_key();

CREATE OR REPLACE FUNCTION gen_guard_last_active_personal_api_key()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_active_non_primary_count BIGINT;
  v_recent_key_count BIGINT;
  v_limit INTEGER;
  v_now BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
  IF OLD.key_scope <> 'personal'
     OR OLD.user_id IS NULL
     OR OLD.is_primary = TRUE
     OR OLD.revoked_at IS NOT NULL THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NEW;
  END IF;

  IF TG_OP <> 'DELETE' THEN
    IF NEW.revoked_at IS NULL
       OR NEW.revoked_at IS NOT DISTINCT FROM OLD.revoked_at THEN
      RETURN NEW;
    END IF;
  END IF;

  PERFORM gen_advisory_xact_lock(
    'api_keys_last_active_personal_key',
    OLD.user_id::TEXT
  );

  SELECT COUNT(*)
  INTO v_active_non_primary_count
  FROM api_keys
  WHERE user_id = OLD.user_id
    AND key_scope = 'personal'
    AND is_primary = FALSE
    AND revoked_at IS NULL;

  IF v_active_non_primary_count <= 1 THEN
    SELECT COUNT(*)
    INTO v_recent_key_count
    FROM api_keys
    WHERE user_id = OLD.user_id
      AND key_scope = 'personal'
      AND is_primary = FALSE
      AND created_at > (v_now - 86400000);

    SELECT personal_api_key_create_limit_per_day
    INTO v_limit
    FROM app_settings
    WHERE id = 1;

    v_limit := COALESCE(v_limit, 10);

    IF v_recent_key_count >= v_limit THEN
      RAISE EXCEPTION
        USING
          ERRCODE = 'ZK104',
          MESSAGE = FORMAT(
            'User %s must keep at least one non-primary active personal API key.',
            OLD.user_id
          );
    END IF;
  END IF;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS api_keys_last_active_personal_guard ON api_keys;

CREATE TRIGGER api_keys_last_active_personal_guard
BEFORE UPDATE OF revoked_at OR DELETE
ON api_keys
FOR EACH ROW
EXECUTE FUNCTION gen_guard_last_active_personal_api_key();

CREATE OR REPLACE FUNCTION gen_enforce_active_personal_api_key_limit()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_active_key_count BIGINT;
BEGIN
  IF NEW.key_scope <> 'personal'
     OR NEW.user_id IS NULL
     OR NEW.revoked_at IS NOT NULL THEN
    RETURN NEW;
  END IF;

  PERFORM gen_advisory_xact_lock(
    'api_keys_active_personal_limit',
    NEW.user_id::TEXT
  );

  SELECT COUNT(*)
  INTO v_active_key_count
  FROM api_keys
  WHERE user_id = NEW.user_id
    AND key_scope = 'personal'
    AND revoked_at IS NULL
    AND id IS DISTINCT FROM NEW.id;

  IF v_active_key_count >= 5 THEN
    RAISE EXCEPTION
      USING
        ERRCODE = 'ZK101',
        MESSAGE = FORMAT(
          'User %s cannot have more than 5 active personal API keys.',
          NEW.user_id
        );
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS api_keys_active_personal_limit_guard ON api_keys;

CREATE TRIGGER api_keys_active_personal_limit_guard
BEFORE INSERT OR UPDATE OF key_scope, user_id, revoked_at ON api_keys
FOR EACH ROW
EXECUTE FUNCTION gen_enforce_active_personal_api_key_limit();

CREATE OR REPLACE FUNCTION gen_enforce_personal_api_key_creation_rate_limit()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_recent_key_count BIGINT;
  v_limit INTEGER;
  v_now BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
  IF NEW.key_scope <> 'personal'
     OR NEW.user_id IS NULL
     OR NEW.is_primary = TRUE THEN
    RETURN NEW;
  END IF;

  IF NEW.created_at IS NULL THEN
    NEW.created_at := v_now;
  END IF;

  PERFORM gen_advisory_xact_lock(
    'api_keys_personal_creation_rate_limit',
    NEW.user_id::TEXT
  );

  SELECT COUNT(*)
  INTO v_recent_key_count
  FROM api_keys
  WHERE user_id = NEW.user_id
    AND key_scope = 'personal'
    AND is_primary = FALSE
    AND created_at > (NEW.created_at - 86400000);

  SELECT personal_api_key_create_limit_per_day
  INTO v_limit
  FROM app_settings
  WHERE id = 1;

  v_limit := COALESCE(v_limit, 10);

  IF v_recent_key_count >= v_limit THEN
    RAISE EXCEPTION
      USING
        ERRCODE = 'ZK103',
        MESSAGE = FORMAT(
          'User %s cannot create more than %s personal API keys per rolling 24-hour window.',
          NEW.user_id
          , v_limit
        );
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS api_keys_personal_creation_rate_limit_guard ON api_keys;

CREATE TRIGGER api_keys_personal_creation_rate_limit_guard
BEFORE INSERT
ON api_keys
FOR EACH ROW
EXECUTE FUNCTION gen_enforce_personal_api_key_creation_rate_limit();

CREATE OR REPLACE FUNCTION gen_enforce_active_company_api_key_limit()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_active_key_count BIGINT;
BEGIN
  IF NEW.key_scope <> 'company'
     OR NEW.company_id IS NULL
     OR NEW.revoked_at IS NOT NULL THEN
    RETURN NEW;
  END IF;

  PERFORM gen_advisory_xact_lock(
    'api_keys_active_company_limit',
    NEW.company_id::TEXT
  );

  SELECT COUNT(*)
  INTO v_active_key_count
  FROM api_keys
  WHERE company_id = NEW.company_id
    AND key_scope = 'company'
    AND revoked_at IS NULL
    AND id IS DISTINCT FROM NEW.id;

  IF v_active_key_count >= 5 THEN
    RAISE EXCEPTION
      USING
        ERRCODE = 'ZK102',
        MESSAGE = FORMAT(
          'Company %s cannot have more than 5 active company API keys.',
          NEW.company_id
        );
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS api_keys_active_company_limit_guard ON api_keys;

CREATE TRIGGER api_keys_active_company_limit_guard
BEFORE INSERT OR UPDATE OF key_scope, company_id, revoked_at ON api_keys
FOR EACH ROW
EXECUTE FUNCTION gen_enforce_active_company_api_key_limit();

CREATE TABLE IF NOT EXISTS model_prices (
  id BIGSERIAL PRIMARY KEY,
  task_kind TEXT NOT NULL,
  model TEXT NOT NULL,
  amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
  currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
  created_by_admin_id BIGINT REFERENCES admins(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL,
  archived_at BIGINT,
  CONSTRAINT model_prices_task_kind_check CHECK (char_length(task_kind) > 0),
  CONSTRAINT model_prices_model_check CHECK (char_length(model) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS model_prices_active_unique
  ON model_prices (task_kind, model)
  WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS model_prices_created_at_idx
  ON model_prices (created_at DESC);

CREATE INDEX IF NOT EXISTS model_prices_sort_idx
  ON model_prices ((archived_at IS NULL) DESC, created_at DESC, id DESC);

DO $$
DECLARE
  v_now BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
BEGIN
  INSERT INTO model_prices (
    task_kind,
    model,
    amount_cents,
    currency,
    created_by_admin_id,
    created_at,
    archived_at
  )
  VALUES
    ('text_to_3d', '404-3dgs', 15, 'USD', NULL, v_now, NULL),
    ('image_to_3d', '404-3dgs', 15, 'USD', NULL, v_now, NULL),
    ('image_to_3d', '404-mesh', 25, 'USD', NULL, v_now, NULL),
    ('image_to_3d', '404-mesh-v2', 25, 'USD', NULL, v_now, NULL)
  ON CONFLICT DO NOTHING;
END;
$$;

-- Generation runtime

CREATE TABLE IF NOT EXISTS generation_reservations (
  id BIGSERIAL PRIMARY KEY,
  reservation_key TEXT NOT NULL UNIQUE,
  billing_mode TEXT NOT NULL CHECK (billing_mode IN ('paid', 'user_free', 'guest_free')),
  status TEXT NOT NULL CHECK (status IN ('reserved', 'committed', 'released', 'expired')),
  account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
  api_key_id BIGINT REFERENCES api_keys(id) ON DELETE SET NULL,
  guest_key_hash BYTEA,
  price_id BIGINT REFERENCES model_prices(id) ON DELETE SET NULL,
  task_kind TEXT NOT NULL,
  model TEXT NOT NULL,
  amount_cents BIGINT NOT NULL DEFAULT 0 CHECK (amount_cents >= 0),
  currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
  release_reason TEXT,
  created_at BIGINT NOT NULL,
  expires_at BIGINT NOT NULL,
  finalized_at BIGINT,
  CONSTRAINT generation_reservations_paid_shape_check
    CHECK (
      (
        billing_mode = 'paid'
        AND account_id IS NOT NULL
        AND (
          (user_id IS NOT NULL AND company_id IS NULL)
          OR
          (user_id IS NULL AND company_id IS NOT NULL)
        )
        AND price_id IS NOT NULL
        AND guest_key_hash IS NULL
        AND amount_cents > 0
      )
      OR
      (
        billing_mode = 'user_free'
        AND account_id IS NULL
        AND user_id IS NOT NULL
        AND company_id IS NULL
        AND guest_key_hash IS NULL
        AND amount_cents = 0
        AND price_id IS NULL
      )
      OR
      (
        billing_mode = 'guest_free'
        AND account_id IS NULL
        AND user_id IS NULL
        AND company_id IS NULL
        AND api_key_id IS NULL
        AND guest_key_hash IS NOT NULL
        AND octet_length(guest_key_hash) = 16
        AND amount_cents = 0
        AND price_id IS NULL
      )
    ),
  CONSTRAINT generation_reservations_account_user_match_fkey
    FOREIGN KEY (account_id, user_id) REFERENCES accounts(id, user_id),
  CONSTRAINT generation_reservations_account_company_match_fkey
    FOREIGN KEY (account_id, company_id) REFERENCES accounts(id, company_id),
  CONSTRAINT generation_reservations_api_key_account_match_fkey
    FOREIGN KEY (api_key_id, account_id) REFERENCES api_keys(id, account_id),
  CONSTRAINT generation_reservations_api_key_user_match_fkey
    FOREIGN KEY (api_key_id, user_id) REFERENCES api_keys(id, user_id),
  CONSTRAINT generation_reservations_api_key_company_match_fkey
    FOREIGN KEY (api_key_id, company_id) REFERENCES api_keys(id, company_id)
);

CREATE INDEX IF NOT EXISTS generation_reservations_status_expires_idx
  ON generation_reservations (status, expires_at, id);

CREATE INDEX IF NOT EXISTS generation_reservations_account_created_idx
  ON generation_reservations (account_id, created_at DESC)
  WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_reservations_user_created_idx
  ON generation_reservations (user_id, created_at DESC)
  WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_reservations_user_free_active_idx
  ON generation_reservations (user_id, created_at DESC, id)
  WHERE billing_mode = 'user_free'
    AND status IN ('reserved', 'committed');

CREATE INDEX IF NOT EXISTS generation_reservations_guest_free_active_idx
  ON generation_reservations (guest_key_hash, created_at DESC, id)
  WHERE billing_mode = 'guest_free'
    AND status IN ('reserved', 'committed');

CREATE INDEX IF NOT EXISTS generation_reservations_guest_free_global_active_idx
  ON generation_reservations (created_at DESC, id)
  WHERE billing_mode = 'guest_free'
    AND status IN ('reserved', 'committed');

CREATE TABLE IF NOT EXISTS generation_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reservation_id BIGINT NOT NULL UNIQUE REFERENCES generation_reservations(id) ON DELETE RESTRICT,
  account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
  api_key_id BIGINT REFERENCES api_keys(id) ON DELETE SET NULL,
  session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
  client_origin TEXT NOT NULL DEFAULT 'website',
  task_kind TEXT NOT NULL,
  model TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'timed_out', 'cancelled')),
  expected_results INTEGER NOT NULL DEFAULT 1 CHECK (expected_results > 0),
  finished_results_count INTEGER NOT NULL DEFAULT 0 CHECK (finished_results_count >= 0),
  success_count INTEGER NOT NULL DEFAULT 0 CHECK (success_count >= 0),
  deadline_at BIGINT NOT NULL,
  gateway_name TEXT,
  request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_json JSONB,
  error_code TEXT,
  error_message TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  started_at BIGINT,
  completed_at BIGINT,
  CONSTRAINT generation_tasks_account_user_match_fkey
    FOREIGN KEY (account_id, user_id) REFERENCES accounts(id, user_id),
  CONSTRAINT generation_tasks_account_company_match_fkey
    FOREIGN KEY (account_id, company_id) REFERENCES accounts(id, company_id),
  CONSTRAINT generation_tasks_api_key_account_match_fkey
    FOREIGN KEY (api_key_id, account_id) REFERENCES api_keys(id, account_id),
  CONSTRAINT generation_tasks_api_key_user_match_fkey
    FOREIGN KEY (api_key_id, user_id) REFERENCES api_keys(id, user_id),
  CONSTRAINT generation_tasks_api_key_company_match_fkey
    FOREIGN KEY (api_key_id, company_id) REFERENCES api_keys(id, company_id),
  CONSTRAINT generation_tasks_result_counts_check
    CHECK (finished_results_count >= success_count),
  CONSTRAINT generation_tasks_completed_state_check
    CHECK (
      completed_at IS NULL
      OR status IN ('succeeded', 'failed', 'timed_out', 'cancelled')
    )
);

CREATE INDEX IF NOT EXISTS generation_tasks_account_created_idx
  ON generation_tasks (account_id, created_at DESC)
  WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_tasks_user_created_idx
  ON generation_tasks (user_id, created_at DESC)
  WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_tasks_company_created_idx
  ON generation_tasks (company_id, created_at DESC)
  WHERE company_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_tasks_status_created_idx
  ON generation_tasks (status, created_at DESC);

DROP INDEX IF EXISTS generation_tasks_status_deadline_idx;
CREATE INDEX IF NOT EXISTS generation_tasks_active_deadline_idx
  ON generation_tasks (deadline_at, id)
  WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS generation_tasks_session_created_idx
  ON generation_tasks (session_id, created_at DESC)
  WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_tasks_terminal_completed_idx
  ON generation_tasks (completed_at, id)
  WHERE status IN ('succeeded', 'failed', 'timed_out', 'cancelled')
    AND completed_at IS NOT NULL;

CREATE OR REPLACE FUNCTION gen_validate_generation_task_mirror()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = public
AS $$
DECLARE
  v_reservation generation_reservations%ROWTYPE;
BEGIN
  SELECT *
  INTO v_reservation
  FROM generation_reservations
  WHERE id = NEW.reservation_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Generation reservation % not found.', NEW.reservation_id;
  END IF;

  IF NEW.account_id IS DISTINCT FROM v_reservation.account_id
     OR NEW.user_id IS DISTINCT FROM v_reservation.user_id
     OR NEW.company_id IS DISTINCT FROM v_reservation.company_id
     OR NEW.api_key_id IS DISTINCT FROM v_reservation.api_key_id
     OR NEW.task_kind IS DISTINCT FROM v_reservation.task_kind
     OR NEW.model IS DISTINCT FROM v_reservation.model THEN
    RAISE EXCEPTION
      'generation_tasks must mirror the owner and task fields of generation_reservations.';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS generation_tasks_reservation_mirror_guard ON generation_tasks;

CREATE CONSTRAINT TRIGGER generation_tasks_reservation_mirror_guard
AFTER INSERT OR UPDATE OF account_id, user_id, company_id, api_key_id, task_kind, model
ON generation_tasks
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW
EXECUTE FUNCTION gen_validate_generation_task_mirror();

CREATE OR REPLACE FUNCTION gen_validate_generation_reservation_mirror()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = public
AS $$
DECLARE
  v_task generation_tasks%ROWTYPE;
BEGIN
  SELECT *
  INTO v_task
  FROM generation_tasks
  WHERE reservation_id = NEW.id;

  IF NOT FOUND THEN
    RETURN NEW;
  END IF;

  IF NEW.account_id IS DISTINCT FROM v_task.account_id
     OR NEW.user_id IS DISTINCT FROM v_task.user_id
     OR NEW.company_id IS DISTINCT FROM v_task.company_id
     OR NEW.api_key_id IS DISTINCT FROM v_task.api_key_id
     OR NEW.task_kind IS DISTINCT FROM v_task.task_kind
     OR NEW.model IS DISTINCT FROM v_task.model THEN
    RAISE EXCEPTION
      'generation_reservations must mirror the owner and task fields of generation_tasks.';
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS generation_reservations_task_mirror_guard ON generation_reservations;

CREATE CONSTRAINT TRIGGER generation_reservations_task_mirror_guard
AFTER UPDATE OF account_id, user_id, company_id, api_key_id, task_kind, model
ON generation_reservations
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW
EXECUTE FUNCTION gen_validate_generation_reservation_mirror();

CREATE TABLE IF NOT EXISTS generation_task_assignments (
  id BIGSERIAL PRIMARY KEY,
  assignment_token UUID NOT NULL DEFAULT gen_random_uuid(),
  task_id UUID NOT NULL REFERENCES generation_tasks(id) ON DELETE CASCADE,
  worker_hotkey TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('assigned', 'succeeded', 'failed', 'timed_out', 'cancelled')),
  assigned_at BIGINT NOT NULL,
  completed_at BIGINT,
  failure_reason TEXT,
  result_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT generation_task_assignments_status_shape_check
    CHECK (
      (status = 'assigned' AND completed_at IS NULL)
      OR
      (status IN ('succeeded', 'failed', 'timed_out', 'cancelled') AND completed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS generation_task_assignments_assignment_token_unique
  ON generation_task_assignments (assignment_token);

CREATE UNIQUE INDEX IF NOT EXISTS generation_task_assignments_task_worker_active_unique
  ON generation_task_assignments (task_id, worker_hotkey)
  WHERE status = 'assigned';

CREATE UNIQUE INDEX IF NOT EXISTS generation_task_assignments_task_worker_identity_unique
  ON generation_task_assignments (task_id, worker_hotkey, worker_id);

CREATE INDEX IF NOT EXISTS generation_task_assignments_task_worker_completed_lookup_idx
  ON generation_task_assignments (task_id, worker_hotkey, worker_id, completed_at DESC, assigned_at DESC)
  WHERE completed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_task_assignments_task_status_idx
  ON generation_task_assignments (task_id, status, assigned_at DESC);

CREATE INDEX IF NOT EXISTS generation_task_assignments_worker_assigned_idx
  ON generation_task_assignments (worker_hotkey, assigned_at DESC);

CREATE TABLE IF NOT EXISTS balance_ledger (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id),
  reservation_id BIGINT REFERENCES generation_reservations(id) ON DELETE SET NULL,
  price_id BIGINT REFERENCES model_prices(id) ON DELETE SET NULL,
  entry_kind TEXT NOT NULL CHECK (entry_kind IN ('topup', 'admin_adjustment', 'generation_reserve', 'generation_refund')),
  amount_cents BIGINT NOT NULL CHECK (amount_cents <> 0),
  balance_after_cents BIGINT NOT NULL CHECK (balance_after_cents >= 0),
  currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
  stripe_session_id TEXT,
  request_id TEXT,
  description TEXT,
  created_by_admin_id BIGINT REFERENCES admins(id) ON DELETE SET NULL,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS balance_ledger_account_created_cursor_idx
  ON balance_ledger (account_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS balance_ledger_account_entry_kind_created_idx
  ON balance_ledger (account_id, entry_kind, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS balance_ledger_account_negative_created_idx
  ON balance_ledger (account_id, created_at DESC, id DESC)
  WHERE amount_cents < 0;

CREATE INDEX IF NOT EXISTS balance_ledger_account_positive_non_topup_created_idx
  ON balance_ledger (account_id, created_at DESC, id DESC)
  WHERE amount_cents > 0
    AND entry_kind <> 'topup';

CREATE INDEX IF NOT EXISTS balance_ledger_description_trgm_idx
  ON balance_ledger USING GIN (LOWER(description) gin_trgm_ops)
  WHERE description IS NOT NULL;

CREATE INDEX IF NOT EXISTS generation_reservations_model_trgm_idx
  ON generation_reservations USING GIN (LOWER(model) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS balance_ledger_reservation_idx
  ON balance_ledger (reservation_id)
  WHERE reservation_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS balance_ledger_request_id_unique
  ON balance_ledger (request_id)
  WHERE request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS balance_ledger_stripe_session_idx
  ON balance_ledger (stripe_session_id)
  WHERE stripe_session_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS gen_balance_write_tokens (
  backend_pid INTEGER NOT NULL,
  txid BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  PRIMARY KEY (backend_pid, txid)
);

REVOKE ALL ON TABLE gen_balance_write_tokens FROM PUBLIC;

CREATE OR REPLACE FUNCTION gen_guard_accounts_balance_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_has_token BOOLEAN;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.balance_cents <> 0 THEN
      RAISE EXCEPTION
        'accounts must be created with zero balance; use ledger functions for subsequent balance changes.';
    END IF;

    RETURN NEW;
  END IF;

  IF NEW.balance_cents IS DISTINCT FROM OLD.balance_cents THEN
    SELECT EXISTS(
      SELECT 1
      FROM gen_balance_write_tokens
      WHERE backend_pid = pg_backend_pid()
        AND txid = txid_current()
    )
    INTO v_has_token;

    IF NOT v_has_token THEN
      RAISE EXCEPTION
        'accounts.balance_cents must be changed via gen_apply_balance_ledger_entry() or its wrappers.';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS accounts_balance_cents_guard ON accounts;

DROP TRIGGER IF EXISTS accounts_balance_cents_insert_guard ON accounts;

CREATE TRIGGER accounts_balance_cents_insert_guard
BEFORE INSERT ON accounts
FOR EACH ROW
EXECUTE FUNCTION gen_guard_accounts_balance_mutation();

CREATE TRIGGER accounts_balance_cents_guard
BEFORE UPDATE OF balance_cents ON accounts
FOR EACH ROW
EXECUTE FUNCTION gen_guard_accounts_balance_mutation();

CREATE OR REPLACE FUNCTION gen_apply_balance_ledger_entry(
  p_account_id BIGINT,
  p_amount_cents BIGINT,
  p_entry_kind TEXT,
  p_reservation_id BIGINT DEFAULT NULL,
  p_price_id BIGINT DEFAULT NULL,
  p_stripe_session_id TEXT DEFAULT NULL,
  p_request_id TEXT DEFAULT NULL,
  p_description TEXT DEFAULT NULL,
  p_created_by_admin_id BIGINT DEFAULT NULL,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT,
  already_processed BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_created_at BIGINT;
  v_existing balance_ledger%ROWTYPE;
  v_backend_pid INTEGER;
  v_txid BIGINT;
BEGIN
  IF p_amount_cents = 0 THEN
    RAISE EXCEPTION 'Balance amount must be non-zero.';
  END IF;

  IF p_entry_kind NOT IN ('topup', 'admin_adjustment', 'generation_reserve', 'generation_refund') THEN
    RAISE EXCEPTION 'Unsupported balance ledger entry kind: %', p_entry_kind;
  END IF;

  v_created_at := COALESCE(
    p_created_at,
    FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
  );

  IF p_request_id IS NOT NULL THEN
    PERFORM gen_advisory_xact_lock('balance_ledger_request', p_request_id);

    SELECT *
    INTO v_existing
    FROM balance_ledger
    WHERE request_id = p_request_id
    LIMIT 1;

    IF FOUND THEN
      IF v_existing.account_id <> p_account_id
         OR v_existing.amount_cents <> p_amount_cents
         OR v_existing.entry_kind <> p_entry_kind
         OR v_existing.reservation_id IS DISTINCT FROM p_reservation_id
         OR v_existing.price_id IS DISTINCT FROM p_price_id
         OR v_existing.stripe_session_id IS DISTINCT FROM p_stripe_session_id
         OR v_existing.description IS DISTINCT FROM p_description
         OR v_existing.created_by_admin_id IS DISTINCT FROM p_created_by_admin_id THEN
        RAISE EXCEPTION
          'Request ID % is already associated with a different balance mutation.',
          p_request_id;
      END IF;

      RETURN QUERY
      SELECT
        v_existing.id AS ledger_id,
        v_existing.balance_after_cents AS balance_after_cents,
        v_existing.currency AS currency,
        TRUE AS already_processed;
      RETURN;
    END IF;
  END IF;

  v_backend_pid := pg_backend_pid();
  v_txid := txid_current();

  INSERT INTO gen_balance_write_tokens (backend_pid, txid, created_at)
  VALUES (v_backend_pid, v_txid, v_created_at)
  ON CONFLICT (backend_pid, txid) DO UPDATE
  SET created_at = EXCLUDED.created_at;

  BEGIN
    RETURN QUERY
    WITH updated AS (
      UPDATE accounts AS account_row
      SET balance_cents = balance_cents + p_amount_cents,
          updated_at = GREATEST(updated_at, v_created_at)
      WHERE id = p_account_id
        AND balance_cents + p_amount_cents >= 0
      RETURNING
        account_row.balance_cents AS next_balance_cents,
        account_row.currency AS account_currency
    ),
    inserted AS (
      INSERT INTO balance_ledger AS ledger_row (
        account_id,
        reservation_id,
        price_id,
        entry_kind,
        amount_cents,
        balance_after_cents,
        currency,
        stripe_session_id,
        request_id,
        description,
        created_by_admin_id,
        created_at
      )
      SELECT
        p_account_id,
        p_reservation_id,
        p_price_id,
        p_entry_kind,
        p_amount_cents,
        updated.next_balance_cents,
        updated.account_currency,
        p_stripe_session_id,
        p_request_id,
        p_description,
        p_created_by_admin_id,
        v_created_at
      FROM updated
      RETURNING
        ledger_row.id AS inserted_ledger_id,
        ledger_row.balance_after_cents AS inserted_balance_after_cents,
        ledger_row.currency AS inserted_currency
    )
    SELECT
      inserted.inserted_ledger_id AS ledger_id,
      inserted.inserted_balance_after_cents AS balance_after_cents,
      inserted.inserted_currency AS currency,
      FALSE AS already_processed
    FROM inserted AS inserted;

    IF NOT FOUND THEN
      PERFORM 1
      FROM accounts
      WHERE id = p_account_id
      LIMIT 1;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'Account % not found.', p_account_id;
      END IF;

      RAISE EXCEPTION
        USING
          ERRCODE = 'ZB001',
          MESSAGE = FORMAT('Insufficient balance for account %s.', p_account_id);
    END IF;

    DELETE FROM gen_balance_write_tokens
    WHERE backend_pid = v_backend_pid
      AND txid = v_txid;
  EXCEPTION
    WHEN OTHERS THEN
      DELETE FROM gen_balance_write_tokens
      WHERE backend_pid = v_backend_pid
        AND txid = v_txid;
      RAISE;
  END;
END;
$$;

CREATE OR REPLACE FUNCTION gen_reserve_generation_balance(
  p_account_id BIGINT,
  p_reservation_id BIGINT,
  p_price_id BIGINT,
  p_amount_cents BIGINT,
  p_request_id TEXT,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT ledger_id, balance_after_cents, currency
  FROM gen_apply_balance_ledger_entry(
    p_account_id,
    -ABS(p_amount_cents),
    'generation_reserve',
    p_reservation_id,
    p_price_id,
    NULL,
    p_request_id,
    NULL,
    NULL,
    p_created_at
  );
$$;

CREATE OR REPLACE FUNCTION gen_refund_generation_balance(
  p_account_id BIGINT,
  p_reservation_id BIGINT,
  p_price_id BIGINT,
  p_amount_cents BIGINT,
  p_request_id TEXT,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT ledger_id, balance_after_cents, currency
  FROM gen_apply_balance_ledger_entry(
    p_account_id,
    ABS(p_amount_cents),
    'generation_refund',
    p_reservation_id,
    p_price_id,
    NULL,
    p_request_id,
    NULL,
    NULL,
    p_created_at
  );
$$;

CREATE OR REPLACE FUNCTION gen_apply_account_topup(
  p_account_id BIGINT,
  p_amount_cents BIGINT,
  p_stripe_session_id TEXT DEFAULT NULL,
  p_request_id TEXT DEFAULT NULL,
  p_description TEXT DEFAULT NULL,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT,
  already_processed BOOLEAN
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT ledger_id, balance_after_cents, currency, already_processed
  FROM gen_apply_balance_ledger_entry(
    p_account_id,
    ABS(p_amount_cents),
    'topup',
    NULL,
    NULL,
    p_stripe_session_id,
    p_request_id,
    p_description,
    NULL,
    p_created_at
  );
$$;

CREATE OR REPLACE FUNCTION gen_reserve_user_generation_balance(
  p_user_id BIGINT,
  p_price_id BIGINT,
  p_amount_cents BIGINT,
  p_request_id TEXT,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_account_id BIGINT;
BEGIN
  SELECT id
  INTO v_account_id
  FROM accounts
  WHERE owner_kind = 'user'
    AND user_id = p_user_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Cannot reserve generation balance: user % account not found.', p_user_id;
  END IF;

  RETURN QUERY
  SELECT *
  FROM gen_reserve_generation_balance(
    v_account_id,
    NULL,
    p_price_id,
    p_amount_cents,
    p_request_id,
    p_created_at
  );
END;
$$;

CREATE OR REPLACE FUNCTION gen_refund_user_generation_balance(
  p_user_id BIGINT,
  p_price_id BIGINT,
  p_amount_cents BIGINT,
  p_request_id TEXT,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_account_id BIGINT;
BEGIN
  SELECT id
  INTO v_account_id
  FROM accounts
  WHERE owner_kind = 'user'
    AND user_id = p_user_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Cannot refund generation balance: user % account not found.', p_user_id;
  END IF;

  RETURN QUERY
  SELECT *
  FROM gen_refund_generation_balance(
    v_account_id,
    NULL,
    p_price_id,
    p_amount_cents,
    p_request_id,
    p_created_at
  );
END;
$$;

CREATE OR REPLACE FUNCTION gen_apply_user_account_topup(
  p_user_id BIGINT,
  p_amount_cents BIGINT,
  p_stripe_session_id TEXT DEFAULT NULL,
  p_request_id TEXT DEFAULT NULL,
  p_description TEXT DEFAULT NULL,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT,
  already_processed BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_account_id BIGINT;
BEGIN
  SELECT id
  INTO v_account_id
  FROM accounts
  WHERE owner_kind = 'user'
    AND user_id = p_user_id
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Cannot top up balance: user % account not found.', p_user_id;
  END IF;

  RETURN QUERY
  SELECT
    mutation.ledger_id,
    mutation.balance_after_cents,
    mutation.currency,
    mutation.already_processed
  FROM gen_apply_account_topup(
    v_account_id,
    p_amount_cents,
    p_stripe_session_id,
    p_request_id,
    p_description,
    p_created_at
  ) AS mutation;
END;
$$;

CREATE OR REPLACE FUNCTION gen_apply_admin_balance_adjustment(
  p_account_id BIGINT,
  p_amount_cents BIGINT,
  p_created_by_admin_id BIGINT,
  p_request_id TEXT DEFAULT NULL,
  p_description TEXT DEFAULT NULL,
  p_created_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  ledger_id BIGINT,
  balance_after_cents BIGINT,
  currency TEXT
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT ledger_id, balance_after_cents, currency
  FROM gen_apply_balance_ledger_entry(
    p_account_id,
    p_amount_cents,
    'admin_adjustment',
    NULL,
    NULL,
    NULL,
    p_request_id,
    p_description,
    p_created_by_admin_id,
    p_created_at
  );
$$;

REVOKE ALL ON FUNCTION gen_apply_balance_ledger_entry(BIGINT, BIGINT, TEXT, BIGINT, BIGINT, TEXT, TEXT, TEXT, BIGINT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_reserve_generation_balance(BIGINT, BIGINT, BIGINT, BIGINT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_refund_generation_balance(BIGINT, BIGINT, BIGINT, BIGINT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_apply_account_topup(BIGINT, BIGINT, TEXT, TEXT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_reserve_user_generation_balance(BIGINT, BIGINT, BIGINT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_refund_user_generation_balance(BIGINT, BIGINT, BIGINT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_apply_user_account_topup(BIGINT, BIGINT, TEXT, TEXT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION gen_apply_admin_balance_adjustment(BIGINT, BIGINT, BIGINT, TEXT, TEXT, BIGINT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION generation_submit_task(
  p_task_id UUID,
  p_account_id BIGINT,
  p_user_id BIGINT,
  p_company_id UUID,
  p_api_key_id BIGINT,
  p_task_kind TEXT,
  p_model TEXT,
  p_expected_results INTEGER,
  p_deadline_at BIGINT,
  p_gateway_name TEXT,
  p_client_origin TEXT DEFAULT 'api',
  p_request_json JSONB DEFAULT '{}'::jsonb,
  -- Registered free-tier convention: NULL/non-positive disables free usage.
  p_registered_generation_limit INTEGER DEFAULT NULL,
  p_registered_window_ms BIGINT DEFAULT NULL,
  p_now BIGINT DEFAULT NULL,
  -- Guest free-tier convention: NULL/non-positive disables guest usage.
  p_guest_generation_limit INTEGER DEFAULT NULL,
  p_guest_window_ms BIGINT DEFAULT NULL,
  p_guest_key_hash BYTEA DEFAULT NULL,
  p_guest_access_mode TEXT DEFAULT NULL,
  p_generic_global_limit INTEGER DEFAULT NULL,
  p_generic_per_ip_limit INTEGER DEFAULT NULL,
  p_generic_window_ms BIGINT DEFAULT NULL,
  p_generic_key_hash BYTEA DEFAULT NULL
)
RETURNS TABLE (
  ok BOOLEAN,
  error_code TEXT,
  error_message TEXT,
  reservation_id BIGINT,
  billing_mode TEXT,
  reservation_status TEXT,
  task_status TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_now BIGINT := COALESCE(
    p_now,
    FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
  );
  v_task generation_tasks%ROWTYPE;
  v_existing_reservation generation_reservations%ROWTYPE;
  v_account accounts%ROWTYPE;
  v_price model_prices%ROWTYPE;
  v_price_found BOOLEAN := FALSE;
  v_reservation generation_reservations%ROWTYPE;
  v_reservation_created BOOLEAN := FALSE;
  v_paid_reserve_insufficient_balance BOOLEAN := FALSE;
  v_registered_free_exhausted BOOLEAN := FALSE;
  v_guest_free_exhausted BOOLEAN := FALSE;
  v_generic_global_exhausted BOOLEAN := FALSE;
  v_generic_per_ip_exhausted BOOLEAN := FALSE;
  v_is_guest_task BOOLEAN := FALSE;
  v_expected_task_account_id BIGINT;
  v_request_json JSONB := COALESCE(p_request_json, '{}'::jsonb);
  v_client_origin TEXT := COALESCE(NULLIF(BTRIM(p_client_origin), ''), 'api');
  v_gateway_name TEXT := NULLIF(BTRIM(p_gateway_name), '');
  v_guest_access_mode TEXT := NULLIF(BTRIM(p_guest_access_mode), '');
BEGIN
  IF p_expected_results IS NULL OR p_expected_results <= 0 THEN
    RAISE EXCEPTION 'Expected results must be greater than zero.';
  END IF;

  IF p_task_id IS NULL THEN
    RAISE EXCEPTION 'Task ID is required.';
  END IF;

  v_is_guest_task := p_account_id IS NULL
    AND p_user_id IS NULL
    AND p_company_id IS NULL
    AND p_api_key_id IS NULL;

  IF v_guest_access_mode IS NOT NULL
     AND v_guest_access_mode NOT IN ('site_guest', 'generic_key') THEN
    RAISE EXCEPTION 'Guest access mode must be site_guest or generic_key.';
  END IF;

  IF v_is_guest_task THEN
    v_guest_access_mode := COALESCE(v_guest_access_mode, 'site_guest');
  ELSIF v_guest_access_mode IS NOT NULL THEN
    RAISE EXCEPTION 'Guest access mode is only allowed for guest task submission.';
  END IF;

  IF NOT v_is_guest_task AND p_account_id IS NULL THEN
    RAISE EXCEPTION 'Account ID is required.';
  END IF;

  IF NOT v_is_guest_task AND p_api_key_id IS NULL THEN
    RAISE EXCEPTION 'API key ID is required.';
  END IF;

  IF NOT v_is_guest_task
     AND ((p_user_id IS NULL AND p_company_id IS NULL)
     OR (p_user_id IS NOT NULL AND p_company_id IS NOT NULL)) THEN
    RAISE EXCEPTION 'Exactly one of user_id or company_id must be present.';
  END IF;

  IF p_deadline_at IS NULL OR p_deadline_at <= v_now THEN
    RAISE EXCEPTION 'Task deadline must be in the future.';
  END IF;

  PERFORM gen_advisory_xact_lock('generation_submit_task', p_task_id::TEXT);

  SELECT *
  INTO v_task
  FROM generation_tasks
  WHERE id = p_task_id
  LIMIT 1;

  IF FOUND THEN
    SELECT *
    INTO v_existing_reservation
    FROM generation_reservations
    WHERE id = v_task.reservation_id
    LIMIT 1;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Existing generation task % is missing reservation %.', p_task_id, v_task.reservation_id;
    END IF;

    v_expected_task_account_id := CASE
      WHEN v_existing_reservation.billing_mode IN ('user_free', 'guest_free') THEN NULL
      ELSE p_account_id
    END;

    IF v_task.account_id IS DISTINCT FROM v_expected_task_account_id
       OR v_task.user_id IS DISTINCT FROM p_user_id
       OR v_task.company_id IS DISTINCT FROM p_company_id
       OR v_task.api_key_id IS DISTINCT FROM p_api_key_id
       OR v_task.task_kind IS DISTINCT FROM p_task_kind
       OR v_task.model IS DISTINCT FROM p_model
       OR v_task.expected_results <> p_expected_results
       OR v_task.deadline_at <> p_deadline_at
       OR v_task.gateway_name IS DISTINCT FROM v_gateway_name
       OR v_task.client_origin IS DISTINCT FROM v_client_origin
       OR v_task.request_json IS DISTINCT FROM v_request_json
       OR (
         v_existing_reservation.billing_mode = 'guest_free'
         AND v_existing_reservation.guest_key_hash IS DISTINCT FROM
           CASE
             WHEN v_client_origin = 'guest_generic' THEN p_generic_key_hash
             ELSE p_guest_key_hash
           END
       ) THEN
      RAISE EXCEPTION 'Task % already exists with different reservation metadata.', p_task_id;
    END IF;

    RETURN QUERY
    SELECT
      TRUE,
      NULL::TEXT,
      NULL::TEXT,
      v_existing_reservation.id,
      v_existing_reservation.billing_mode,
      v_existing_reservation.status,
      v_task.status;
    RETURN;
  END IF;

  IF v_is_guest_task THEN
    IF v_guest_access_mode = 'generic_key' THEN
      IF v_client_origin <> 'guest_generic' THEN
        RAISE EXCEPTION 'Generic-key guest task submission requires client origin guest_generic.';
      END IF;

      IF p_generic_global_limit IS NULL
         OR p_generic_global_limit < 0
         OR p_generic_per_ip_limit IS NULL
         OR p_generic_per_ip_limit < 0
         OR p_generic_window_ms IS NULL
         OR p_generic_window_ms <= 0 THEN
        RAISE EXCEPTION 'Generic-key rolling-window settings are required for generic guest task submission.';
      END IF;

      IF p_generic_key_hash IS NULL OR octet_length(p_generic_key_hash) <> 16 THEN
        RAISE EXCEPTION 'Generic key hash is required for generic guest task submission.';
      END IF;

      PERFORM gen_advisory_xact_lock(
        'generation_submit_task_generic_global',
        'global'
      );
      PERFORM gen_advisory_xact_lock(
        'generation_submit_task_generic_ip',
        encode(p_generic_key_hash, 'hex')
      );

      SELECT COUNT(*)::INTEGER >= p_generic_global_limit
      INTO v_generic_global_exhausted
      FROM generation_reservations AS reservation_row
      INNER JOIN generation_tasks AS task_row
        ON task_row.reservation_id = reservation_row.id
      WHERE reservation_row.billing_mode = 'guest_free'
        AND reservation_row.status IN ('reserved', 'committed')
        AND reservation_row.created_at >= GREATEST(0, v_now - p_generic_window_ms)
        AND task_row.client_origin = 'guest_generic';

      IF COALESCE(v_generic_global_exhausted, FALSE) THEN
        RETURN QUERY
        SELECT
          FALSE,
          'daily_limit'::TEXT,
          'Generic key global rolling limit exceeded.'::TEXT,
          NULL::BIGINT,
          NULL::TEXT,
          NULL::TEXT,
          NULL::TEXT;
        RETURN;
      END IF;

      SELECT COUNT(*)::INTEGER >= p_generic_per_ip_limit
      INTO v_generic_per_ip_exhausted
      FROM generation_reservations AS reservation_row
      INNER JOIN generation_tasks AS task_row
        ON task_row.reservation_id = reservation_row.id
      WHERE reservation_row.billing_mode = 'guest_free'
        AND reservation_row.guest_key_hash = p_generic_key_hash
        AND reservation_row.status IN ('reserved', 'committed')
        AND reservation_row.created_at >= GREATEST(0, v_now - p_generic_window_ms)
        AND task_row.client_origin = 'guest_generic';

      IF COALESCE(v_generic_per_ip_exhausted, FALSE) THEN
        RETURN QUERY
        SELECT
          FALSE,
          'daily_limit'::TEXT,
          'Generic key per-IP rolling limit exceeded.'::TEXT,
          NULL::BIGINT,
          NULL::TEXT,
          NULL::TEXT,
          NULL::TEXT;
        RETURN;
      END IF;
    ELSE
      IF p_guest_generation_limit IS NULL
         OR p_guest_generation_limit <= 0
         OR p_guest_window_ms IS NULL
         OR p_guest_window_ms <= 0 THEN
        RETURN QUERY
        SELECT
          FALSE,
          'login_required'::TEXT,
          'Sign in to continue.'::TEXT,
          NULL::BIGINT,
          NULL::TEXT,
          NULL::TEXT,
          NULL::TEXT;
        RETURN;
      END IF;

      IF p_guest_key_hash IS NULL OR octet_length(p_guest_key_hash) <> 16 THEN
        RAISE EXCEPTION 'Guest key hash is required for guest task submission.';
      END IF;

      PERFORM gen_advisory_xact_lock(
        'generation_submit_task_guest_free',
        encode(p_guest_key_hash, 'hex')
      );

      SELECT COUNT(*)::INTEGER >= p_guest_generation_limit
      INTO v_guest_free_exhausted
      FROM generation_reservations AS reservation_row
      INNER JOIN generation_tasks AS task_row
        ON task_row.reservation_id = reservation_row.id
      WHERE reservation_row.billing_mode = 'guest_free'
        AND reservation_row.guest_key_hash = p_guest_key_hash
        AND reservation_row.status IN ('reserved', 'committed')
        AND reservation_row.created_at >= GREATEST(0, v_now - p_guest_window_ms)
        AND task_row.client_origin IS DISTINCT FROM 'guest_generic';

      IF COALESCE(v_guest_free_exhausted, FALSE) THEN
        RETURN QUERY
        SELECT
          FALSE,
          'login_required'::TEXT,
          'Sign in to continue.'::TEXT,
          NULL::BIGINT,
          NULL::TEXT,
          NULL::TEXT,
          NULL::TEXT;
        RETURN;
      END IF;
    END IF;

    INSERT INTO generation_reservations (
      reservation_key,
      billing_mode,
      status,
      account_id,
      user_id,
      company_id,
      api_key_id,
      guest_key_hash,
      price_id,
      task_kind,
      model,
      amount_cents,
      currency,
      release_reason,
      created_at,
      expires_at,
      finalized_at
    )
    VALUES (
      'gateway_task:' || p_task_id::TEXT,
      'guest_free',
      'reserved',
      NULL,
      NULL,
      NULL,
      NULL,
      CASE
        WHEN v_guest_access_mode = 'generic_key' THEN p_generic_key_hash
        ELSE p_guest_key_hash
      END,
      NULL,
      p_task_kind,
      p_model,
      0,
      'USD',
      NULL,
      v_now,
      p_deadline_at,
      NULL
    )
    RETURNING *
    INTO v_reservation;
    v_reservation_created := TRUE;
  ELSE
    IF p_user_id IS NOT NULL THEN
      SELECT *
      INTO v_account
      FROM accounts
      WHERE id = p_account_id
        AND user_id = p_user_id
      LIMIT 1;
    ELSE
      SELECT *
      INTO v_account
      FROM accounts
      WHERE id = p_account_id
        AND company_id = p_company_id
      LIMIT 1;
    END IF;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Account % does not match the billed owner for task %.', p_account_id, p_task_id;
    END IF;

    SELECT *
    INTO v_price
    FROM model_prices
    WHERE task_kind = p_task_kind
      AND model = p_model
      AND archived_at IS NULL
    LIMIT 1;

    v_price_found := FOUND;

    IF p_company_id IS NOT NULL THEN
      IF NOT v_price_found OR v_price.amount_cents <= 0 THEN
        RETURN QUERY
        SELECT
          FALSE,
          'pricing_unavailable'::TEXT,
          FORMAT('No active paid price configured for %s / %s.', p_task_kind, p_model),
          NULL::BIGINT,
          NULL::TEXT,
          NULL::TEXT,
          NULL::TEXT;
        RETURN;
      END IF;
      BEGIN
      -- Queue same-account paid submissions before touching the hot balance
      -- row. The debit path must serialize per account anyway, so taking this
      -- lock here avoids row-lock convoying deeper in the ledger mutation.
      PERFORM gen_advisory_xact_lock(
        'generation_submit_task_paid_account',
        p_account_id::TEXT
      );
      INSERT INTO generation_reservations (
        reservation_key,
        billing_mode,
        status,
        account_id,
        user_id,
        company_id,
        api_key_id,
        price_id,
        task_kind,
        model,
        amount_cents,
        currency,
        release_reason,
        created_at,
        expires_at,
        finalized_at
      )
      VALUES (
        'gateway_task:' || p_task_id::TEXT,
        'paid',
        'reserved',
        p_account_id,
        NULL,
        p_company_id,
        p_api_key_id,
        v_price.id,
        p_task_kind,
        p_model,
        v_price.amount_cents,
        v_price.currency,
        NULL,
        v_now,
        p_deadline_at,
        NULL
      )
      RETURNING *
      INTO v_reservation;

      PERFORM *
      FROM gen_reserve_generation_balance(
        p_account_id,
        v_reservation.id,
        v_price.id,
        v_price.amount_cents,
        'gateway:reserve:' || p_task_id::TEXT,
        v_now
      );

      v_reservation_created := TRUE;
    EXCEPTION
        WHEN SQLSTATE 'ZB001' THEN
          RETURN QUERY
          SELECT
            FALSE,
            'insufficient_balance'::TEXT,
            'Insufficient company balance.'::TEXT,
            NULL::BIGINT,
            NULL::TEXT,
            NULL::TEXT,
            NULL::TEXT;
          RETURN;
      END;
    ELSE
      IF p_registered_generation_limit IS NOT NULL
         AND p_registered_generation_limit > 0
         AND p_registered_window_ms IS NOT NULL
         AND p_registered_window_ms > 0 THEN
        PERFORM gen_advisory_xact_lock(
          'generation_submit_task_registered_free',
          p_user_id::TEXT
        );

        SELECT COUNT(*)::INTEGER >= p_registered_generation_limit
        INTO v_registered_free_exhausted
        FROM generation_reservations AS reservation_row
        WHERE reservation_row.billing_mode = 'user_free'
          AND reservation_row.user_id = p_user_id
          AND reservation_row.status IN ('reserved', 'committed')
          AND reservation_row.created_at >= GREATEST(0, v_now - p_registered_window_ms);

        IF NOT COALESCE(v_registered_free_exhausted, FALSE) THEN
          INSERT INTO generation_reservations (
            reservation_key,
            billing_mode,
            status,
            account_id,
            user_id,
            company_id,
            api_key_id,
            price_id,
            task_kind,
            model,
            amount_cents,
            currency,
            release_reason,
            created_at,
            expires_at,
            finalized_at
          )
          VALUES (
            'gateway_task:' || p_task_id::TEXT,
            'user_free',
            'reserved',
            NULL,
            p_user_id,
            NULL,
            p_api_key_id,
            NULL,
            p_task_kind,
            p_model,
            0,
            'USD',
            NULL,
            v_now,
            p_deadline_at,
            NULL
          )
          RETURNING *
          INTO v_reservation;
          v_reservation_created := TRUE;
        END IF;
      END IF;

      IF NOT v_reservation_created
         AND v_price_found
         AND v_price.amount_cents > 0 THEN
        BEGIN
          -- Apply the same per-account gate for the personal paid fallback.
          PERFORM gen_advisory_xact_lock(
            'generation_submit_task_paid_account',
            p_account_id::TEXT
          );
          INSERT INTO generation_reservations (
            reservation_key,
            billing_mode,
            status,
            account_id,
            user_id,
            company_id,
            api_key_id,
            price_id,
            task_kind,
            model,
            amount_cents,
            currency,
            release_reason,
            created_at,
            expires_at,
            finalized_at
          )
          VALUES (
            'gateway_task:' || p_task_id::TEXT,
            'paid',
            'reserved',
            p_account_id,
            p_user_id,
            NULL,
            p_api_key_id,
            v_price.id,
            p_task_kind,
            p_model,
            v_price.amount_cents,
            v_price.currency,
            NULL,
            v_now,
            p_deadline_at,
            NULL
          )
          RETURNING *
          INTO v_reservation;

          PERFORM *
          FROM gen_reserve_generation_balance(
            p_account_id,
            v_reservation.id,
            v_price.id,
            v_price.amount_cents,
            'gateway:reserve:' || p_task_id::TEXT,
            v_now
          );

          v_reservation_created := TRUE;
        EXCEPTION
          WHEN SQLSTATE 'ZB001' THEN
            v_reservation := NULL;
            v_reservation_created := FALSE;
            v_paid_reserve_insufficient_balance := TRUE;
        END;
      END IF;

      IF NOT v_reservation_created THEN
        RETURN QUERY
        SELECT
          FALSE,
          CASE
            WHEN v_paid_reserve_insufficient_balance THEN 'insufficient_balance'::TEXT
            ELSE 'pricing_unavailable'::TEXT
          END,
          CASE
            WHEN v_paid_reserve_insufficient_balance THEN 'Insufficient user balance.'::TEXT
            WHEN v_registered_free_exhausted THEN FORMAT(
              'Registered free limit exhausted and no active paid price configured for %s / %s.',
              p_task_kind,
              p_model
            )
            ELSE FORMAT('No active paid price configured for %s / %s.', p_task_kind, p_model)
          END,
          NULL::BIGINT,
          NULL::TEXT,
          NULL::TEXT,
          NULL::TEXT;
        RETURN;
      END IF;
    END IF;
  END IF;

  INSERT INTO generation_tasks (
    id,
    reservation_id,
    account_id,
    user_id,
    company_id,
    api_key_id,
    session_id,
    client_origin,
    task_kind,
    model,
    status,
    expected_results,
    finished_results_count,
    success_count,
    deadline_at,
    gateway_name,
    request_json,
    result_json,
    error_code,
    error_message,
    created_at,
    updated_at,
    started_at,
    completed_at
  )
  VALUES (
    p_task_id,
    v_reservation.id,
    v_reservation.account_id,
    v_reservation.user_id,
    v_reservation.company_id,
    v_reservation.api_key_id,
    NULL,
    v_client_origin,
    p_task_kind,
    p_model,
    'queued',
    p_expected_results,
    0,
    0,
    p_deadline_at,
    v_gateway_name,
    v_request_json,
    NULL,
    NULL,
    NULL,
    v_now,
    v_now,
    NULL,
    NULL
  );

  RETURN QUERY
  SELECT
    TRUE,
    NULL::TEXT,
    NULL::TEXT,
    v_reservation.id,
    v_reservation.billing_mode,
    v_reservation.status,
    'queued'::TEXT;
END;
$$;

CREATE OR REPLACE FUNCTION generation_record_task_assignment(
  p_task_id UUID,
  p_worker_hotkey TEXT,
  p_worker_id TEXT,
  p_assigned_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  assignment_id BIGINT,
  assignment_token UUID,
  task_status TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_now BIGINT := COALESCE(
    p_assigned_at,
    FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
  );
  v_task generation_tasks%ROWTYPE;
  v_assignment generation_task_assignments%ROWTYPE;
BEGIN
  SELECT *
  INTO v_task
  FROM generation_tasks
  WHERE id = p_task_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  SELECT *
  INTO v_assignment
  FROM generation_task_assignments
  WHERE task_id = p_task_id
    AND worker_hotkey = p_worker_hotkey
    AND status = 'assigned'
  ORDER BY assigned_at DESC
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    IF v_assignment.worker_id IS DISTINCT FROM p_worker_id THEN
      RAISE EXCEPTION
        'Task % already has an active assignment for worker hotkey % owned by worker_id %.',
        p_task_id,
        p_worker_hotkey,
        v_assignment.worker_id;
    END IF;

    RETURN QUERY
    SELECT v_assignment.id, v_assignment.assignment_token, v_task.status;
    RETURN;
  END IF;

  SELECT *
  INTO v_assignment
  FROM generation_task_assignments
  WHERE task_id = p_task_id
    AND worker_hotkey = p_worker_hotkey
    AND worker_id = p_worker_id
  ORDER BY completed_at DESC NULLS LAST, assigned_at DESC, id DESC
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    RETURN;
  END IF;

  IF v_task.status NOT IN ('queued', 'running') THEN
    RETURN;
  END IF;

  INSERT INTO generation_task_assignments (
    task_id,
    worker_hotkey,
    worker_id,
    status,
    assigned_at,
    completed_at,
    failure_reason,
    result_metadata_json
  )
  VALUES (
    p_task_id,
    p_worker_hotkey,
    p_worker_id,
    'assigned',
    v_now,
    NULL,
    NULL,
    '{}'::jsonb
  )
  RETURNING *
  INTO v_assignment;

  UPDATE generation_tasks
  SET status = CASE WHEN status = 'queued' THEN 'running' ELSE status END,
      started_at = COALESCE(started_at, v_now),
      updated_at = GREATEST(updated_at, v_now)
  WHERE id = p_task_id;

  RETURN QUERY
  SELECT
    v_assignment.id,
    v_assignment.assignment_token,
    CASE WHEN v_task.status = 'queued' THEN 'running' ELSE v_task.status END;
END;
$$;

CREATE OR REPLACE FUNCTION generation_record_task_assignments(
  p_task_ids UUID[],
  p_worker_hotkey TEXT,
  p_worker_id TEXT,
  p_assigned_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  task_id UUID,
  assignment_id BIGINT,
  assignment_token UUID,
  task_status TEXT,
  delivery_action TEXT
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  WITH runtime AS (
    SELECT COALESCE(
      p_assigned_at,
      FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
    ) AS assigned_at
  ),
  requested AS (
    SELECT task_id, ord
    FROM unnest(COALESCE(p_task_ids, ARRAY[]::UUID[])) WITH ORDINALITY AS requested(task_id, ord)
  ),
  unique_requested AS (
    SELECT DISTINCT task_id
    FROM requested
  ),
  locked_tasks AS (
    SELECT
      task_row.id,
      task_row.status
    FROM generation_tasks AS task_row
    WHERE task_row.id IN (SELECT task_id FROM unique_requested)
    ORDER BY task_row.id
    FOR UPDATE OF task_row
  ),
  conflicting_assignments AS (
    SELECT
      assignment_row.task_id,
      assignment_row.worker_id
    FROM locked_tasks
    JOIN generation_task_assignments AS assignment_row
      ON assignment_row.task_id = locked_tasks.id
    WHERE assignment_row.worker_hotkey = p_worker_hotkey
      AND assignment_row.status = 'assigned'
      AND assignment_row.worker_id IS DISTINCT FROM p_worker_id
    ORDER BY assignment_row.task_id, assignment_row.id
    FOR UPDATE OF assignment_row
  ),
  conflict_check AS (
    SELECT CASE
      WHEN EXISTS (SELECT 1 FROM conflicting_assignments)
        THEN pg_raise(
          FORMAT(
            'Task %s already has an active assignment for worker hotkey %s owned by a different worker_id.',
            (SELECT task_id::TEXT FROM conflicting_assignments LIMIT 1),
            p_worker_hotkey
          )
        )
      ELSE NULL
    END
  ),
  active_assignments AS (
    SELECT
      assignment_row.task_id,
      assignment_row.id AS assignment_id,
      assignment_row.assignment_token
    FROM generation_task_assignments AS assignment_row
    JOIN unique_requested AS requested_unique
      ON requested_unique.task_id = assignment_row.task_id
    WHERE assignment_row.worker_hotkey = p_worker_hotkey
      AND assignment_row.status = 'assigned'
      AND assignment_row.worker_id = p_worker_id
  ),
  historical_assignments AS (
    SELECT DISTINCT ON (assignment_row.task_id)
      assignment_row.task_id
    FROM generation_task_assignments AS assignment_row
    JOIN unique_requested AS requested_unique
      ON requested_unique.task_id = assignment_row.task_id
    WHERE assignment_row.worker_hotkey = p_worker_hotkey
      AND assignment_row.worker_id = p_worker_id
      AND assignment_row.status <> 'assigned'
    ORDER BY
      assignment_row.task_id,
      assignment_row.completed_at DESC NULLS LAST,
      assignment_row.assigned_at DESC,
      assignment_row.id DESC
  ),
  inserted_assignments AS (
    INSERT INTO generation_task_assignments (
      task_id,
      worker_hotkey,
      worker_id,
      status,
      assigned_at,
      completed_at,
      failure_reason,
      result_metadata_json
    )
    SELECT
      locked_tasks.id,
      p_worker_hotkey,
      p_worker_id,
      'assigned',
      runtime.assigned_at,
      NULL,
      NULL,
      '{}'::jsonb
    FROM locked_tasks
    CROSS JOIN runtime
    CROSS JOIN conflict_check
    LEFT JOIN active_assignments
      ON active_assignments.task_id = locked_tasks.id
    LEFT JOIN historical_assignments
      ON historical_assignments.task_id = locked_tasks.id
    WHERE active_assignments.task_id IS NULL
      AND historical_assignments.task_id IS NULL
      AND locked_tasks.status IN ('queued', 'running')
    RETURNING
      task_id,
      id AS assignment_id,
      assignment_token
  ),
  promoted_tasks AS (
    UPDATE generation_tasks AS task_row
    SET status = CASE WHEN task_row.status = 'queued' THEN 'running' ELSE task_row.status END,
        started_at = COALESCE(task_row.started_at, runtime.assigned_at),
        updated_at = GREATEST(task_row.updated_at, runtime.assigned_at)
    FROM runtime
    WHERE task_row.id IN (SELECT task_id FROM inserted_assignments)
    RETURNING task_row.id
  ),
  task_outcomes AS (
    SELECT
      requested_unique.task_id,
      COALESCE(active_assignments.assignment_id, inserted_assignments.assignment_id) AS assignment_id,
      COALESCE(active_assignments.assignment_token, inserted_assignments.assignment_token) AS assignment_token,
      CASE
        WHEN COALESCE(active_assignments.assignment_id, inserted_assignments.assignment_id) IS NOT NULL THEN
          CASE
            WHEN inserted_assignments.task_id IS NOT NULL AND locked_tasks.status = 'queued' THEN 'running'
            ELSE locked_tasks.status
          END
        WHEN historical_assignments.task_id IS NOT NULL THEN locked_tasks.status
        WHEN locked_tasks.id IS NULL THEN 'missing'
        ELSE locked_tasks.status
      END AS task_status,
      CASE
        WHEN COALESCE(active_assignments.assignment_id, inserted_assignments.assignment_id) IS NOT NULL THEN 'assigned'
        WHEN historical_assignments.task_id IS NOT NULL AND locked_tasks.status IN ('queued', 'running') THEN 'requeue'
        ELSE 'retire'
      END AS delivery_action
    FROM unique_requested AS requested_unique
    LEFT JOIN locked_tasks
      ON locked_tasks.id = requested_unique.task_id
    LEFT JOIN active_assignments
      ON active_assignments.task_id = requested_unique.task_id
    LEFT JOIN historical_assignments
      ON historical_assignments.task_id = requested_unique.task_id
    LEFT JOIN inserted_assignments
      ON inserted_assignments.task_id = requested_unique.task_id
  )
  SELECT
    requested.task_id,
    task_outcomes.assignment_id,
    task_outcomes.assignment_token,
    task_outcomes.task_status,
    task_outcomes.delivery_action
  FROM requested
  JOIN task_outcomes
    ON task_outcomes.task_id = requested.task_id
  ORDER BY requested.ord;
$$;

CREATE OR REPLACE FUNCTION generation_finalize_task_assignment(
  p_task_id UUID,
  p_worker_hotkey TEXT,
  p_worker_id TEXT,
  p_assignment_token UUID,
  p_assignment_status TEXT,
  p_failure_reason TEXT DEFAULT NULL,
  p_result_metadata_json JSONB DEFAULT '{}'::jsonb,
  p_completed_at BIGINT DEFAULT NULL
)
RETURNS TABLE (
  assignment_outcome TEXT,
  task_status TEXT,
  reservation_status TEXT,
  finished_results_count INTEGER,
  success_count INTEGER,
  stored_assignment_status TEXT,
  stored_failure_reason TEXT,
  stored_result_metadata_json JSONB
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_now BIGINT := COALESCE(
    p_completed_at,
    FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
  );
  v_task generation_tasks%ROWTYPE;
  v_reservation generation_reservations%ROWTYPE;
  v_assignment generation_task_assignments%ROWTYPE;
  v_metadata JSONB := COALESCE(p_result_metadata_json, '{}'::jsonb);
  v_finished_results_count INTEGER := 0;
  v_success_count INTEGER := 0;
  v_finished_delta INTEGER := 0;
  v_success_delta INTEGER := 0;
  v_error_code TEXT := NULL;
  v_error_message TEXT := NULL;
BEGIN
  IF p_assignment_status NOT IN ('succeeded', 'failed', 'cancelled') THEN
    RAISE EXCEPTION 'Unsupported assignment final status: %', p_assignment_status;
  END IF;

  IF p_assignment_status = 'succeeded' AND p_failure_reason IS NOT NULL THEN
    RAISE EXCEPTION 'Successful assignments cannot carry a failure reason.';
  END IF;

  SELECT *
  INTO v_task
  FROM generation_tasks
  WHERE id = p_task_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  SELECT *
  INTO v_reservation
  FROM generation_reservations
  WHERE id = v_task.reservation_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Task % is missing reservation %.', p_task_id, v_task.reservation_id;
  END IF;

  IF p_assignment_token IS NOT NULL THEN
    SELECT *
    INTO v_assignment
    FROM generation_task_assignments
    WHERE assignment_token = p_assignment_token
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Assignment token % does not exist.', p_assignment_token;
    END IF;

    IF v_assignment.task_id IS DISTINCT FROM p_task_id THEN
      RAISE EXCEPTION
        'Assignment token % belongs to task % and cannot finalize task %.',
        p_assignment_token,
        v_assignment.task_id,
        p_task_id;
    END IF;

    IF v_assignment.worker_hotkey IS DISTINCT FROM p_worker_hotkey THEN
      RAISE EXCEPTION
        'Assignment token % belongs to worker hotkey % and cannot be finalized by %.',
        p_assignment_token,
        v_assignment.worker_hotkey,
        p_worker_hotkey;
    END IF;

    IF v_assignment.worker_id IS DISTINCT FROM p_worker_id THEN
      RAISE EXCEPTION
        'Assignment token % for task % is owned by worker_id %.',
        p_assignment_token,
        p_task_id,
        v_assignment.worker_id;
    END IF;
  ELSE
    SELECT *
    INTO v_assignment
    FROM generation_task_assignments
    WHERE task_id = p_task_id
      AND worker_hotkey = p_worker_hotkey
      AND worker_id = p_worker_id
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION
        'Task % has no assignment for worker hotkey % and worker_id %.',
        p_task_id,
        p_worker_hotkey,
        p_worker_id;
    END IF;
  END IF;

  IF v_assignment.status = 'assigned' THEN
    UPDATE generation_task_assignments AS assignment_row
    SET status = p_assignment_status,
        completed_at = v_now,
        failure_reason = p_failure_reason,
        result_metadata_json = v_metadata
    WHERE assignment_row.id = v_assignment.id
    RETURNING assignment_row.*
    INTO v_assignment;

    assignment_outcome := 'applied';
    v_finished_delta := 1;
    v_success_delta := CASE WHEN p_assignment_status = 'succeeded' THEN 1 ELSE 0 END;
  ELSE
    stored_assignment_status := v_assignment.status;
    stored_failure_reason := v_assignment.failure_reason;
    stored_result_metadata_json := v_assignment.result_metadata_json;

    IF v_assignment.completed_at IS NULL THEN
      RAISE EXCEPTION
        'Assignment token % for task % is neither active nor completed.',
        p_assignment_token,
        p_task_id;
    END IF;

    IF v_assignment.status IS DISTINCT FROM p_assignment_status
       OR v_assignment.failure_reason IS DISTINCT FROM p_failure_reason
       OR v_assignment.result_metadata_json IS DISTINCT FROM v_metadata THEN
      assignment_outcome := 'duplicate_conflict';
      RETURN QUERY
      SELECT
        assignment_outcome,
        v_task.status,
        v_reservation.status,
        COALESCE(v_task.finished_results_count, 0),
        COALESCE(v_task.success_count, 0),
        stored_assignment_status,
        stored_failure_reason,
        stored_result_metadata_json;
      RETURN;
    END IF;

    assignment_outcome := 'duplicate_replay';
    RETURN QUERY
    SELECT
      assignment_outcome,
      v_task.status,
      v_reservation.status,
      COALESCE(v_task.finished_results_count, 0),
      COALESCE(v_task.success_count, 0),
      stored_assignment_status,
      stored_failure_reason,
      stored_result_metadata_json;
    RETURN;
  END IF;

  stored_assignment_status := v_assignment.status;
  stored_failure_reason := v_assignment.failure_reason;
  stored_result_metadata_json := v_assignment.result_metadata_json;

  v_finished_results_count := GREATEST(0, COALESCE(v_task.finished_results_count, 0)) + v_finished_delta;
  v_success_count := GREATEST(0, COALESCE(v_task.success_count, 0)) + v_success_delta;

  IF v_task.status IN ('succeeded', 'failed', 'timed_out', 'cancelled') THEN
    UPDATE generation_tasks
    SET finished_results_count = v_finished_results_count,
        success_count = v_success_count,
        updated_at = GREATEST(updated_at, v_now)
    WHERE id = p_task_id
    RETURNING status
    INTO task_status;

    SELECT status
    INTO reservation_status
    FROM generation_reservations
    WHERE id = v_reservation.id;

    finished_results_count := v_finished_results_count;
    success_count := v_success_count;
    RETURN NEXT;
    RETURN;
  END IF;

  IF v_success_count > 0
     AND v_reservation.status = 'reserved' THEN
    UPDATE generation_reservations
    SET status = 'committed',
        release_reason = NULL,
        finalized_at = v_now
    WHERE id = v_reservation.id
      AND status = 'reserved'
    RETURNING *
    INTO v_reservation;
  END IF;

  IF v_finished_results_count >= v_task.expected_results THEN
    IF v_success_count > 0 THEN
      UPDATE generation_tasks
      SET status = 'succeeded',
          finished_results_count = v_finished_results_count,
          success_count = v_success_count,
          updated_at = GREATEST(updated_at, v_now),
          completed_at = COALESCE(completed_at, v_now),
          result_json = COALESCE(
            result_json,
            jsonb_build_object(
              'worker_hotkey', p_worker_hotkey,
              'worker_id', p_worker_id,
              'metadata', v_metadata
            )
          ),
          error_code = NULL,
          error_message = NULL
      WHERE id = p_task_id
      RETURNING status
      INTO task_status;

      reservation_status := COALESCE(v_reservation.status, 'committed');
      finished_results_count := v_finished_results_count;
      success_count := v_success_count;
      RETURN NEXT;
      RETURN;
    END IF;

    v_error_code := CASE
      WHEN p_assignment_status = 'cancelled' THEN 'cancelled'
      ELSE 'worker_failed'
    END;
    v_error_message := COALESCE(p_failure_reason, 'Task finished without a successful result.');

    IF v_reservation.status = 'reserved' THEN
      IF v_reservation.billing_mode = 'paid' THEN
        PERFORM *
        FROM gen_refund_generation_balance(
          v_reservation.account_id,
          v_reservation.id,
          v_reservation.price_id,
          v_reservation.amount_cents,
          'gateway:release:' || p_task_id::TEXT,
          v_now
        );
      END IF;

      UPDATE generation_reservations
      SET status = 'released',
          release_reason = CASE
            WHEN p_assignment_status = 'cancelled' THEN 'cancelled'
            ELSE 'failed'
          END,
          finalized_at = v_now
      WHERE id = v_reservation.id
        AND status = 'reserved'
      RETURNING *
      INTO v_reservation;
    END IF;

    UPDATE generation_tasks
    SET status = CASE
          WHEN p_assignment_status = 'cancelled' THEN 'cancelled'
          ELSE 'failed'
        END,
        finished_results_count = v_finished_results_count,
        success_count = v_success_count,
        updated_at = GREATEST(updated_at, v_now),
        completed_at = COALESCE(completed_at, v_now),
        error_code = v_error_code,
        error_message = v_error_message
    WHERE id = p_task_id
    RETURNING status
    INTO task_status;

    reservation_status := COALESCE(v_reservation.status, 'released');
    finished_results_count := v_finished_results_count;
    success_count := v_success_count;
    RETURN NEXT;
    RETURN;
  END IF;

  UPDATE generation_tasks
  SET status = 'running',
      finished_results_count = v_finished_results_count,
      success_count = v_success_count,
      updated_at = GREATEST(updated_at, v_now),
      result_json = CASE
        WHEN p_assignment_status = 'succeeded' THEN COALESCE(
          result_json,
          jsonb_build_object(
            'worker_hotkey', p_worker_hotkey,
            'worker_id', p_worker_id,
            'metadata', v_metadata
          )
        )
        ELSE result_json
      END,
      error_code = NULL,
      error_message = NULL
  WHERE id = p_task_id
  RETURNING status
  INTO task_status;

  reservation_status := v_reservation.status;
  finished_results_count := v_finished_results_count;
  success_count := v_success_count;
  RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION generation_timeout_task(
  p_task_id UUID,
  p_now BIGINT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_now BIGINT := COALESCE(
    p_now,
    FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
  );
  v_task generation_tasks%ROWTYPE;
  v_reservation generation_reservations%ROWTYPE;
  v_finished_results_count INTEGER := 0;
  v_success_count INTEGER := 0;
  v_timed_out_assignments INTEGER := 0;
BEGIN
  SELECT *
  INTO v_task
  FROM generation_tasks
  WHERE id = p_task_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND OR v_task.status IN ('succeeded', 'failed', 'timed_out', 'cancelled') THEN
    RETURN FALSE;
  END IF;

  SELECT *
  INTO v_reservation
  FROM generation_reservations
  WHERE id = v_task.reservation_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Task % is missing reservation %.', p_task_id, v_task.reservation_id;
  END IF;

  WITH timed_out_rows AS (
    UPDATE generation_task_assignments
    SET status = 'timed_out',
        completed_at = v_now,
        failure_reason = COALESCE(failure_reason, 'gateway_timeout'),
        result_metadata_json = CASE
          WHEN result_metadata_json = '{}'::jsonb
            THEN jsonb_build_object('timeout', true)
          ELSE result_metadata_json
        END
    WHERE task_id = p_task_id
      AND status = 'assigned'
    RETURNING 1
  )
  SELECT COUNT(*)::INTEGER
  INTO v_timed_out_assignments
  FROM timed_out_rows;

  v_finished_results_count := GREATEST(0, COALESCE(v_task.finished_results_count, 0)) + v_timed_out_assignments;
  v_success_count := GREATEST(0, COALESCE(v_task.success_count, 0));

  IF v_reservation.status = 'reserved' THEN
    IF v_reservation.billing_mode = 'paid' THEN
      PERFORM *
      FROM gen_refund_generation_balance(
        v_reservation.account_id,
        v_reservation.id,
        v_reservation.price_id,
        v_reservation.amount_cents,
        'gateway:timeout:' || p_task_id::TEXT,
        v_now
      );
    END IF;

    UPDATE generation_reservations
    SET status = 'released',
        release_reason = 'timed_out',
        finalized_at = v_now
    WHERE id = v_reservation.id
      AND status = 'reserved';
  END IF;

  UPDATE generation_tasks
  SET status = 'timed_out',
      finished_results_count = v_finished_results_count,
      success_count = v_success_count,
      updated_at = GREATEST(updated_at, v_now),
      completed_at = COALESCE(completed_at, v_now),
      error_code = 'timed_out',
      error_message = 'Gateway task deadline exceeded.'
  WHERE id = p_task_id;

  RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION generation_expire_tasks(
  p_limit INTEGER DEFAULT 100,
  p_now BIGINT DEFAULT NULL
)
RETURNS TABLE (
  task_id UUID
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_now BIGINT := COALESCE(
    p_now,
    FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
  );
  v_candidate RECORD;
  v_reservation generation_reservations%ROWTYPE;
  v_timed_out_assignments INTEGER := 0;
  v_candidates JSONB := '[]'::JSONB;
BEGIN
  FOR v_candidate IN
    SELECT
      task_row.id AS task_id,
      task_row.reservation_id,
      task_row.deadline_at,
      GREATEST(0, COALESCE(task_row.finished_results_count, 0)) AS base_finished_results_count,
      GREATEST(0, COALESCE(task_row.success_count, 0)) AS base_success_count
    FROM generation_tasks AS task_row
    WHERE task_row.status IN ('queued', 'running')
      AND task_row.deadline_at <= v_now
    ORDER BY task_row.deadline_at ASC, task_row.id ASC
    LIMIT GREATEST(COALESCE(p_limit, 100), 1)
    FOR UPDATE SKIP LOCKED
  LOOP
    SELECT *
    INTO v_reservation
    FROM generation_reservations
    WHERE id = v_candidate.reservation_id
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Task % is missing reservation %.', v_candidate.task_id, v_candidate.reservation_id;
    END IF;

    WITH timed_out_rows AS (
      UPDATE generation_task_assignments AS assignment_row
      SET status = 'timed_out',
          completed_at = v_now,
          failure_reason = COALESCE(assignment_row.failure_reason, 'gateway_timeout'),
          result_metadata_json = CASE
            WHEN assignment_row.result_metadata_json = '{}'::jsonb
              THEN jsonb_build_object('timeout', true)
            ELSE assignment_row.result_metadata_json
          END
      WHERE assignment_row.task_id = v_candidate.task_id
        AND assignment_row.status = 'assigned'
      RETURNING 1
    )
    SELECT COUNT(*)::INTEGER
    INTO v_timed_out_assignments
    FROM timed_out_rows;

    v_candidates := v_candidates || jsonb_build_array(
      jsonb_build_object(
        'task_id', v_candidate.task_id,
        'reservation_id', v_candidate.reservation_id,
        'deadline_at', v_candidate.deadline_at,
        'now_ms', v_now,
        'base_finished_results_count', v_candidate.base_finished_results_count,
        'base_success_count', v_candidate.base_success_count,
        'reservation_status', v_reservation.status,
        'billing_mode', v_reservation.billing_mode,
        'account_id', v_reservation.account_id,
        'user_id', v_reservation.user_id,
        'price_id', v_reservation.price_id,
        'amount_cents', v_reservation.amount_cents,
        'timed_out_assignments', v_timed_out_assignments
      )
    );
  END LOOP;

  FOR v_candidate IN
    SELECT *
    FROM jsonb_to_recordset(v_candidates) AS candidate(
      task_id UUID,
      reservation_id BIGINT,
      deadline_at BIGINT,
      now_ms BIGINT,
      base_finished_results_count INTEGER,
      base_success_count INTEGER,
      reservation_status TEXT,
      billing_mode TEXT,
      account_id BIGINT,
      user_id BIGINT,
      price_id BIGINT,
      amount_cents BIGINT,
      timed_out_assignments INTEGER
    )
    WHERE candidate.reservation_status = 'reserved'
      AND candidate.billing_mode = 'paid'
    ORDER BY candidate.account_id ASC, candidate.reservation_id ASC
  LOOP
    PERFORM *
    FROM gen_refund_generation_balance(
      v_candidate.account_id,
      v_candidate.reservation_id,
      v_candidate.price_id,
      v_candidate.amount_cents,
      'gateway:timeout:' || v_candidate.task_id::TEXT,
      v_candidate.now_ms
    );
  END LOOP;

  FOR v_candidate IN
    SELECT *
    FROM jsonb_to_recordset(v_candidates) AS candidate(
      task_id UUID,
      reservation_id BIGINT,
      deadline_at BIGINT,
      now_ms BIGINT,
      base_finished_results_count INTEGER,
      base_success_count INTEGER,
      reservation_status TEXT,
      billing_mode TEXT,
      account_id BIGINT,
      user_id BIGINT,
      price_id BIGINT,
      amount_cents BIGINT,
      timed_out_assignments INTEGER
    )
    ORDER BY candidate.deadline_at ASC, candidate.task_id ASC
  LOOP
    IF v_candidate.reservation_status = 'reserved' THEN
      UPDATE generation_reservations AS reservation_row
      SET status = 'released',
          release_reason = 'timed_out',
          finalized_at = v_candidate.now_ms
      WHERE reservation_row.id = v_candidate.reservation_id
        AND reservation_row.status = 'reserved'
      RETURNING *
      INTO v_reservation;
    END IF;

    UPDATE generation_tasks AS task_row
    SET status = 'timed_out',
        finished_results_count = v_candidate.base_finished_results_count
          + COALESCE(v_candidate.timed_out_assignments, 0),
        success_count = v_candidate.base_success_count,
        updated_at = GREATEST(task_row.updated_at, v_candidate.now_ms),
        completed_at = COALESCE(task_row.completed_at, v_candidate.now_ms),
        error_code = 'timed_out',
        error_message = 'Gateway task deadline exceeded.'
    WHERE task_row.id = v_candidate.task_id;

    task_id := v_candidate.task_id;
    RETURN NEXT;
  END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION generation_purge_terminal_tasks(
  p_limit INTEGER DEFAULT 100,
  p_completed_before BIGINT DEFAULT NULL
)
RETURNS TABLE (
  task_id UUID
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  WITH validated AS (
    SELECT CASE
      WHEN p_completed_before IS NULL THEN pg_raise('generation_purge_terminal_tasks requires p_completed_before.')
      ELSE NULL
    END
  ),
  doomed AS (
    SELECT
      task_row.id,
      task_row.completed_at
    FROM generation_tasks AS task_row
    CROSS JOIN validated
    WHERE task_row.status IN ('succeeded', 'failed', 'timed_out', 'cancelled')
      AND task_row.completed_at IS NOT NULL
      AND task_row.completed_at <= p_completed_before
    ORDER BY task_row.completed_at ASC, task_row.id ASC
    LIMIT GREATEST(COALESCE(p_limit, 100), 1)
    FOR UPDATE SKIP LOCKED
  ),
  deleted AS (
    DELETE FROM generation_tasks AS task_row
    USING doomed
    WHERE task_row.id = doomed.id
    RETURNING task_row.id
  )
  SELECT doomed.id AS task_id
  FROM doomed
  INNER JOIN deleted
    ON deleted.id = doomed.id
  ORDER BY doomed.completed_at ASC, doomed.id ASC;
$$;

REVOKE ALL ON FUNCTION generation_submit_task(UUID, BIGINT, BIGINT, UUID, BIGINT, TEXT, TEXT, INTEGER, BIGINT, TEXT, TEXT, JSONB, INTEGER, BIGINT, BIGINT, INTEGER, BIGINT, BYTEA, TEXT, INTEGER, INTEGER, BIGINT, BYTEA) FROM PUBLIC;
REVOKE ALL ON FUNCTION generation_record_task_assignment(UUID, TEXT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION generation_record_task_assignments(UUID[], TEXT, TEXT, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION generation_finalize_task_assignment(UUID, TEXT, TEXT, UUID, TEXT, TEXT, JSONB, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION generation_timeout_task(UUID, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION generation_expire_tasks(INTEGER, BIGINT) FROM PUBLIC;
REVOKE ALL ON FUNCTION generation_purge_terminal_tasks(INTEGER, BIGINT) FROM PUBLIC;

-- Analytics

CREATE TABLE IF NOT EXISTS activity_events (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID,
  account_id BIGINT,
  user_id BIGINT,
  company_id UUID,
  action TEXT NOT NULL,
  event_family TEXT NOT NULL DEFAULT 'other' CHECK (event_family IN ('auth', 'other')),
  client_origin TEXT NOT NULL DEFAULT 'website',
  task_kind TEXT,
  model TEXT,
  gateway_name TEXT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS activity_events_task_id_idx
  ON activity_events (task_id)
  WHERE task_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS activity_events_account_created_idx
  ON activity_events (account_id, created_at DESC)
  WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS activity_events_user_created_idx
  ON activity_events (user_id, created_at DESC)
  WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS activity_events_company_created_idx
  ON activity_events (company_id, created_at DESC)
  WHERE company_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS activity_events_action_created_idx
  ON activity_events (action, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS activity_events_stripe_event_action_unique
  ON activity_events (
    gateway_name,
    action,
    ((metadata_json->>'eventId')),
    (COALESCE(metadata_json->>'error', ''))
  )
  WHERE gateway_name = 'stripe'
    AND metadata_json ? 'eventId';

CREATE INDEX IF NOT EXISTS activity_events_created_idx
  ON activity_events (created_at, id);

CREATE INDEX IF NOT EXISTS activity_events_event_family_created_idx
  ON activity_events (event_family, created_at, id);

CREATE TABLE IF NOT EXISTS stripe_webhook_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('processing', 'processed', 'failed')),
  handler_outcome TEXT,
  error TEXT,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL,
  completed_at BIGINT,
  CONSTRAINT stripe_webhook_events_handler_outcome_check CHECK (
    handler_outcome IS NULL
    OR handler_outcome IN ('processed', 'ignored')
  )
);

CREATE INDEX IF NOT EXISTS stripe_webhook_events_status_updated_idx
  ON stripe_webhook_events (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS worker_events (
  id BIGSERIAL PRIMARY KEY,
  task_id UUID,
  worker_id TEXT,
  action TEXT NOT NULL,
  task_kind TEXT NOT NULL,
  model TEXT,
  reason TEXT,
  gateway_name TEXT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS worker_events_worker_created_idx
  ON worker_events (worker_id, created_at DESC)
  WHERE worker_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS worker_events_task_idx
  ON worker_events (task_id)
  WHERE task_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS worker_events_gateway_created_idx
  ON worker_events (gateway_name, created_at DESC)
  WHERE gateway_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS worker_events_created_idx
  ON worker_events (created_at, id);

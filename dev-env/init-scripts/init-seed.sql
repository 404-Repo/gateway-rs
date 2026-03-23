-- Dev seed data for the gateway dev environment.
-- Personal API key plaintext: 123e4567-e89b-12d3-a456-426614174001
-- Company API key plaintext: 223e4567-e89b-12d3-a456-426614174002

DO $$
DECLARE
  v_now BIGINT := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT;
  v_company_id UUID := '123e4567-e89b-12d3-a456-426614174100';
BEGIN
  INSERT INTO users (
    id,
    email,
    display_name,
    created_at,
    updated_at,
    last_login_at
  )
  VALUES (
    1,
    'user@example.com',
    'Example User',
    v_now,
    v_now,
    v_now
  )
  ON CONFLICT DO NOTHING;

  INSERT INTO accounts (
    id,
    owner_kind,
    user_id,
    company_id,
    balance_cents,
    currency,
    stripe_customer_id,
    created_at,
    updated_at
  )
  VALUES (
    1,
    'user',
    1,
    NULL,
    0,
    'USD',
    NULL,
    v_now,
    v_now
  )
  ON CONFLICT DO NOTHING;

  INSERT INTO companies (
    id,
    name,
    owner_email,
    vat,
    task_limit_concurrent,
    task_limit_daily,
    ownership_state,
    created_by_user_id,
    created_at,
    updated_at
  )
  VALUES (
    v_company_id,
    'Example Company',
    'billing@example.com',
    NULL,
    1,
    600,
    'unassigned',
    1,
    v_now,
    v_now
  )
  ON CONFLICT DO NOTHING;

  INSERT INTO accounts (
    id,
    owner_kind,
    user_id,
    company_id,
    balance_cents,
    currency,
    stripe_customer_id,
    created_at,
    updated_at
  )
  VALUES (
    2,
    'company',
    NULL,
    v_company_id,
    0,
    'USD',
    NULL,
    v_now,
    v_now
  )
  ON CONFLICT DO NOTHING;

  INSERT INTO api_keys (
    id,
    account_id,
    key_scope,
    user_id,
    company_id,
    name,
    api_key_hash,
    api_key_encrypted,
    api_key_partial,
    is_primary,
    created_by_user_id,
    created_at,
    updated_at,
    last_used_at,
    revoked_at
  )
  VALUES
    (
      1,
      1,
      'personal',
      1,
      NULL,
      'Primary personal API key',
      decode('4021bfe31dd2dc7ac3b6991ecaed73eaa82b56eec2f53ec861ac43214614d5b3', 'hex'),
      decode('01000102030405060708090a0bbe85ab7683350cec07d0f0535204692e5f0c2c9dfef022983bacf52272b27e514117012ce01297df4df182196b9dd0dec4075d2f', 'hex'),
      '14174001',
      TRUE,
      1,
      v_now,
      v_now,
      NULL,
      NULL
    ),
    (
      2,
      2,
      'company',
      NULL,
      v_company_id,
      'Example company API key',
      decode('bba18a3a5649e7720b73de7d69eb9764093dc33831075fb267f85cf1f12ae946', 'hex'),
      NULL,
      '14174002',
      FALSE,
      1,
      v_now,
      v_now,
      NULL,
      NULL
    )
  ON CONFLICT DO NOTHING;

  PERFORM *
  FROM gen_apply_account_topup(
    2,
    5000,
    NULL,
    'gateway_dev_seed_company_topup',
    'gateway dev seed company balance',
    v_now
  );

  PERFORM setval(
    pg_get_serial_sequence('users', 'id'),
    GREATEST(COALESCE((SELECT MAX(id) FROM users), 1), 1),
    TRUE
  );
  PERFORM setval(
    pg_get_serial_sequence('accounts', 'id'),
    GREATEST(COALESCE((SELECT MAX(id) FROM accounts), 1), 1),
    TRUE
  );
  PERFORM setval(
    pg_get_serial_sequence('api_keys', 'id'),
    GREATEST(COALESCE((SELECT MAX(id) FROM api_keys), 1), 1),
    TRUE
  );
  PERFORM setval(
    pg_get_serial_sequence('model_prices', 'id'),
    GREATEST(COALESCE((SELECT MAX(id) FROM model_prices), 1), 1),
    TRUE
  );
END;
$$;

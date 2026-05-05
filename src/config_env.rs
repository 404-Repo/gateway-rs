use anyhow::{Result, anyhow};
use std::{env, path::Path};
use uuid::Uuid;

use crate::config::NodeConfig;

pub(crate) const API_KEY_SECRET_ENV: &str = "GATEWAY_API_KEY_SECRET";
pub(crate) const ADMIN_KEY_ENV: &str = "GATEWAY_ADMIN_KEY";
pub(crate) const DB_HOST_ENV: &str = "GATEWAY_DB_HOST";
pub(crate) const DB_USER_ENV: &str = "GATEWAY_DB_USER";
pub(crate) const DB_PASSWORD_ENV: &str = "GATEWAY_DB_PASSWORD";
pub(crate) const DB_NAME_ENV: &str = "GATEWAY_DB_NAME";
pub(crate) const ALLOW_DANGEROUS_SKIP_VERIFICATION_ENV: &str =
    "GATEWAY_ALLOW_DANGEROUS_SKIP_VERIFICATION";

type NormalizeFn = fn(String) -> Result<String>;

struct ConfigEnvBinding {
    env_name: &'static str,
    section: &'static str,
    field: &'static str,
    normalize: NormalizeFn,
}

macro_rules! config_env_binding {
    ($env_name:ident, $section:ident.$field:ident) => {
        ConfigEnvBinding {
            env_name: $env_name,
            section: stringify!($section),
            field: stringify!($field),
            normalize: normalize_passthrough,
        }
    };
    ($env_name:ident, $section:ident.$field:ident, $normalize:expr) => {
        ConfigEnvBinding {
            env_name: $env_name,
            section: stringify!($section),
            field: stringify!($field),
            normalize: $normalize,
        }
    };
}

const CONFIG_ENV_BINDINGS: &[ConfigEnvBinding] = &[
    config_env_binding!(API_KEY_SECRET_ENV, http.api_key_secret),
    config_env_binding!(ADMIN_KEY_ENV, http.admin_key, normalize_admin_key),
    config_env_binding!(DB_HOST_ENV, db.host),
    config_env_binding!(DB_USER_ENV, db.user),
    config_env_binding!(DB_PASSWORD_ENV, db.password),
    config_env_binding!(DB_NAME_ENV, db.db),
];

trait EnvSource {
    fn get(&self, name: &str) -> Option<String>;
}

struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn get(&self, name: &str) -> Option<String> {
        env::var(name).ok()
    }
}

#[cfg(test)]
struct OverrideEnv<'a> {
    overrides: &'a [(&'a str, &'a str)],
}

#[cfg(test)]
impl EnvSource for OverrideEnv<'_> {
    fn get(&self, name: &str) -> Option<String> {
        self.overrides
            .iter()
            .rev()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_string())
    }
}

pub(crate) fn dangerous_skip_verification_allowed() -> bool {
    matches!(
        env::var(ALLOW_DANGEROUS_SKIP_VERIFICATION_ENV),
        Ok(value)
            if matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
    )
}

pub(crate) fn parse_config_from_contents(path: &Path, contents: &str) -> Result<NodeConfig> {
    parse_config_from_contents_with_source(path, contents, &ProcessEnv)
}

#[cfg(test)]
pub(crate) async fn read_config_from_path_with_overrides<P: AsRef<Path>>(
    path: P,
    overrides: &[(&str, &str)],
) -> Result<NodeConfig> {
    let path = path.as_ref();
    let contents = tokio::fs::read_to_string(path).await?;
    parse_config_from_contents_with_overrides(path, &contents, overrides)
}

#[cfg(test)]
fn parse_config_from_contents_with_overrides(
    path: &Path,
    contents: &str,
    overrides: &[(&str, &str)],
) -> Result<NodeConfig> {
    parse_config_from_contents_with_source(path, contents, &OverrideEnv { overrides })
}

fn parse_config_from_contents_with_source(
    path: &Path,
    contents: &str,
    env_source: &impl EnvSource,
) -> Result<NodeConfig> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("toml") => {
            let mut config: toml::Value = toml::from_str(contents)?;
            apply_env_overrides_to_toml(&mut config, env_source)?;
            let config: NodeConfig = config.try_into()?;
            Ok(config)
        }
        Some("json") => {
            let mut config: serde_json::Value = serde_json::from_str(contents)?;
            apply_env_overrides_to_json(&mut config, env_source)?;
            let config: NodeConfig = serde_json::from_value(config)?;
            Ok(config)
        }
        _ => Err(anyhow!("Unsupported file format")),
    }
}

fn apply_env_overrides_to_toml(root: &mut toml::Value, env_source: &impl EnvSource) -> Result<()> {
    for binding in CONFIG_ENV_BINDINGS {
        if let Some(value) = read_env_override(binding, env_source)? {
            set_toml_string(root, binding, &value)?;
        }
    }
    Ok(())
}

fn apply_env_overrides_to_json(
    root: &mut serde_json::Value,
    env_source: &impl EnvSource,
) -> Result<()> {
    for binding in CONFIG_ENV_BINDINGS {
        if let Some(value) = read_env_override(binding, env_source)? {
            set_json_string(root, binding, &value)?;
        }
    }
    Ok(())
}

fn read_env_override(
    binding: &ConfigEnvBinding,
    env_source: &impl EnvSource,
) -> Result<Option<String>> {
    let Some(raw_value) = env_source.get(binding.env_name) else {
        return Ok(None);
    };
    Ok(Some((binding.normalize)(raw_value)?))
}

fn normalize_passthrough(value: String) -> Result<String> {
    Ok(value)
}

fn normalize_admin_key(value: String) -> Result<String> {
    let trimmed = value.trim();
    let parsed =
        Uuid::parse_str(trimmed).map_err(|err| anyhow!("Invalid {}: {}", ADMIN_KEY_ENV, err))?;
    Ok(parsed.to_string())
}

fn set_toml_string(root: &mut toml::Value, binding: &ConfigEnvBinding, value: &str) -> Result<()> {
    let current = root
        .as_table_mut()
        .ok_or_else(|| anyhow!("TOML config root must be a table"))?;
    let section = current
        .entry(binding.section.to_string())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or_else(|| {
            anyhow!(
                "Cannot apply {} because '{}' is not a table",
                binding.env_name,
                binding.section
            )
        })?;
    section.insert(
        binding.field.to_string(),
        toml::Value::String(value.to_string()),
    );
    Ok(())
}

fn set_json_string(
    root: &mut serde_json::Value,
    binding: &ConfigEnvBinding,
    value: &str,
) -> Result<()> {
    let current = root
        .as_object_mut()
        .ok_or_else(|| anyhow!("JSON config root must be an object"))?;
    let section = current
        .entry(binding.section.to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
        .as_object_mut()
        .ok_or_else(|| {
            anyhow!(
                "Cannot apply {} because '{}' is not an object",
                binding.env_name,
                binding.section
            )
        })?;
    section.insert(
        binding.field.to_string(),
        serde_json::Value::String(value.to_string()),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ADMIN_KEY_ENV, API_KEY_SECRET_ENV, DB_HOST_ENV, DB_NAME_ENV, DB_PASSWORD_ENV, DB_USER_ENV,
        read_config_from_path_with_overrides,
    };
    use crate::config::NodeConfig;
    use anyhow::Result;
    use tempfile::Builder;

    fn read_config_single() -> String {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("dev-env/config/config-single.toml");
        std::fs::read_to_string(path).expect("read config-single.toml")
    }

    fn remove_line(config_text: &str, line: &str) -> String {
        config_text.replacen(&format!("{line}\n"), "", 1)
    }

    fn remove_env_backed_fields(config_text: &str) -> String {
        let config_text = remove_line(
            config_text,
            "admin_key = \"b6c8597a-00e9-493a-b6cd-5dfc7244d46b\"",
        );
        let config_text = remove_line(
            &config_text,
            "api_key_secret = \"CHANGE_ME_IN_PRODUCTION_58392047\"",
        );
        let config_text = remove_line(&config_text, "host = \"db\"");
        let config_text = remove_line(&config_text, "user = \"postgres\"");
        let config_text = remove_line(&config_text, "password = \"api_keys_!54321\"");
        remove_line(&config_text, "db = \"api_keys_db\"")
    }

    fn env_backed_overrides() -> [(&'static str, &'static str); 6] {
        [
            (API_KEY_SECRET_ENV, "env-secret-for-hashing"),
            (ADMIN_KEY_ENV, "11111111-1111-1111-1111-111111111111"),
            (DB_HOST_ENV, "env-db-host"),
            (DB_USER_ENV, "env-db-user"),
            (DB_PASSWORD_ENV, "env-db-password"),
            (DB_NAME_ENV, "env-db-name"),
        ]
    }

    async fn load_temp_config(contents: &str, overrides: &[(&str, &str)]) -> Result<NodeConfig> {
        let file = Builder::new().suffix(".toml").tempfile()?;
        std::fs::write(file.path(), contents)?;
        read_config_from_path_with_overrides(file.path(), overrides).await
    }

    #[tokio::test]
    async fn read_config_prefers_file_values_when_env_is_absent() -> Result<()> {
        let config = load_temp_config(&read_config_single(), &[]).await?;

        assert_eq!(
            config.http.api_key_secret,
            "CHANGE_ME_IN_PRODUCTION_58392047"
        );
        assert_eq!(
            config.http.admin_key.to_string(),
            "b6c8597a-00e9-493a-b6cd-5dfc7244d46b"
        );
        assert_eq!(config.db.host, "db");
        assert_eq!(config.db.user, "postgres");
        assert_eq!(config.db.password, "api_keys_!54321");
        assert_eq!(config.db.db, "api_keys_db");

        Ok(())
    }

    #[tokio::test]
    async fn read_config_uses_env_values_when_file_omits_env_backed_fields() -> Result<()> {
        let config_text = remove_env_backed_fields(&read_config_single());
        let overrides = env_backed_overrides();
        let config = load_temp_config(&config_text, &overrides).await?;

        assert_eq!(config.http.api_key_secret, "env-secret-for-hashing");
        assert_eq!(
            config.http.admin_key.to_string(),
            "11111111-1111-1111-1111-111111111111"
        );
        assert_eq!(config.db.host, "env-db-host");
        assert_eq!(config.db.user, "env-db-user");
        assert_eq!(config.db.password, "env-db-password");
        assert_eq!(config.db.db, "env-db-name");

        Ok(())
    }

    #[tokio::test]
    async fn read_config_env_overrides_file_values() -> Result<()> {
        let overrides = env_backed_overrides();
        let config = load_temp_config(&read_config_single(), &overrides).await?;

        assert_eq!(config.http.api_key_secret, "env-secret-for-hashing");
        assert_eq!(
            config.http.admin_key.to_string(),
            "11111111-1111-1111-1111-111111111111"
        );
        assert_eq!(config.db.host, "env-db-host");
        assert_eq!(config.db.user, "env-db-user");
        assert_eq!(config.db.password, "env-db-password");
        assert_eq!(config.db.db, "env-db-name");

        Ok(())
    }

    #[tokio::test]
    async fn read_config_fails_when_env_backed_field_is_missing_everywhere() -> Result<()> {
        let config_text = remove_line(
            &read_config_single(),
            "api_key_secret = \"CHANGE_ME_IN_PRODUCTION_58392047\"",
        );
        let err = load_temp_config(&config_text, &[])
            .await
            .expect_err("missing field must fail");

        assert!(err.to_string().contains("api_key_secret"));
        Ok(())
    }

    #[tokio::test]
    async fn read_config_fails_on_invalid_admin_key_override() -> Result<()> {
        let overrides = [
            (ADMIN_KEY_ENV, "not-a-uuid"),
            (API_KEY_SECRET_ENV, "env-secret-for-hashing"),
        ];
        let err = load_temp_config(&read_config_single(), &overrides)
            .await
            .expect_err("invalid admin key override must fail");

        assert!(err.to_string().contains(ADMIN_KEY_ENV));
        Ok(())
    }
}

use std::collections::HashMap;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelCatalog {
    models_by_task_kind: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelCatalogResolveError {
    Unavailable {
        task_kind: String,
    },
    Unsupported {
        task_kind: String,
        model: String,
        known: Vec<String>,
    },
}

impl ModelCatalog {
    pub(crate) fn from_rows(rows: Vec<(String, String)>) -> Self {
        let mut models_by_task_kind = HashMap::<String, Vec<String>>::new();
        for (task_kind, model) in rows {
            models_by_task_kind
                .entry(task_kind)
                .or_default()
                .push(model);
        }
        for models in models_by_task_kind.values_mut() {
            models.sort_unstable();
            models.dedup();
        }
        Self {
            models_by_task_kind,
        }
    }

    pub fn resolve(
        &self,
        task_kind: &str,
        requested: Option<&str>,
        default_model: &str,
    ) -> Result<String, ModelCatalogResolveError> {
        let Some(known) = self.models_by_task_kind.get(task_kind) else {
            return Err(ModelCatalogResolveError::Unavailable {
                task_kind: task_kind.to_string(),
            });
        };

        let model = requested.unwrap_or(default_model);
        if known.iter().any(|known_model| known_model == model) {
            return Ok(model.to_string());
        }

        if requested.is_none() {
            return Err(ModelCatalogResolveError::Unavailable {
                task_kind: task_kind.to_string(),
            });
        }

        Err(ModelCatalogResolveError::Unsupported {
            task_kind: task_kind.to_string(),
            model: model.to_string(),
            known: known.clone(),
        })
    }

    pub fn model_count(&self) -> usize {
        self.models_by_task_kind.values().map(Vec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelCatalog, ModelCatalogResolveError};

    #[test]
    fn resolves_requested_model_for_matching_task_kind() {
        let catalog = ModelCatalog::from_rows(vec![
            ("image_to_3d".to_string(), "first".to_string()),
            ("image_to_3d".to_string(), "second".to_string()),
        ]);

        assert_eq!(
            catalog.resolve("image_to_3d", Some("second"), "first"),
            Ok("second".to_string())
        );
    }

    #[test]
    fn configured_model_is_the_stable_default_for_task_kind() {
        let catalog = ModelCatalog::from_rows(vec![
            ("image_to_3d".to_string(), "z-model".to_string()),
            ("image_to_3d".to_string(), "a-model".to_string()),
        ]);

        assert_eq!(
            catalog.resolve("image_to_3d", None, "z-model"),
            Ok("z-model".to_string())
        );
    }

    #[test]
    fn rejects_model_supported_only_for_another_task_kind() {
        let catalog = ModelCatalog::from_rows(vec![
            ("image_to_3d".to_string(), "mesh".to_string()),
            ("text_to_3d".to_string(), "splat".to_string()),
        ]);

        assert!(matches!(
            catalog.resolve("text_to_3d", Some("mesh"), "splat"),
            Err(ModelCatalogResolveError::Unsupported { .. })
        ));
    }

    #[test]
    fn missing_task_kind_is_unavailable_for_explicit_model() {
        let catalog =
            ModelCatalog::from_rows(vec![("image_to_3d".to_string(), "mesh".to_string())]);

        assert_eq!(
            catalog.resolve("text_to_3d", Some("splat"), "splat"),
            Err(ModelCatalogResolveError::Unavailable {
                task_kind: "text_to_3d".to_string()
            })
        );
    }

    #[test]
    fn empty_task_kind_fails_closed() {
        let catalog = ModelCatalog::default();

        assert_eq!(
            catalog.resolve("text_to_3d", None, "splat"),
            Err(ModelCatalogResolveError::Unavailable {
                task_kind: "text_to_3d".to_string()
            })
        );
    }

    #[test]
    fn missing_configured_default_fails_closed_instead_of_selecting_another_model() {
        let catalog =
            ModelCatalog::from_rows(vec![("image_to_3d".to_string(), "other-model".to_string())]);

        assert_eq!(
            catalog.resolve("image_to_3d", None, "configured-default"),
            Err(ModelCatalogResolveError::Unavailable {
                task_kind: "image_to_3d".to_string()
            })
        );
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("item is not a document")]
    ItemIsNotADocument,
    #[error(transparent)]
    Serde(toml_edit::de::Error),
}

pub fn deserialize_item<T>(item: &toml_edit::Item) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    let document: toml_edit::DocumentMut = match item {
        toml_edit::Item::Table(toml) => toml.clone().into(),
        toml_edit::Item::Value(toml_edit::Value::InlineTable(toml)) => {
            toml.clone().into_table().into()
        }
        _ => return Err(Error::ItemIsNotADocument),
    };

    toml_edit::de::from_document(document).map_err(Error::Serde)
}

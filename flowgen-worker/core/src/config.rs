use handlebars::Handlebars;
use serde::{de::DeserializeOwned, Serialize};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Render(#[from] handlebars::RenderError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

pub trait ConfigExt {
    fn render<T>(&self, data: &T) -> Result<Self, Error>
    where
        Self: Serialize + DeserializeOwned + Sized,
        T: Serialize,
    {
        let template = serde_json::to_string(self)?;
        let data = serde_json::to_value(data)?;

        let handlebars = Handlebars::new();
        let rendered = handlebars.render_template(&template, &data)?;

        let result: Self = serde_json::from_str(&rendered)?;
        Ok(result)
    }
}

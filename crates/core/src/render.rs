use handlebars::Handlebars;
use serde::Serialize;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RenderError {
    #[error("There was an issue with rendering string with data.")]
    Render(#[source] handlebars::RenderError),
}
pub trait Render {
    type Error;
    fn render<T>(&self, data: &T) -> Result<String, Self::Error>
    where
        T: Serialize;
}

impl Render for str {
    type Error = RenderError;
    fn render<T>(&self, data: &T) -> Result<String, Self::Error>
    where
        T: Serialize,
    {
        let handlebars = Handlebars::new();
        let rendered = handlebars
            .render_template(self, &data)
            .map_err(RenderError::Render)?;
        Ok(rendered)
    }
}

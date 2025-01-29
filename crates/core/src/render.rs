use handlebars::Handlebars;
use serde::Serialize;

#[derive(thiserror::Error, Debug)]
pub enum Error {
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
    type Error = Error;
    fn render<T>(&self, data: &T) -> Result<String, Self::Error>
    where
        T: Serialize,
    {
        let handlebars = Handlebars::new();
        let rendered = handlebars
            .render_template(self, &data)
            .map_err(Error::Render)?;
        Ok(rendered)
    }
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EventError {
    #[error("Missing required attributes.")]
    MissingRequiredAttribute(String),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Event {
    pub data: arrow::array::RecordBatch,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: String,
    pub current_task_id: Option<usize>,
}

#[derive(PartialEq, Debug, Clone, Default)]
pub struct EventBuilder {
    pub data: Option<arrow::array::RecordBatch>,
    pub extensions: Option<arrow::array::RecordBatch>,
    pub subject: Option<String>,
    pub current_task_id: Option<usize>,
}

impl EventBuilder {
    pub fn new() -> Self {
        EventBuilder {
            ..Default::default()
        }
    }
    pub fn data(mut self, data: arrow::array::RecordBatch) -> Self {
        self.data = Some(data);
        self
    }
    pub fn subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }
    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = Some(current_task_id);
        self
    }
    pub fn extensions(mut self, extensions: arrow::array::RecordBatch) -> Self {
        self.extensions = Some(extensions);
        self
    }
    pub fn build(self) -> Result<Event, EventError> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| EventError::MissingRequiredAttribute("data".to_string()))?,
            extensions: self.extensions,
            subject: self
                .subject
                .ok_or_else(|| EventError::MissingRequiredAttribute("subject".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}

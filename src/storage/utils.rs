
pub struct NameUtils;

pub struct QueueNames<'q>{
    application: &'q str,
    inner: String
}

impl<'q> QueueNames<'q> {
    pub fn new(application: &'q str) -> Self {
        QueueNames {
            application,
            inner: format!("{}_uncommited", application),
        }
    }
    
    pub fn default(&self) -> &str {
        &self.inner[0..self.application.len()]
    }

    pub fn uncommited(&self) -> &str {
        &self.inner[..]
    }
}

impl NameUtils {
    pub fn application(application : &str) -> QueueNames {
        QueueNames::new(application)
    }
}
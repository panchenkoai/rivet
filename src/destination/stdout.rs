use std::io::Write;
use std::path::Path;

use crate::error::Result;

pub struct StdoutDestination;

impl StdoutDestination {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}

impl super::Destination for StdoutDestination {
    fn write(&self, local_path: &Path, _remote_key: &str) -> Result<()> {
        let mut src = std::fs::File::open(local_path)?;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        std::io::copy(&mut src, &mut handle)?;
        handle.flush()?;
        Ok(())
    }
}

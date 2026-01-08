use std::fs;
use std::io::Write;
use std::path::Path;

use tempfile::NamedTempFile;

pub fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = NamedTempFile::new_in(parent)?;
    tmp.as_file_mut().write_all(data)?;
    tmp.as_file_mut().sync_all()?;

    let (_file, tmp_path) = tmp
        .keep()
        .map_err(|e| std::io::Error::other(format!("keep temporary file: {e}")))?;
    fs::rename(&tmp_path, path)?;

    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        dir.sync_all()?;
    }
    let final_file = fs::File::open(path)?;
    final_file.sync_all()?;

    Ok(())
}

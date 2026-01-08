use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn extract_idx_from_path(path: &Path, prefix: &str, suffix: &str) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    extract_idx_from_name(file_name, prefix, suffix)
}

pub(crate) fn collect_indexed_files(
    dir: &Path,
    prefix: &str,
    suffix: &str,
) -> std::io::Result<Vec<(PathBuf, u64)>> {
    let matching = collect_matching_files(dir, prefix, suffix)?;
    let mut entries = Vec::new();
    for path in matching {
        if let Some(idx) = extract_idx_from_path(&path, prefix, suffix) {
            entries.push((path, idx));
        }
    }
    entries.sort_by_key(|(_, idx)| *idx);
    Ok(entries)
}

pub(crate) fn collect_matching_files(
    dir: &Path,
    prefix: &str,
    suffix: &str,
) -> std::io::Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if is_matching_name(&path, prefix, suffix) {
            entries.push(path);
        }
    }
    Ok(entries)
}

fn is_matching_name(path: &Path, prefix: &str, suffix: &str) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|name| name.starts_with(prefix) && name.ends_with(suffix))
        .unwrap_or(false)
}

pub(crate) fn extract_idx_from_name(file_name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    let rest = file_name.strip_suffix(suffix)?.strip_prefix(prefix)?;
    let id_part = rest.split_once('_').map(|(id, _)| id).unwrap_or(rest);
    parse_trailing_index(id_part)
}

pub(crate) fn parse_trailing_index(s: &str) -> Option<u64> {
    // Snapshot/log archive names embed the index as the last '-' separated segment.
    s.rsplit('-')
        .find(|seg| !seg.is_empty())
        .and_then(|seg| seg.parse::<u64>().ok())
}

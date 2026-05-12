use crate::simple::{SourcePage, normalize_project_name};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const PROJECT_MARKER: &str = ".devpi-rs-project";
const PROJECT_METADATA: &str = ".devpi-rs-metadata.json";
const PROJECT_TOXRESULTS: &str = ".devpi-rs-toxresults.json";
const PROJECT_VERSIONS: &str = ".devpi-rs-versions.json";
const PROJECT_DELETED: &str = ".devpi-rs-deleted.json";
const DELETED_FILES: &str = ".devpi-rs-deleted-files.json";

#[derive(Debug, Clone)]
pub struct LocalStore {
    package_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    pub user: String,
    pub index: String,
    pub project: String,
    pub filename: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFile {
    pub filename: String,
    pub bytes: u64,
}

#[derive(Debug, Clone)]
pub struct LocalReadFile {
    pub bytes: Vec<u8>,
    pub modified: Option<SystemTime>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMetadata {
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub log: Vec<FileLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileLogEntry {
    pub what: String,
    pub who: Option<String>,
    pub when: [u64; 6],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}

impl LocalStore {
    pub fn new(package_dir: PathBuf) -> Self {
        Self { package_dir }
    }

    pub fn save(&self, project: &str, filename: &str, bytes: &[u8]) -> io::Result<StoredFile> {
        self.save_in("root", "pypi", project, filename, bytes)
    }

    pub fn save_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
        bytes: &[u8],
    ) -> io::Result<StoredFile> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;

        let dir = self.project_dir_in(user, index, &project);
        fs::create_dir_all(&dir)?;
        let path = dir.join(filename);
        if path.is_file() {
            self.remove_hash_sidecar_for_path(user, index, filename, &path)?;
        }
        fs::write(dir.join(filename), bytes)?;
        self.write_hash_sidecar_in(user, index, filename, bytes)?;
        let mut deleted = self.project_deleted_files_from_dir(&dir)?;
        if deleted.remove(filename).is_some() {
            self.write_project_deleted_files(&dir, &deleted)?;
        }
        let mut globally_deleted = self.deleted_files()?;
        if globally_deleted
            .remove(&deleted_file_key(user, index, &project, filename))
            .is_some()
        {
            self.write_deleted_files(&globally_deleted)?;
        }

        Ok(StoredFile {
            user: user.to_string(),
            index: index.to_string(),
            project,
            filename: filename.to_string(),
            bytes: bytes.len() as u64,
        })
    }

    pub fn create_project_in(&self, user: &str, index: &str, project: &str) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;

        let dir = self.project_dir_in(user, index, &project);
        let marker = dir.join(PROJECT_MARKER);
        let created = !marker.exists()
            && self
                .list_project_files_in(user, index, &project)?
                .is_empty();
        fs::create_dir_all(&dir)?;
        fs::write(marker, b"registered\n")?;
        Ok(created)
    }

    pub fn project_exists_in(&self, user: &str, index: &str, project: &str) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;

        let dir = self.project_dir_in(user, index, &project);
        if !dir.exists() {
            return Ok(false);
        }
        Ok(dir.join(PROJECT_MARKER).exists()
            || !self
                .list_project_files_in(user, index, &project)?
                .is_empty())
    }

    pub fn read(&self, project: &str, filename: &str) -> io::Result<Vec<u8>> {
        self.read_in("root", "pypi", project, filename)
    }

    pub fn read_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
    ) -> io::Result<Vec<u8>> {
        self.read_file_in(user, index, project, filename)
            .map(|file| file.bytes)
    }

    pub fn read_file_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
    ) -> io::Result<LocalReadFile> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;
        read_local_file(&self.project_dir_in(user, index, &project).join(filename))
    }

    pub fn file_deleted_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
    ) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;
        if self
            .project_deleted_files_from_dir(&self.project_dir_in(user, index, &project))?
            .contains_key(filename)
        {
            return Ok(true);
        }
        Ok(self
            .deleted_files()?
            .contains_key(&deleted_file_key(user, index, &project, filename)))
    }

    pub fn tombstone_stage_files(&self, user: &str, index: &str) -> io::Result<()> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let stage_dir = self.stage_dir(user, index);
        if !stage_dir.exists() {
            return Ok(());
        }
        let mut deleted = self.deleted_files()?;
        for entry in fs::read_dir(&stage_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let project = entry.file_name().to_string_lossy().to_string();
            validate_project(&project)?;
            for filename in self.list_project_files_in(user, index, &project)? {
                self.remove_hash_sidecar_for_path(
                    user,
                    index,
                    &filename,
                    &entry.path().join(&filename),
                )?;
                deleted.insert(deleted_file_key(user, index, &project, &filename), true);
            }
        }
        self.write_deleted_files(&deleted)
    }

    pub fn tombstone_user_files(&self, user: &str) -> io::Result<()> {
        validate_stage_segment(user)?;
        let user_dir = self.package_dir.join(user);
        if !user_dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(user_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let index = entry.file_name().to_string_lossy().to_string();
                self.tombstone_stage_files(user, &index)?;
            }
        }
        Ok(())
    }

    pub fn read_hash_path_in(
        &self,
        user: &str,
        index: &str,
        hash_prefix: &str,
        hash_rest: &str,
        filename: &str,
    ) -> io::Result<Option<Vec<u8>>> {
        self.read_hash_path_file_in(user, index, hash_prefix, hash_rest, filename)
            .map(|file| file.map(|file| file.bytes))
    }

    pub fn read_hash_path_file_in(
        &self,
        user: &str,
        index: &str,
        hash_prefix: &str,
        hash_rest: &str,
        filename: &str,
    ) -> io::Result<Option<LocalReadFile>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        validate_hash_path_segment(hash_prefix)?;
        validate_hash_path_segment(hash_rest)?;
        if hash_prefix.len() != 3 || hash_rest.len() != 13 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid hash path segment",
            ));
        }
        validate_filename(filename)?;
        let expected_prefix = format!("{hash_prefix}{hash_rest}").to_ascii_lowercase();
        let sidecar_path = self.hash_sidecar_path(user, index, &expected_prefix, filename);
        if sidecar_path.is_file() {
            let file = read_local_file(&sidecar_path)?;
            if sha256_hex(&file.bytes).starts_with(&expected_prefix) {
                return Ok(Some(file));
            }
            return Ok(None);
        }
        let stage_dir = self.stage_dir(user, index);
        if !stage_dir.exists() {
            return Ok(None);
        }
        for entry in fs::read_dir(stage_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path().join(filename);
            if !path.is_file() {
                continue;
            }
            let file = read_local_file(&path)?;
            if sha256_hex(&file.bytes).starts_with(&expected_prefix) {
                return Ok(Some(file));
            }
        }
        Ok(None)
    }

    pub fn file_exists_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
    ) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;
        Ok(self
            .project_dir_in(user, index, &project)
            .join(filename)
            .is_file())
    }

    pub fn save_file_metadata_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
        fields: BTreeMap<String, String>,
    ) -> io::Result<()> {
        self.save_file_metadata_with_log_in(user, index, project, filename, fields, Vec::new())
    }

    pub fn save_file_metadata_with_log_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
        fields: BTreeMap<String, String>,
        log: Vec<FileLogEntry>,
    ) -> io::Result<()> {
        if fields.is_empty() && log.is_empty() {
            return Ok(());
        }
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;

        let dir = self.project_dir_in(user, index, &project);
        fs::create_dir_all(&dir)?;
        let mut metadata = self.project_file_metadata_from_dir(&dir)?;
        metadata.insert(filename.to_string(), FileMetadata { fields, log });
        self.write_project_file_metadata(&dir, &metadata)
    }

    pub fn project_file_metadata_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
    ) -> io::Result<BTreeMap<String, FileMetadata>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        self.project_file_metadata_from_dir(&self.project_dir_in(user, index, &project))
    }

    pub fn save_version_metadata_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        version: &str,
        fields: BTreeMap<String, String>,
    ) -> io::Result<()> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_version(version)?;

        let dir = self.project_dir_in(user, index, &project);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join(PROJECT_MARKER), b"registered\n")?;
        let mut metadata = self.project_version_metadata_from_dir(&dir)?;
        metadata.insert(
            version.to_string(),
            FileMetadata {
                fields,
                log: Vec::new(),
            },
        );
        self.write_project_version_metadata(&dir, &metadata)
    }

    pub fn project_version_metadata_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
    ) -> io::Result<BTreeMap<String, FileMetadata>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        self.project_version_metadata_from_dir(&self.project_dir_in(user, index, &project))
    }

    pub fn store_tox_result_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
        result: serde_json::Value,
    ) -> io::Result<usize> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;

        let dir = self.project_dir_in(user, index, &project);
        if !dir.join(filename).is_file() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        }
        let mut tox_results = self.project_tox_results_from_dir(&dir)?;
        let results = tox_results.entry(filename.to_string()).or_default();
        results.push(result);
        let index = results.len() - 1;
        self.write_project_tox_results(&dir, &tox_results)?;
        Ok(index)
    }

    pub fn project_tox_results_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
    ) -> io::Result<BTreeMap<String, Vec<serde_json::Value>>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        self.project_tox_results_from_dir(&self.project_dir_in(user, index, &project))
    }

    pub fn tox_result_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
        result_index: usize,
    ) -> io::Result<serde_json::Value> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;
        let tox_results =
            self.project_tox_results_from_dir(&self.project_dir_in(user, index, &project))?;
        tox_results
            .get(filename)
            .and_then(|results| results.get(result_index))
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "tox result not found"))
    }

    pub fn delete_tox_result_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
        result_index: usize,
    ) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;

        let dir = self.project_dir_in(user, index, &project);
        let mut tox_results = self.project_tox_results_from_dir(&dir)?;
        let Some(results) = tox_results.get_mut(filename) else {
            return Ok(false);
        };
        if result_index >= results.len() {
            return Ok(false);
        }
        results.remove(result_index);
        if results.is_empty() {
            tox_results.remove(filename);
        }
        self.write_project_tox_results(&dir, &tox_results)?;
        Ok(true)
    }

    pub fn delete_project_in(&self, user: &str, index: &str, project: &str) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;

        let dir = self.project_dir_in(user, index, &project);
        if !dir.exists() {
            return Ok(false);
        }
        for filename in self.list_project_files_in(user, index, &project)? {
            self.remove_hash_sidecar_for_path(user, index, &filename, &dir.join(&filename))?;
        }
        fs::remove_dir_all(dir)?;
        Ok(true)
    }

    pub fn delete_version_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        version: &str,
    ) -> io::Result<usize> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        validate_version(version)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;

        let dir = self.project_dir_in(user, index, &project);
        if !dir.exists() {
            return Ok(0);
        }

        let mut deleted = 0;
        let mut deleted_files = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let filename = entry.file_name().to_string_lossy().to_string();
            if !is_internal_project_file(&filename) && filename.contains(version) {
                self.remove_hash_sidecar_for_path(user, index, &filename, &entry.path())?;
                fs::remove_file(entry.path())?;
                deleted_files.push(filename);
                deleted += 1;
            }
        }

        if !deleted_files.is_empty() {
            let mut metadata = self.project_file_metadata_from_dir(&dir)?;
            let mut tox_results = self.project_tox_results_from_dir(&dir)?;
            let mut deleted_file_markers = self.project_deleted_files_from_dir(&dir)?;
            for filename in deleted_files {
                metadata.remove(&filename);
                tox_results.remove(&filename);
                deleted_file_markers.insert(filename, true);
            }
            self.write_project_file_metadata(&dir, &metadata)?;
            self.write_project_tox_results(&dir, &tox_results)?;
            self.write_project_deleted_files(&dir, &deleted_file_markers)?;
        }
        let mut version_metadata = self.project_version_metadata_from_dir(&dir)?;
        let removed_version_metadata = version_metadata.remove(version).is_some();
        if removed_version_metadata {
            self.write_project_version_metadata(&dir, &version_metadata)?;
            if deleted == 0 {
                deleted = 1;
            }
        }

        if deleted > 0
            && self
                .list_project_files_in(user, index, &project)?
                .is_empty()
            && !dir.join(PROJECT_MARKER).exists()
            && !self.project_has_deleted_files(&dir)?
        {
            fs::remove_dir(&dir)?;
        }

        Ok(deleted)
    }

    pub fn delete_file_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
        filename: &str,
    ) -> io::Result<bool> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        validate_filename(filename)?;

        let dir = self.project_dir_in(user, index, &project);
        let path = dir.join(filename);
        if !path.exists() {
            return Ok(false);
        }
        self.remove_hash_sidecar_for_path(user, index, filename, &path)?;
        fs::remove_file(path)?;

        let mut metadata = self.project_file_metadata_from_dir(&dir)?;
        let mut tox_results = self.project_tox_results_from_dir(&dir)?;
        let mut deleted = self.project_deleted_files_from_dir(&dir)?;
        metadata.remove(filename);
        tox_results.remove(filename);
        deleted.insert(filename.to_string(), true);
        self.write_project_file_metadata(&dir, &metadata)?;
        self.write_project_tox_results(&dir, &tox_results)?;
        self.write_project_deleted_files(&dir, &deleted)?;
        if self
            .list_project_files_in(user, index, &project)?
            .is_empty()
            && !dir.join(PROJECT_MARKER).exists()
            && !self.project_has_deleted_files(&dir)?
        {
            fs::remove_dir(&dir)?;
        }
        Ok(true)
    }

    pub fn project_page(&self, project: &str) -> io::Result<Option<SourcePage>> {
        self.project_page_with_prefix("root", "pypi", project, "/files")
    }

    pub fn project_page_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
    ) -> io::Result<Option<SourcePage>> {
        self.project_page_with_prefix(user, index, project, "../../+f")
    }

    fn project_page_with_prefix(
        &self,
        user: &str,
        index: &str,
        project: &str,
        link_prefix: &str,
    ) -> io::Result<Option<SourcePage>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        let files = self.list_project_files_in(user, index, &project)?;
        if files.is_empty() {
            return Ok(None);
        }
        let dir = self.project_dir_in(user, index, &project);
        let metadata = self.project_file_metadata_from_dir(&dir)?;

        let mut body = String::new();
        body.push_str("<!doctype html>\n<html><body>\n");
        for file in files {
            let hash = file_sha256_hex(&dir.join(&file))?;
            body.push_str("<a href=\"");
            body.push_str(&escape_attr(link_prefix));
            body.push('/');
            body.push_str(&escape_attr(&project));
            body.push('/');
            body.push_str(&escape_attr(&file));
            body.push_str("#sha256=");
            body.push_str(&hash);
            if let Some(metadata) = metadata.get(&file) {
                append_simple_link_metadata(&mut body, metadata);
            }
            body.push_str("\">");
            body.push_str(&escape_html(&file));
            body.push_str("</a><br>\n");
        }
        body.push_str("</body></html>\n");

        Ok(Some(SourcePage {
            source: "local".to_string(),
            body,
            page_url: None,
            pypi_last_serial: None,
        }))
    }

    pub fn root_page(&self) -> io::Result<Option<SourcePage>> {
        self.root_page_in("root", "pypi")
    }

    pub fn root_page_in(&self, user: &str, index: &str) -> io::Result<Option<SourcePage>> {
        let projects = self.projects_in(user, index)?;
        if projects.is_empty() {
            return Ok(None);
        }

        let mut body = String::new();
        body.push_str("<!doctype html>\n<html><body>\n");
        for project in projects {
            body.push_str("<a href=\"");
            body.push_str(&escape_attr(&project));
            body.push_str("/\">");
            body.push_str(&escape_html(&project));
            body.push_str("</a><br>\n");
        }
        body.push_str("</body></html>\n");

        Ok(Some(SourcePage {
            source: "local".to_string(),
            body,
            page_url: None,
            pypi_last_serial: None,
        }))
    }

    pub fn projects_in(&self, user: &str, index: &str) -> io::Result<Vec<String>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let mut projects = Vec::new();
        let stage_dir = self.stage_dir(user, index);
        if !stage_dir.exists() {
            return Ok(projects);
        }
        for entry in fs::read_dir(&stage_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if self.project_exists_in(user, index, &name)? {
                projects.push(name);
            }
        }
        projects.sort();
        Ok(projects)
    }

    pub fn files_in(&self, user: &str, index: &str, project: &str) -> io::Result<Vec<LocalFile>> {
        validate_stage_segment(user)?;
        validate_stage_segment(index)?;
        let project = normalize_project_name(project);
        validate_project(&project)?;
        let dir = self.project_dir_in(user, index, &project);
        let mut files = Vec::new();
        if !dir.exists() {
            return Ok(files);
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let filename = entry.file_name().to_string_lossy().to_string();
            if entry.file_type()?.is_file() && !is_internal_project_file(&filename) {
                files.push(LocalFile {
                    filename,
                    bytes: entry.metadata()?.len(),
                });
            }
        }
        files.sort_by(|left, right| left.filename.cmp(&right.filename));
        Ok(files)
    }

    fn list_project_files_in(
        &self,
        user: &str,
        index: &str,
        project: &str,
    ) -> io::Result<Vec<String>> {
        let dir = self.project_dir_in(user, index, project);
        let mut files = Vec::new();
        if !dir.exists() {
            return Ok(files);
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let filename = entry.file_name().to_string_lossy().to_string();
            if entry.file_type()?.is_file() && !is_internal_project_file(&filename) {
                files.push(filename);
            }
        }
        files.sort();
        Ok(files)
    }

    fn project_dir_in(&self, user: &str, index: &str, project: &str) -> PathBuf {
        self.stage_dir(user, index).join(project)
    }

    fn stage_dir(&self, user: &str, index: &str) -> PathBuf {
        self.package_dir.join(user).join(index)
    }

    fn write_hash_sidecar_in(
        &self,
        user: &str,
        index: &str,
        filename: &str,
        bytes: &[u8],
    ) -> io::Result<()> {
        let hash = sha256_hex(bytes);
        let path = self.hash_sidecar_path(user, index, &hash, filename);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, bytes)
    }

    fn remove_hash_sidecar_for_path(
        &self,
        user: &str,
        index: &str,
        filename: &str,
        path: &Path,
    ) -> io::Result<()> {
        if !path.is_file() {
            return Ok(());
        }
        let hash = file_sha256_hex(path)?;
        match fs::remove_file(self.hash_sidecar_path(user, index, &hash, filename)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn hash_sidecar_path(&self, user: &str, index: &str, hash: &str, filename: &str) -> PathBuf {
        self.package_dir
            .join("+files")
            .join(user)
            .join(index)
            .join("+f")
            .join(&hash[..3])
            .join(&hash[3..16])
            .join(filename)
    }

    fn project_file_metadata_from_dir(
        &self,
        dir: &Path,
    ) -> io::Result<BTreeMap<String, FileMetadata>> {
        let path = dir.join(PROJECT_METADATA);
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let text = fs::read_to_string(&path)?;
        serde_json::from_str(&text).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid project metadata {}: {err}", path.display()),
            )
        })
    }

    fn write_project_file_metadata(
        &self,
        dir: &Path,
        metadata: &BTreeMap<String, FileMetadata>,
    ) -> io::Result<()> {
        let path = dir.join(PROJECT_METADATA);
        if metadata.is_empty() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            return Ok(());
        }
        let text = serde_json::to_string_pretty(metadata).map_err(io::Error::other)?;
        fs::write(path, format!("{text}\n"))
    }

    fn project_tox_results_from_dir(
        &self,
        dir: &Path,
    ) -> io::Result<BTreeMap<String, Vec<serde_json::Value>>> {
        let path = dir.join(PROJECT_TOXRESULTS);
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let text = fs::read_to_string(&path)?;
        serde_json::from_str(&text).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid tox results {}: {err}", path.display()),
            )
        })
    }

    fn project_version_metadata_from_dir(
        &self,
        dir: &Path,
    ) -> io::Result<BTreeMap<String, FileMetadata>> {
        let path = dir.join(PROJECT_VERSIONS);
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let text = fs::read_to_string(&path)?;
        serde_json::from_str(&text).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid version metadata {}: {err}", path.display()),
            )
        })
    }

    fn project_deleted_files_from_dir(&self, dir: &Path) -> io::Result<BTreeMap<String, bool>> {
        let path = dir.join(PROJECT_DELETED);
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let text = fs::read_to_string(&path)?;
        serde_json::from_str(&text).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid deleted file metadata {}: {err}", path.display()),
            )
        })
    }

    fn project_has_deleted_files(&self, dir: &Path) -> io::Result<bool> {
        Ok(!self.project_deleted_files_from_dir(dir)?.is_empty())
    }

    fn deleted_files(&self) -> io::Result<BTreeMap<String, bool>> {
        let path = self.package_dir.join(DELETED_FILES);
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let text = fs::read_to_string(&path)?;
        serde_json::from_str(&text).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid deleted file metadata {}: {err}", path.display()),
            )
        })
    }

    fn write_project_version_metadata(
        &self,
        dir: &Path,
        metadata: &BTreeMap<String, FileMetadata>,
    ) -> io::Result<()> {
        let path = dir.join(PROJECT_VERSIONS);
        if metadata.is_empty() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            return Ok(());
        }
        let text = serde_json::to_string_pretty(metadata).map_err(io::Error::other)?;
        fs::write(path, format!("{text}\n"))
    }

    fn write_project_tox_results(
        &self,
        dir: &Path,
        tox_results: &BTreeMap<String, Vec<serde_json::Value>>,
    ) -> io::Result<()> {
        let path = dir.join(PROJECT_TOXRESULTS);
        if tox_results.is_empty() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            return Ok(());
        }
        let text = serde_json::to_string_pretty(tox_results).map_err(io::Error::other)?;
        fs::write(path, format!("{text}\n"))
    }

    fn write_project_deleted_files(
        &self,
        dir: &Path,
        deleted: &BTreeMap<String, bool>,
    ) -> io::Result<()> {
        let path = dir.join(PROJECT_DELETED);
        if deleted.is_empty() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            return Ok(());
        }
        fs::create_dir_all(dir)?;
        let text = serde_json::to_string_pretty(deleted).map_err(io::Error::other)?;
        fs::write(path, format!("{text}\n"))
    }

    fn write_deleted_files(&self, deleted: &BTreeMap<String, bool>) -> io::Result<()> {
        let path = self.package_dir.join(DELETED_FILES);
        if deleted.is_empty() {
            if path.exists() {
                fs::remove_file(path)?;
            }
            return Ok(());
        }
        fs::create_dir_all(&self.package_dir)?;
        let text = serde_json::to_string_pretty(deleted).map_err(io::Error::other)?;
        fs::write(path, format!("{text}\n"))
    }
}

fn deleted_file_key(user: &str, index: &str, project: &str, filename: &str) -> String {
    format!("{user}/{index}/{project}/{filename}")
}

fn is_internal_project_file(filename: &str) -> bool {
    filename == PROJECT_MARKER
        || filename == PROJECT_METADATA
        || filename == PROJECT_TOXRESULTS
        || filename == PROJECT_VERSIONS
        || filename == PROJECT_DELETED
}

fn read_local_file(path: &Path) -> io::Result<LocalReadFile> {
    let bytes = fs::read(path)?;
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok();
    Ok(LocalReadFile { bytes, modified })
}

fn validate_stage_segment(value: &str) -> io::Result<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains('+')
        || value == "."
        || value == ".."
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid stage segment",
        ));
    }
    Ok(())
}

fn validate_project(project: &str) -> io::Result<()> {
    if project.is_empty() || project.contains('/') || project.contains('\\') || project == ".." {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid project name",
        ));
    }
    Ok(())
}

fn validate_version(version: &str) -> io::Result<()> {
    if version.is_empty()
        || version.contains('/')
        || version.contains('\\')
        || version == "."
        || version == ".."
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid version",
        ));
    }
    Ok(())
}

fn validate_filename(filename: &str) -> io::Result<()> {
    let path = Path::new(filename);
    if filename.is_empty()
        || filename.contains('/')
        || filename.contains('\\')
        || filename == "."
        || filename == ".."
        || path.file_name().and_then(|name| name.to_str()) != Some(filename)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid filename",
        ));
    }
    Ok(())
}

fn validate_hash_path_segment(value: &str) -> io::Result<()> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid hash path segment",
        ));
    }
    Ok(())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_attr(value: &str) -> String {
    escape_html(value).replace('"', "&quot;")
}

fn append_simple_link_metadata(body: &mut String, metadata: &FileMetadata) {
    if let Some(value) = metadata_value(&metadata.fields, &["requires_python", "requires-python"]) {
        body.push_str("\" data-requires-python=\"");
        body.push_str(&escape_attr(value));
    }
    if let Some(value) = metadata_value(&metadata.fields, &["yanked", "data-yanked"]) {
        body.push_str("\" data-yanked=\"");
        body.push_str(&escape_attr(value));
    }
    if let Some(value) = metadata_value(
        &metadata.fields,
        &[
            "dist_info_metadata",
            "dist-info-metadata",
            "data-dist-info-metadata",
        ],
    ) {
        body.push_str("\" data-dist-info-metadata=\"");
        body.push_str(&escape_attr(value));
    }
    if let Some(value) = metadata_value(
        &metadata.fields,
        &["core_metadata", "core-metadata", "data-core-metadata"],
    ) {
        body.push_str("\" data-core-metadata=\"");
        body.push_str(&escape_attr(value));
    }
    if let Some(value) = metadata_value(&metadata.fields, &["gpg_sig", "gpg-sig", "data-gpg-sig"]) {
        body.push_str("\" data-gpg-sig=\"");
        body.push_str(&escape_attr(value));
    }
}

fn metadata_value<'a>(fields: &'a BTreeMap<String, String>, keys: &[&str]) -> Option<&'a String> {
    keys.iter().find_map(|key| fields.get(*key))
}

pub(crate) fn file_sha256_hex(path: &Path) -> io::Result<String> {
    Ok(sha256_hex(&fs::read(path)?))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(name: &str) -> LocalStore {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-local-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        LocalStore::new(dir)
    }

    #[test]
    fn saves_and_reads_package_file() {
        let store = store("read");

        let saved = store
            .save("Demo_Pkg", "demo_pkg-1.0.0-py3-none-any.whl", b"wheel")
            .unwrap();

        assert_eq!(saved.project, "demo-pkg");
        assert_eq!(saved.user, "root");
        assert_eq!(saved.index, "pypi");
        assert_eq!(
            store
                .read("demo-pkg", "demo_pkg-1.0.0-py3-none-any.whl")
                .unwrap(),
            b"wheel"
        );
    }

    #[test]
    fn mirrors_stage_files_to_hash_sidecar_paths() {
        let store = store("hash-sidecar");
        let old_hash = sha256_hex(b"old");
        let new_hash = sha256_hex(b"new");

        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"old")
            .unwrap();
        assert_eq!(
            store
                .read_hash_path_in(
                    "alice",
                    "dev",
                    &old_hash[..3],
                    &old_hash[3..16],
                    "demo-1.0.0.tar.gz"
                )
                .unwrap()
                .unwrap(),
            b"old"
        );

        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"new")
            .unwrap();
        assert!(
            store
                .read_hash_path_in(
                    "alice",
                    "dev",
                    &old_hash[..3],
                    &old_hash[3..16],
                    "demo-1.0.0.tar.gz"
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .read_hash_path_in(
                    "alice",
                    "dev",
                    &new_hash[..3],
                    &new_hash[3..16],
                    "demo-1.0.0.tar.gz"
                )
                .unwrap()
                .unwrap(),
            b"new"
        );

        assert!(
            store
                .delete_file_in("alice", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap()
        );
        assert!(
            store
                .read_hash_path_in(
                    "alice",
                    "dev",
                    &new_hash[..3],
                    &new_hash[3..16],
                    "demo-1.0.0.tar.gz"
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .file_deleted_in("alice", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap()
        );
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"newer")
            .unwrap();
        assert!(
            !store
                .file_deleted_in("alice", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap()
        );
    }

    #[test]
    fn renders_local_project_page() {
        let store = store("page");
        store.save("demo", "demo-1.0.0.tar.gz", b"sdist").unwrap();

        let page = store.project_page("demo").unwrap().unwrap();

        assert_eq!(page.source, "local");
        assert!(page.body.contains("/files/demo/demo-1.0.0.tar.gz"));
        assert!(
            page.body.contains(
                "#sha256=714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
            )
        );
    }

    #[test]
    fn separates_packages_by_stage() {
        let store = store("stage");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"alice")
            .unwrap();
        store
            .save_in("bob", "dev", "demo", "demo-1.0.0.tar.gz", b"bob")
            .unwrap();

        assert_eq!(
            store
                .read_in("alice", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap(),
            b"alice"
        );
        assert_eq!(
            store
                .read_in("bob", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap(),
            b"bob"
        );
    }

    #[test]
    fn renders_stage_specific_file_links() {
        let store = store("stage-links");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let page = store
            .project_page_in("alice", "dev", "demo")
            .unwrap()
            .unwrap();

        assert!(page.body.contains("../../+f/demo/demo-1.0.0.tar.gz"));
        assert!(
            page.body.contains(
                "#sha256=714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
            )
        );
    }

    #[test]
    fn lists_projects_for_stage() {
        let store = store("projects");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        store
            .save_in("alice", "dev", "other", "other-1.0.0.tar.gz", b"sdist")
            .unwrap();
        store
            .save_in("bob", "dev", "hidden", "hidden-1.0.0.tar.gz", b"sdist")
            .unwrap();

        assert_eq!(
            store.projects_in("alice", "dev").unwrap(),
            vec!["demo".to_string(), "other".to_string()]
        );
    }

    #[test]
    fn registers_project_without_files() {
        let store = store("register-project");

        assert!(store.create_project_in("alice", "dev", "Demo_Pkg").unwrap());

        assert_eq!(
            store.projects_in("alice", "dev").unwrap(),
            vec!["demo-pkg".to_string()]
        );
        assert!(store.project_exists_in("alice", "dev", "demo-pkg").unwrap());
        assert_eq!(
            store.files_in("alice", "dev", "demo-pkg").unwrap(),
            Vec::new()
        );
        assert!(
            store
                .project_page_in("alice", "dev", "demo-pkg")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn registering_existing_file_backed_project_is_not_created() {
        let store = store("register-existing-project");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        assert!(!store.create_project_in("alice", "dev", "demo").unwrap());

        assert_eq!(
            store.files_in("alice", "dev", "demo").unwrap(),
            vec![LocalFile {
                filename: "demo-1.0.0.tar.gz".to_string(),
                bytes: 5
            }]
        );
    }

    #[test]
    fn lists_files_for_project() {
        let store = store("files");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        assert_eq!(
            store.files_in("alice", "dev", "demo").unwrap(),
            vec![LocalFile {
                filename: "demo-1.0.0.tar.gz".to_string(),
                bytes: 5
            }]
        );
    }

    #[test]
    fn stores_file_metadata_without_listing_sidecar() {
        let store = store("metadata");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        store
            .save_file_metadata_in(
                "alice",
                "dev",
                "demo",
                "demo-1.0.0.tar.gz",
                BTreeMap::from([
                    ("version".to_string(), "1.0.0".to_string()),
                    ("summary".to_string(), "Demo package".to_string()),
                ]),
            )
            .unwrap();

        assert_eq!(
            store.files_in("alice", "dev", "demo").unwrap(),
            vec![LocalFile {
                filename: "demo-1.0.0.tar.gz".to_string(),
                bytes: 5
            }]
        );
        assert_eq!(
            store
                .project_file_metadata_in("alice", "dev", "demo")
                .unwrap()["demo-1.0.0.tar.gz"]
                .fields["summary"],
            "Demo package"
        );
    }

    #[test]
    fn renders_file_metadata_in_simple_links() {
        let store = store("simple-link-metadata");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        store
            .save_file_metadata_in(
                "alice",
                "dev",
                "demo",
                "demo-1.0.0.tar.gz",
                BTreeMap::from([
                    ("requires_python".to_string(), ">=3.9".to_string()),
                    ("yanked".to_string(), "bad build".to_string()),
                    ("dist_info_metadata".to_string(), "true".to_string()),
                    ("core_metadata".to_string(), "sha256=meta123".to_string()),
                    ("gpg_sig".to_string(), "true".to_string()),
                ]),
            )
            .unwrap();

        let page = store
            .project_page_in("alice", "dev", "demo")
            .unwrap()
            .unwrap();

        assert!(page.body.contains(r#"data-requires-python="&gt;=3.9""#));
        assert!(page.body.contains(r#"data-yanked="bad build""#));
        assert!(page.body.contains(r#"data-dist-info-metadata="true""#));
        assert!(page.body.contains(r#"data-core-metadata="sha256=meta123""#));
        assert!(page.body.contains(r#"data-gpg-sig="true""#));
    }

    #[test]
    fn stores_version_metadata_without_listing_sidecar() {
        let store = store("version-metadata");
        store
            .save_version_metadata_in(
                "alice",
                "dev",
                "demo",
                "1.0.0",
                BTreeMap::from([
                    ("name".to_string(), "demo".to_string()),
                    ("version".to_string(), "1.0.0".to_string()),
                    ("summary".to_string(), "Demo release".to_string()),
                ]),
            )
            .unwrap();

        assert!(store.project_exists_in("alice", "dev", "demo").unwrap());
        assert!(store.files_in("alice", "dev", "demo").unwrap().is_empty());
        assert_eq!(
            store
                .project_version_metadata_in("alice", "dev", "demo")
                .unwrap()["1.0.0"]
                .fields["summary"],
            "Demo release"
        );
        assert_eq!(
            store
                .delete_version_in("alice", "dev", "demo", "1.0.0")
                .unwrap(),
            1
        );
        assert!(
            store
                .project_version_metadata_in("alice", "dev", "demo")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn stores_tox_results_without_listing_sidecar() {
        let store = store("toxresults");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let index = store
            .store_tox_result_in(
                "alice",
                "dev",
                "demo",
                "demo-1.0.0.tar.gz",
                serde_json::json!({"envname":"py312","retcode":0}),
            )
            .unwrap();

        assert_eq!(index, 0);
        assert_eq!(
            store.files_in("alice", "dev", "demo").unwrap(),
            vec![LocalFile {
                filename: "demo-1.0.0.tar.gz".to_string(),
                bytes: 5
            }]
        );
        assert_eq!(
            store
                .project_tox_results_in("alice", "dev", "demo")
                .unwrap()["demo-1.0.0.tar.gz"][0]["envname"],
            "py312"
        );
        assert_eq!(
            store
                .tox_result_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", 0)
                .unwrap()["retcode"],
            0
        );
        assert_eq!(
            store
                .tox_result_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert!(
            store
                .delete_tox_result_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", 0)
                .unwrap()
        );
        assert!(
            !store
                .delete_tox_result_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", 0)
                .unwrap()
        );
        assert!(
            store
                .project_tox_results_in("alice", "dev", "demo")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn checks_file_existence() {
        let store = store("exists");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        assert!(
            store
                .file_exists_in("alice", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap()
        );
        assert!(
            !store
                .file_exists_in("alice", "dev", "demo", "demo-2.0.0.tar.gz")
                .unwrap()
        );
    }

    #[test]
    fn rejects_path_traversal_filename() {
        let store = store("rejects");

        let err = store.save("demo", "../bad.whl", b"bad").unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn deletes_only_files_matching_version() {
        let store = store("delete-version");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        store
            .save_in("alice", "dev", "demo", "demo-2.0.0.tar.gz", b"sdist")
            .unwrap();

        assert_eq!(
            store
                .delete_version_in("alice", "dev", "demo", "1.0.0")
                .unwrap(),
            1
        );

        assert_eq!(
            store.files_in("alice", "dev", "demo").unwrap(),
            vec![LocalFile {
                filename: "demo-2.0.0.tar.gz".to_string(),
                bytes: 5
            }]
        );
    }

    #[test]
    fn deletes_empty_project_after_last_version() {
        let store = store("delete-last-version");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        assert_eq!(
            store
                .delete_version_in("alice", "dev", "demo", "1.0.0")
                .unwrap(),
            1
        );

        assert_eq!(
            store.projects_in("alice", "dev").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn deletes_project_directory() {
        let store = store("delete-project");
        store
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        assert!(store.delete_project_in("alice", "dev", "demo").unwrap());

        assert_eq!(store.files_in("alice", "dev", "demo").unwrap(), Vec::new());
        assert_eq!(
            store.projects_in("alice", "dev").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn reports_missing_deletes() {
        let store = store("delete-missing");

        assert!(!store.delete_project_in("alice", "dev", "demo").unwrap());
        assert_eq!(
            store
                .delete_version_in("alice", "dev", "demo", "1.0.0")
                .unwrap(),
            0
        );
    }
}

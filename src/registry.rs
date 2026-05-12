use crate::local::sha256_hex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub const LOGIN_EXPIRATION_SECS: u64 = 10 * 60 * 60;

const TOKEN_PREFIX: &str = "devpi-rs-token-v1";
const PASSWORD_HASH_PREFIX: &str = "devpi-rs-password-v1";
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone)]
pub struct Registry {
    package_dir: PathBuf,
    path: PathBuf,
    auth_secret_path: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryData {
    pub users: BTreeMap<String, UserConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserConfig {
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default)]
    pub indexes: BTreeMap<String, IndexConfig>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PublicUserConfig {
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default)]
    pub indexes: BTreeMap<String, IndexConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexConfig {
    #[serde(rename = "type")]
    pub index_type: String,
    #[serde(default)]
    pub bases: Vec<String>,
    pub volatile: bool,
    #[serde(default)]
    pub acl_upload: Vec<String>,
    #[serde(default = "default_pkg_read_acl")]
    pub acl_pkg_read: Vec<String>,
    #[serde(default = "default_toxresult_acl")]
    pub acl_toxresult_upload: Vec<String>,
    #[serde(default)]
    pub mirror_whitelist: Vec<String>,
    #[serde(default = "default_mirror_whitelist_inheritance")]
    pub mirror_whitelist_inheritance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror_web_url_fmt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror_cache_expiry: Option<i64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mirror_ignore_serial_header: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mirror_no_project_list: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mirror_provides_core_metadata: bool,
    #[serde(default)]
    pub mirror_use_external_urls: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    #[serde(default, flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct UserInput {
    pub email: Option<String>,
    #[allow(dead_code)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct IndexInput {
    pub bases: Option<Vec<String>>,
    pub volatile: Option<bool>,
    #[serde(default, rename = "type")]
    pub index_type: Option<String>,
    pub acl_upload: Option<Vec<String>>,
    pub acl_pkg_read: Option<Vec<String>>,
    pub acl_toxresult_upload: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_optional_whitelist")]
    pub mirror_whitelist: Option<Vec<String>>,
    pub mirror_whitelist_inheritance: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub custom_data: Option<Value>,
    pub mirror_url: Option<String>,
    pub mirror_web_url_fmt: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_i64")]
    pub mirror_cache_expiry: Option<i64>,
    pub mirror_ignore_serial_header: Option<bool>,
    pub mirror_no_project_list: Option<bool>,
    pub mirror_provides_core_metadata: Option<bool>,
    pub mirror_use_external_urls: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_whitelist")]
    pub pypi_whitelist: Option<Vec<String>>,
    pub sources: Option<Vec<String>>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
    #[serde(skip)]
    pub clear_fields: Vec<String>,
}

impl Registry {
    pub fn new(package_dir: PathBuf) -> Self {
        Self {
            path: package_dir.join(".devpi-rs-users.json"),
            auth_secret_path: package_dir.join(".devpi-rs-auth-secret"),
            package_dir,
        }
    }

    pub fn users(&self) -> io::Result<BTreeMap<String, UserConfig>> {
        Ok(self.load()?.users)
    }

    pub fn public_users(&self) -> io::Result<BTreeMap<String, PublicUserConfig>> {
        Ok(self
            .load()?
            .users
            .into_iter()
            .map(|(name, config)| (name, config.into()))
            .collect())
    }

    pub fn user(&self, username: &str) -> io::Result<Option<UserConfig>> {
        validate_name(username)?;
        Ok(self.load()?.users.remove(username))
    }

    pub fn public_user(&self, username: &str) -> io::Result<Option<PublicUserConfig>> {
        Ok(self.user(username)?.map(Into::into))
    }

    pub fn verify_password(&self, username: &str, password: &str) -> io::Result<bool> {
        validate_name(username)?;
        let data = self.load()?;
        let Some(user) = data.users.get(username) else {
            return Ok(false);
        };
        if user_accepts_password(username, user, password) {
            return Ok(true);
        }
        if !password.starts_with(TOKEN_PREFIX) {
            return Ok(false);
        }
        let Some(stored_password) = stored_password_for_signature(username, user) else {
            return Ok(false);
        };
        let secret = self.auth_secret()?;
        Ok(verify_proxy_token(
            &secret,
            username,
            password,
            stored_password,
            now_secs(),
        ))
    }

    pub fn issue_proxy_token(&self, username: &str, password: &str) -> io::Result<Option<String>> {
        validate_name(username)?;
        let data = self.load()?;
        let Some(user) = data.users.get(username) else {
            return Ok(None);
        };
        if !user_accepts_password(username, user, password) {
            return Ok(None);
        }
        let Some(stored_password) = stored_password_for_signature(username, user) else {
            return Ok(None);
        };
        let secret = self.auth_secret()?;
        let expires = now_secs() + LOGIN_EXPIRATION_SECS;
        Ok(Some(proxy_token(
            &secret,
            username,
            expires,
            stored_password,
        )))
    }

    pub fn put_user(&self, username: &str, input: UserInput) -> io::Result<(bool, UserConfig)> {
        validate_name(username)?;
        let mut data = self.load()?;
        let created = !data.users.contains_key(username);
        let user = data
            .users
            .entry(username.to_string())
            .or_insert_with(|| UserConfig {
                username: username.to_string(),
                email: None,
                password: None,
                indexes: BTreeMap::new(),
            });
        if input.email.is_some() {
            user.email = input.email;
        }
        if let Some(password) = input.password {
            user.password = Some(hash_password(&password));
        }
        let user = user.clone();
        self.save(&data)?;
        Ok((created, user))
    }

    pub fn delete_user(&self, username: &str) -> io::Result<bool> {
        validate_name(username)?;
        if username == "root" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "cannot delete root user",
            ));
        }
        let mut data = self.load()?;
        let Some(user) = data.users.get(username) else {
            return Ok(false);
        };
        if let Some((index, _)) = user.indexes.iter().find(|(_, config)| !config.volatile) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("user {username:?} has non-volatile index: {index}"),
            ));
        }
        data.users.remove(username);
        self.save(&data)?;
        let user_dir = self.package_dir.join(username);
        if user_dir.exists() {
            fs::remove_dir_all(user_dir)?;
        }
        Ok(true)
    }

    pub fn index(&self, username: &str, index: &str) -> io::Result<Option<IndexConfig>> {
        validate_name(username)?;
        validate_name(index)?;
        Ok(self
            .load()?
            .users
            .get(username)
            .and_then(|user| user.indexes.get(index))
            .cloned())
    }

    pub fn put_index(
        &self,
        username: &str,
        index: &str,
        input: IndexInput,
    ) -> io::Result<(bool, IndexConfig)> {
        validate_name(username)?;
        validate_name(index)?;
        validate_index_input(&input)?;

        let mut data = self.load()?;
        let user = data
            .users
            .entry(username.to_string())
            .or_insert_with(|| UserConfig {
                username: username.to_string(),
                email: None,
                password: None,
                indexes: BTreeMap::new(),
            });
        let created = !user.indexes.contains_key(index);
        let config = user
            .indexes
            .entry(index.to_string())
            .or_insert_with(|| default_stage_config(username));

        if let Some(index_type) = input.index_type {
            config.index_type = index_type;
        }
        if let Some(bases) = input.bases {
            config.bases = normalize_bases(bases);
        }
        if let Some(volatile) = input.volatile {
            config.volatile = volatile;
        }
        if let Some(acl_upload) = input.acl_upload {
            config.acl_upload = normalize_acl_list(acl_upload);
        }
        if let Some(acl_pkg_read) = input.acl_pkg_read {
            config.acl_pkg_read = normalize_acl_list(acl_pkg_read);
        }
        if let Some(acl_toxresult_upload) = input.acl_toxresult_upload {
            config.acl_toxresult_upload = normalize_acl_list(acl_toxresult_upload);
        }
        if let Some(mirror_whitelist) = input.mirror_whitelist {
            config.mirror_whitelist = normalize_mirror_whitelist(mirror_whitelist);
        }
        let _legacy_pypi_whitelist = input.pypi_whitelist;
        for field in input.clear_fields {
            clear_index_config_field(config, &field);
        }
        if let Some(mirror_whitelist_inheritance) = input.mirror_whitelist_inheritance {
            config.mirror_whitelist_inheritance = mirror_whitelist_inheritance.to_ascii_lowercase();
        }
        if input.title.is_some() {
            config.title = input.title;
        }
        if input.description.is_some() {
            config.description = input.description;
        }
        if input.custom_data.is_some() {
            config.custom_data = input.custom_data;
        }
        if input.mirror_url.is_some() {
            config.mirror_url = input.mirror_url;
        }
        if input.mirror_web_url_fmt.is_some() {
            config.mirror_web_url_fmt = input.mirror_web_url_fmt;
        }
        if input.mirror_cache_expiry.is_some() {
            config.mirror_cache_expiry = input.mirror_cache_expiry;
        }
        if let Some(mirror_ignore_serial_header) = input.mirror_ignore_serial_header {
            config.mirror_ignore_serial_header = mirror_ignore_serial_header;
        }
        if let Some(mirror_no_project_list) = input.mirror_no_project_list {
            config.mirror_no_project_list = mirror_no_project_list;
        }
        if let Some(mirror_provides_core_metadata) = input.mirror_provides_core_metadata {
            config.mirror_provides_core_metadata = mirror_provides_core_metadata;
        }
        if let Some(mirror_use_external_urls) = input.mirror_use_external_urls {
            config.mirror_use_external_urls = mirror_use_external_urls;
        }
        if let Some(sources) = input.sources {
            config.sources = normalize_sources(sources);
        }
        for (key, value) in input.extra {
            config.extra.insert(key, value);
        }

        let config = config.clone();
        self.save(&data)?;
        Ok((created, config))
    }

    pub fn delete_index(&self, username: &str, index: &str) -> io::Result<bool> {
        validate_name(username)?;
        validate_name(index)?;
        let mut data = self.load()?;
        let Some(user) = data.users.get_mut(username) else {
            return Ok(false);
        };
        let Some(config) = user.indexes.get(index) else {
            return Ok(false);
        };
        if !config.volatile {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "cannot delete non-volatile index",
            ));
        }
        user.indexes.remove(index);
        self.save(&data)?;
        let stage_dir = self.package_dir.join(username).join(index);
        if stage_dir.exists() {
            fs::remove_dir_all(stage_dir)?;
        }
        Ok(true)
    }

    fn load(&self) -> io::Result<RegistryData> {
        if !self.path.exists() {
            return Ok(default_data());
        }
        let text = fs::read_to_string(&self.path)?;
        let mut data: RegistryData = serde_json::from_str(&text).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid registry json {}: {err}", self.path.display()),
            )
        })?;
        ensure_root(&mut data);
        Ok(data)
    }

    fn save(&self, data: &RegistryData) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_string_pretty(data).map_err(io::Error::other)?;
        fs::write(&self.path, format!("{text}\n"))
    }

    fn auth_secret(&self) -> io::Result<String> {
        if self.auth_secret_path.exists() {
            return Ok(fs::read_to_string(&self.auth_secret_path)?
                .trim()
                .to_string());
        }
        if let Some(parent) = self.auth_secret_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let secret = generate_secret();
        fs::write(&self.auth_secret_path, format!("{secret}\n"))?;
        Ok(secret)
    }
}

fn user_accepts_password(username: &str, user: &UserConfig, password: &str) -> bool {
    user.password
        .as_deref()
        .is_some_and(|stored| verify_stored_password(stored, password))
        || (username == "root" && user.password.is_none() && password.is_empty())
}

fn stored_password_for_signature<'a>(username: &str, user: &'a UserConfig) -> Option<&'a str> {
    user.password
        .as_deref()
        .or((username == "root" && user.password.is_none()).then_some(""))
}

fn hash_password(password: &str) -> String {
    let salt = generate_secret();
    let digest = password_digest(&salt, password);
    format!("{PASSWORD_HASH_PREFIX}:{salt}:{digest}")
}

fn verify_stored_password(stored: &str, password: &str) -> bool {
    let Some(rest) = stored.strip_prefix(PASSWORD_HASH_PREFIX) else {
        return constant_time_eq(stored.as_bytes(), password.as_bytes());
    };
    let Some((salt, digest)) = rest.strip_prefix(':').and_then(|rest| rest.split_once(':')) else {
        return false;
    };
    constant_time_eq(
        digest.as_bytes(),
        password_digest(salt, password).as_bytes(),
    )
}

fn password_digest(salt: &str, password: &str) -> String {
    sha256_hex(format!("{salt}:{password}").as_bytes())
}

fn proxy_token(secret: &str, username: &str, expires: u64, stored_password: &str) -> String {
    let user_hex = hex_encode(username.as_bytes());
    let signature = token_signature(secret, username, expires, stored_password);
    format!("{TOKEN_PREFIX}:{user_hex}:{expires}:{signature}")
}

fn verify_proxy_token(
    secret: &str,
    username: &str,
    token: &str,
    stored_password: &str,
    now: u64,
) -> bool {
    let parts = token.split(':').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != TOKEN_PREFIX {
        return false;
    }
    let Some(token_user) = hex_decode_string(parts[1]) else {
        return false;
    };
    if token_user != username {
        return false;
    }
    let Ok(expires) = parts[2].parse::<u64>() else {
        return false;
    };
    if expires < now {
        return false;
    }
    constant_time_eq(
        parts[3].as_bytes(),
        token_signature(secret, username, expires, stored_password).as_bytes(),
    )
}

fn token_signature(secret: &str, username: &str, expires: u64, stored_password: &str) -> String {
    let mut hash = FNV_OFFSET;
    let expires_text = expires.to_string();
    for part in [
        secret.as_bytes(),
        username.as_bytes(),
        expires_text.as_bytes(),
        stored_password.as_bytes(),
    ] {
        for byte in part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |diff, (left, right)| diff | (left ^ right))
        == 0
}

fn generate_secret() -> String {
    let mut bytes = [0_u8; 32];
    if fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .is_err()
    {
        let fallback = now_secs().to_le_bytes();
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = fallback[index % fallback.len()] ^ (index as u8).wrapping_mul(31);
        }
    }
    hex_encode(&bytes)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode_string(value: &str) -> Option<String> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        out.push((high << 4) | low);
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub fn default_stage_config(username: &str) -> IndexConfig {
    IndexConfig {
        index_type: "stage".to_string(),
        bases: Vec::new(),
        volatile: true,
        acl_upload: vec![username.to_string()],
        acl_pkg_read: default_pkg_read_acl(),
        acl_toxresult_upload: default_toxresult_acl(),
        mirror_whitelist: Vec::new(),
        mirror_whitelist_inheritance: default_mirror_whitelist_inheritance(),
        title: None,
        description: None,
        custom_data: None,
        mirror_url: None,
        mirror_web_url_fmt: None,
        mirror_cache_expiry: None,
        mirror_ignore_serial_header: false,
        mirror_no_project_list: false,
        mirror_provides_core_metadata: false,
        mirror_use_external_urls: false,
        sources: Vec::new(),
        extra: BTreeMap::new(),
    }
}

fn default_pkg_read_acl() -> Vec<String> {
    vec![":ANONYMOUS:".to_string()]
}

fn default_toxresult_acl() -> Vec<String> {
    vec![":ANONYMOUS:".to_string()]
}

fn default_mirror_whitelist_inheritance() -> String {
    "intersection".to_string()
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_data() -> RegistryData {
    let mut data = RegistryData::default();
    ensure_root(&mut data);
    data
}

fn ensure_root(data: &mut RegistryData) {
    data.users
        .entry("root".to_string())
        .or_insert_with(|| UserConfig {
            username: "root".to_string(),
            email: None,
            password: None,
            indexes: BTreeMap::new(),
        })
        .indexes
        .entry("pypi".to_string())
        .or_insert_with(|| IndexConfig {
            index_type: "mirror".to_string(),
            bases: Vec::new(),
            volatile: false,
            acl_upload: Vec::new(),
            acl_pkg_read: default_pkg_read_acl(),
            acl_toxresult_upload: default_toxresult_acl(),
            mirror_whitelist: Vec::new(),
            mirror_whitelist_inheritance: default_mirror_whitelist_inheritance(),
            title: Some("PyPI".to_string()),
            description: None,
            custom_data: None,
            mirror_url: Some("https://pypi.org/simple/".to_string()),
            mirror_web_url_fmt: Some("https://pypi.org/project/{name}/".to_string()),
            mirror_cache_expiry: None,
            mirror_ignore_serial_header: false,
            mirror_no_project_list: false,
            mirror_provides_core_metadata: false,
            mirror_use_external_urls: false,
            sources: Vec::new(),
            extra: BTreeMap::new(),
        });
}

fn clear_index_config_field(config: &mut IndexConfig, field: &str) {
    match field {
        "title" => config.title = None,
        "description" => config.description = None,
        "custom_data" => config.custom_data = None,
        "mirror_url" => config.mirror_url = None,
        "mirror_web_url_fmt" => config.mirror_web_url_fmt = None,
        "mirror_cache_expiry" => config.mirror_cache_expiry = None,
        "mirror_ignore_serial_header" => config.mirror_ignore_serial_header = false,
        "mirror_no_project_list" => config.mirror_no_project_list = false,
        "mirror_provides_core_metadata" => config.mirror_provides_core_metadata = false,
        "mirror_use_external_urls" => config.mirror_use_external_urls = false,
        "sources" => config.sources.clear(),
        _ => {
            config.extra.remove(field);
        }
    }
}

fn validate_index_input(input: &IndexInput) -> io::Result<()> {
    if let Some(index_type) = &input.index_type
        && index_type != "stage"
        && index_type != "mirror"
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "index type must be stage or mirror",
        ));
    }
    if let Some(bases) = &input.bases {
        for base in bases {
            validate_base(base)?;
        }
    }
    if let Some(sources) = &input.sources {
        let mut seen = std::collections::BTreeSet::new();
        for source in sources {
            validate_source_name(source)?;
            let source = source.trim();
            if !seen.insert(source) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate source name {source}"),
                ));
            }
        }
    }
    if let Some(inheritance) = &input.mirror_whitelist_inheritance {
        let inheritance = inheritance.to_ascii_lowercase();
        if inheritance != "intersection" && inheritance != "union" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mirror_whitelist_inheritance must be intersection or union",
            ));
        }
    }
    if let Some(mirror_url) = &input.mirror_url
        && !mirror_url.starts_with("http://")
        && !mirror_url.starts_with("https://")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mirror_url must be an http or https URL",
        ));
    }
    if let Some(mirror_cache_expiry) = input.mirror_cache_expiry
        && mirror_cache_expiry < 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mirror_cache_expiry must be a non-negative integer",
        ));
    }
    Ok(())
}

fn validate_base(base: &str) -> io::Result<()> {
    let base = base.trim_matches('/');
    let Some((user, index)) = base.split_once('/') else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "base must use user/index format",
        ));
    };
    validate_name(user)?;
    validate_name(index)
}

fn normalize_bases(bases: Vec<String>) -> Vec<String> {
    bases
        .into_iter()
        .map(|base| base.trim_matches('/').to_string())
        .collect()
}

fn normalize_sources(sources: Vec<String>) -> Vec<String> {
    sources
        .into_iter()
        .map(|source| source.trim().to_string())
        .filter(|source| !source.is_empty())
        .collect()
}

fn normalize_acl_list(entries: Vec<String>) -> Vec<String> {
    entries
        .into_iter()
        .map(|entry| {
            let upper = entry.to_ascii_uppercase();
            if upper == ":ANONYMOUS:" || upper == ":AUTHENTICATED:" {
                upper
            } else {
                entry
            }
        })
        .collect()
}

fn normalize_mirror_whitelist(entries: Vec<String>) -> Vec<String> {
    entries
        .into_iter()
        .flat_map(|entry| {
            entry
                .split(',')
                .map(|part| part.trim().to_string())
                .collect::<Vec<_>>()
        })
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            if entry == "*" {
                entry
            } else {
                entry.to_ascii_lowercase().replace('_', "-")
            }
        })
        .collect()
}

fn deserialize_optional_whitelist<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        Value::String(value) => Ok(Some(vec![value])),
        Value::Array(values) => values
            .into_iter()
            .map(|value| match value {
                Value::String(value) => Ok(value),
                other => Err(serde::de::Error::custom(format!(
                    "whitelist entries must be strings, got {other}"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        other => Err(serde::de::Error::custom(format!(
            "whitelist must be a string or list of strings, got {other}"
        ))),
    }
}

fn deserialize_optional_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| serde::de::Error::custom("integer value out of range"))
            .map(Some),
        Value::String(value) => value
            .parse::<i64>()
            .map(Some)
            .map_err(|err| serde::de::Error::custom(format!("invalid integer: {err}"))),
        other => Err(serde::de::Error::custom(format!(
            "expected integer or integer string, got {other}"
        ))),
    }
}

fn validate_source_name(value: &str) -> io::Result<()> {
    if value.trim().is_empty() || value.contains('/') || value.contains('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid source name",
        ));
    }
    Ok(())
}

fn validate_name(value: &str) -> io::Result<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains('+')
        || value == "."
        || value == ".."
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid name"));
    }
    Ok(())
}

impl From<UserConfig> for PublicUserConfig {
    fn from(value: UserConfig) -> Self {
        Self {
            username: value.username,
            email: value.email,
            indexes: value.indexes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(name: &str) -> Registry {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-registry-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        Registry::new(dir)
    }

    #[test]
    fn starts_with_root_pypi_mirror() {
        let registry = registry("root");

        let root = registry.user("root").unwrap().unwrap();

        assert_eq!(root.indexes["pypi"].index_type, "mirror");
        assert!(!root.indexes["pypi"].volatile);
        assert!(registry.verify_password("root", "").unwrap());
        assert!(registry.issue_proxy_token("root", "").unwrap().is_some());
    }

    #[test]
    fn creates_user_and_index_config() {
        let registry = registry("create");

        let (created, user) = registry
            .put_user(
                "alice",
                UserInput {
                    email: Some("alice@example.com".to_string()),
                    password: Some("secret".to_string()),
                },
            )
            .unwrap();

        assert!(created);
        assert_eq!(user.email.as_deref(), Some("alice@example.com"));
        let stored_password = user.password.as_deref().unwrap();
        assert!(stored_password.starts_with(PASSWORD_HASH_PREFIX));
        assert!(!stored_password.contains("secret"));
        assert!(registry.verify_password("alice", "secret").unwrap());
        assert!(!registry.verify_password("alice", "wrong").unwrap());
        let token = registry
            .issue_proxy_token("alice", "secret")
            .unwrap()
            .unwrap();
        assert!(registry.verify_password("alice", &token).unwrap());
        assert!(!registry.verify_password("bob", &token).unwrap());
        assert!(
            registry
                .public_user("alice")
                .unwrap()
                .unwrap()
                .email
                .is_some()
        );

        let (created, index) = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["/root/pypi/".to_string()]),
                    volatile: Some(false),
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: Some(vec!["corp".to_string(), "pypi".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(created);
        assert_eq!(index.bases, vec!["root/pypi".to_string()]);
        assert!(!index.volatile);
        assert_eq!(index.acl_upload, vec!["alice".to_string()]);
        assert_eq!(index.acl_pkg_read, vec![":ANONYMOUS:".to_string()]);
        assert_eq!(index.sources, vec!["corp".to_string(), "pypi".to_string()]);

        let (_, index) = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: None,
                    volatile: None,
                    index_type: None,
                    acl_upload: Some(vec![":anonymous:".to_string(), "Bob".to_string()]),
                    acl_pkg_read: Some(vec![":authenticated:".to_string()]),
                    acl_toxresult_upload: Some(vec![":anonymous:".to_string()]),
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(
            index.acl_upload,
            vec![":ANONYMOUS:".to_string(), "Bob".to_string()]
        );
        assert_eq!(index.acl_pkg_read, vec![":AUTHENTICATED:".to_string()]);
        assert_eq!(index.acl_toxresult_upload, vec![":ANONYMOUS:".to_string()]);
    }

    #[test]
    fn verifies_legacy_plaintext_passwords() {
        let registry = registry("legacy-password");
        let mut data = RegistryData::default();
        data.users.insert(
            "alice".to_string(),
            UserConfig {
                username: "alice".to_string(),
                email: None,
                password: Some("legacy-secret".to_string()),
                indexes: BTreeMap::new(),
            },
        );
        registry.save(&data).unwrap();

        assert!(registry.verify_password("alice", "legacy-secret").unwrap());
        assert!(!registry.verify_password("alice", "wrong").unwrap());
        let token = registry
            .issue_proxy_token("alice", "legacy-secret")
            .unwrap()
            .unwrap();
        assert!(registry.verify_password("alice", &token).unwrap());
    }

    #[test]
    fn rejects_invalid_base() {
        let registry = registry("bad-base");

        let err = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["root".to_string()]),
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_duplicate_index_sources() {
        let registry = registry("duplicate-sources");

        let err = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: None,
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: Some(vec!["pypi".to_string(), " pypi ".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("duplicate source name pypi"));
    }

    #[test]
    fn stores_devpi_index_metadata_keys() {
        let registry = registry("index-metadata");

        let (_, index) = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    mirror_whitelist: Some(vec!["he_llo,Django".to_string()]),
                    mirror_whitelist_inheritance: Some("UNION".to_string()),
                    title: Some("Alice Dev".to_string()),
                    description: Some("private stage".to_string()),
                    custom_data: Some(serde_json::json!({"team": "platform"})),
                    mirror_url: Some("https://example.com/simple/".to_string()),
                    mirror_web_url_fmt: Some("https://example.com/project/{name}/".to_string()),
                    mirror_cache_expiry: Some(600),
                    mirror_ignore_serial_header: Some(true),
                    mirror_no_project_list: Some(true),
                    mirror_provides_core_metadata: Some(true),
                    mirror_use_external_urls: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(
            index.mirror_whitelist,
            vec!["he-llo".to_string(), "django".to_string()]
        );
        assert_eq!(index.mirror_whitelist_inheritance, "union");
        assert_eq!(index.title.as_deref(), Some("Alice Dev"));
        assert_eq!(index.description.as_deref(), Some("private stage"));
        assert_eq!(
            index.custom_data,
            Some(serde_json::json!({"team": "platform"}))
        );
        assert_eq!(
            index.mirror_url.as_deref(),
            Some("https://example.com/simple/")
        );
        assert_eq!(
            index.mirror_web_url_fmt.as_deref(),
            Some("https://example.com/project/{name}/")
        );
        assert_eq!(index.mirror_cache_expiry, Some(600));
        assert!(index.mirror_ignore_serial_header);
        assert!(index.mirror_no_project_list);
        assert!(index.mirror_provides_core_metadata);
        assert!(index.mirror_use_external_urls);

        let (_, index) = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    pypi_whitelist: Some(vec!["legacy".to_string()]),
                    volatile: Some(false),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(
            index.mirror_whitelist,
            vec!["he-llo".to_string(), "django".to_string()]
        );
        assert!(!index.volatile);

        let input: IndexInput =
            serde_json::from_value(serde_json::json!({"mirror_cache_expiry": "42"})).unwrap();
        assert_eq!(input.mirror_cache_expiry, Some(42));
    }

    #[test]
    fn rejects_invalid_mirror_metadata_keys() {
        let registry = registry("bad-mirror-metadata");

        let err = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    mirror_whitelist_inheritance: Some("replace".to_string()),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let err = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    mirror_url: Some("ftp://example.com/simple/".to_string()),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let err = registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    mirror_cache_expiry: Some(-1),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn refuses_non_volatile_index_delete() {
        let registry = registry("non-volatile");
        registry
            .put_index(
                "alice",
                "prod",
                IndexInput {
                    bases: None,
                    volatile: Some(false),
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();

        let err = registry.delete_index("alice", "prod").unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn refuses_user_delete_with_non_volatile_index() {
        let registry = registry("non-volatile-user");
        registry
            .put_index(
                "alice",
                "prod",
                IndexInput {
                    bases: None,
                    volatile: Some(false),
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();

        let err = registry.delete_user("alice").unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(registry.user("alice").unwrap().is_some());
    }
}

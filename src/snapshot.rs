use std::fs;
use std::io;
use std::path::Path;

const SNAPSHOT_MANIFEST: &str = ".devpi-rs-export.json";
const SNAPSHOT_FORMAT: &str = "devpi-rs-package-dir-v1";

pub fn export_package_dir(package_dir: &Path, output_dir: &Path) -> io::Result<()> {
    if !package_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("package dir {} does not exist", package_dir.display()),
        ));
    }
    if output_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("output dir {} already exists", output_dir.display()),
        ));
    }
    copy_dir_all(package_dir, output_dir)?;
    write_manifest(output_dir)
}

pub fn import_package_dir(input_dir: &Path, package_dir: &Path) -> io::Result<()> {
    if !input_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("input dir {} does not exist", input_dir.display()),
        ));
    }
    validate_manifest(input_dir)?;
    if package_dir.exists() && package_dir.read_dir()?.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("package dir {} is not empty", package_dir.display()),
        ));
    }
    copy_dir_all(input_dir, package_dir)
}

fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_name() == SNAPSHOT_MANIFEST {
            continue;
        }
        let file_type = entry.file_type()?;
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn write_manifest(output_dir: &Path) -> io::Result<()> {
    let manifest = serde_json::json!({
        "format": SNAPSHOT_FORMAT,
        "server": "devpi-rs",
        "version": env!("CARGO_PKG_VERSION"),
    });
    fs::write(
        output_dir.join(SNAPSHOT_MANIFEST),
        format!("{manifest}\n").as_bytes(),
    )
}

fn validate_manifest(input_dir: &Path) -> io::Result<()> {
    let path = input_dir.join(SNAPSHOT_MANIFEST);
    let text = fs::read_to_string(&path)?;
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid snapshot manifest {}: {err}", path.display()),
        )
    })?;
    if value["format"] != SNAPSHOT_FORMAT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported snapshot format in {}", path.display()),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_and_imports_package_dir_copy() {
        let root = std::env::temp_dir().join(format!("devpi-rs-snapshot-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let packages = root.join("packages");
        let export = root.join("export");
        let import = root.join("imported");
        fs::create_dir_all(packages.join("alice/dev/demo")).unwrap();
        fs::write(packages.join("alice/dev/demo/demo-1.0.0.tar.gz"), b"sdist").unwrap();

        export_package_dir(&packages, &export).unwrap();
        import_package_dir(&export, &import).unwrap();

        assert!(export.join(SNAPSHOT_MANIFEST).is_file());
        assert!(!import.join(SNAPSHOT_MANIFEST).exists());
        assert_eq!(
            fs::read(import.join("alice/dev/demo/demo-1.0.0.tar.gz")).unwrap(),
            b"sdist"
        );
    }

    #[test]
    fn import_refuses_non_empty_package_dir() {
        let root = std::env::temp_dir().join(format!(
            "devpi-rs-snapshot-non-empty-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let input = root.join("input");
        let packages = root.join("packages");
        fs::create_dir_all(&input).unwrap();
        fs::write(input.join("file"), b"input").unwrap();
        write_manifest(&input).unwrap();
        fs::create_dir_all(&packages).unwrap();
        fs::write(packages.join("file"), b"existing").unwrap();

        let err = import_package_dir(&input, &packages).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn import_requires_supported_manifest() {
        let root =
            std::env::temp_dir().join(format!("devpi-rs-snapshot-manifest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let input = root.join("input");
        let packages = root.join("packages");
        fs::create_dir_all(&input).unwrap();

        let missing = import_package_dir(&input, &packages).unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);

        fs::write(
            input.join(SNAPSHOT_MANIFEST),
            r#"{"format":"something-else"}"#,
        )
        .unwrap();
        let unsupported = import_package_dir(&input, &packages).unwrap_err();
        assert_eq!(unsupported.kind(), io::ErrorKind::InvalidData);
    }
}

#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

use pi_agent::lsp::{
    diagnostics::{format_file, FormatResultStatus},
    types::{CustomLinter, ServerConfig},
    LspSession,
};
use tokio_util::sync::CancellationToken;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-agent-lsp-format-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn write(&self, name: &str, content: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::write(&path, content).expect("write fixture");
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn shell_path() -> PathBuf {
    let path = std::env::var_os("PATH").expect("PATH must be set");
    std::env::split_paths(&path)
        .map(|dir| dir.join("sh"))
        .find(|candidate| {
            fs::metadata(candidate)
                .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        })
        .expect("PATH must provide executable sh")
}

fn fake_biome(dir: &TempDir, mutate_target: bool) -> PathBuf {
    let executable = dir.path.join("fake-biome");
    let mutation = if mutate_target {
        "printf '%s\\n' 'external editor content' > \"$3\"\n"
    } else {
        ""
    };
    let shell = shell_path();
    let script = format!(
        "#!{shell}\nset -eu\n[ \"$1\" = format ]\n[ \"$2\" = --stdin-file-path ]\ncat \"$3\" > .formatter-seen-source\ncat > .formatter-seen-stdin\n{mutation}if [ ! -e .formatter-empty ]; then printf '%s\\n' 'const value = 1;'; fi\n",
        shell = shell.display(),
    );
    fs::write(&executable, script).expect("write fake biome");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).expect("chmod fake biome");
    executable
}

fn fake_empty_biome(dir: &TempDir) -> PathBuf {
    let executable = fake_biome(dir, false);
    fs::write(dir.path.join(".formatter-empty"), "").expect("mark empty output");
    executable
}

fn biome_servers(executable: &Path) -> Vec<(String, ServerConfig)> {
    vec![(
        "biome".to_string(),
        ServerConfig {
            command: "biome".to_string(),
            args: Vec::new(),
            file_types: vec![".ts".to_string()],
            language_id: Some("typescript".to_string()),
            root_markers: Vec::new(),
            init_options: None,
            settings: None,
            disabled: None,
            warmup_timeout_ms: None,
            workspace_ready_timings: None,
            capabilities: None,
            is_linter: Some(true),
            resolved_command: Some(executable.to_path_buf()),
            custom_linter: Some(CustomLinter::Biome),
        },
    )]
}

async fn format_fixture(
    dir: &TempDir,
    file: &Path,
    executable: &Path,
) -> Result<(FormatResultStatus, String), String> {
    format_file(
        file,
        &dir.path,
        &biome_servers(executable),
        &CancellationToken::new(),
        &LspSession::new(false),
    )
    .await
}

#[tokio::test]
async fn biome_formatter_receives_snapshot_via_stdin_before_guarded_publish() {
    let dir = TempDir::new("stdin-snapshot");
    let original = "const value=1;\n";
    let file = dir.write("source.ts", original);
    let executable = fake_biome(&dir, false);

    let (status, result) = format_fixture(&dir, &file, &executable).await.expect("format");

    assert_eq!(status, FormatResultStatus::Formatted);
    assert_eq!(result, "const value = 1;\n");
    assert_eq!(fs::read_to_string(dir.path.join(".formatter-seen-source")).expect("source snapshot"), original);
    assert_eq!(fs::read_to_string(dir.path.join(".formatter-seen-stdin")).expect("stdin snapshot"), original);
}

#[tokio::test]
async fn biome_formatter_rejects_file_change_during_formatting_and_preserves_external_content() {
    let dir = TempDir::new("external-change");
    let file = dir.write("source.ts", "const value=1;\n");
    let executable = fake_biome(&dir, true);

    let error = format_fixture(&dir, &file, &executable).await.expect_err("stale publish");

    assert!(error.contains("changed since it was read"), "unexpected error: {error}");
    assert_eq!(fs::read_to_string(&file).expect("external content"), "external editor content\n");
}

#[tokio::test]
async fn biome_formatter_publishes_formatted_content_when_file_is_unchanged() {
    let dir = TempDir::new("normal-format");
    let file = dir.write("source.ts", "const value=1;\n");
    let executable = fake_biome(&dir, false);

    let (status, result) = format_fixture(&dir, &file, &executable).await.expect("format");

    assert_eq!(status, FormatResultStatus::Formatted);
    assert_eq!(result, "const value = 1;\n");
    assert_eq!(fs::read_to_string(&file).expect("formatted content"), "const value = 1;\n");
}

#[tokio::test]
async fn biome_empty_output_does_not_erase_nonempty_file() {
    let dir = TempDir::new("empty-output");
    let original = "const value=1;\n";
    let file = dir.write("source.ts", original);
    let executable = fake_empty_biome(&dir);

    let (status, result) = format_fixture(&dir, &file, &executable).await.expect("format failure status");

    assert_eq!(status, FormatResultStatus::Failed);
    assert_eq!(result, original);
    assert_eq!(fs::read_to_string(&file).expect("source content"), original);
}

#[tokio::test]
async fn biome_can_format_whitespace_only_file_to_empty() {
    let dir = TempDir::new("empty-whitespace");
    let file = dir.write("source.ts", "  \n\t");
    let executable = fake_empty_biome(&dir);
    let (status, result) = format_fixture(&dir, &file, &executable).await.expect("format");
    assert_eq!(status, FormatResultStatus::Formatted);
    assert_eq!(result, "");
    assert_eq!(fs::read_to_string(&file).expect("source content"), "");
}

//! Helpers for the live Vitess tests. Uses the `mysql` CLI so the test crate needs no MySQL
//! driver; the local cluster from `infra/vitess` listens on 33577 (MySQL) and 33575 (gRPC).

use std::time::Duration;

use orbit_vstream::StreamItem;
use tokio::sync::mpsc;

pub fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    });
}

pub fn grpc_uri() -> String {
    std::env::var("ORBIT_TEST_VITESS_GRPC").unwrap_or_else(|_| "http://127.0.0.1:33575".into())
}

fn mysql_args() -> Vec<String> {
    let host = std::env::var("ORBIT_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ORBIT_TEST_MYSQL_PORT").unwrap_or_else(|_| "33577".into());
    vec![
        "-h".into(),
        host,
        "-P".into(),
        port,
        "-u".into(),
        "root".into(),
        "orbit".into(),
    ]
}

pub async fn mysql(statements: &[&str]) {
    use tokio::io::AsyncWriteExt;
    let sql = statements.join(";\n") + ";";
    let mut child = tokio::process::Command::new("mysql")
        .args(mysql_args())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("mysql cli");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin.write_all(sql.as_bytes()).await.expect("write sql");
    drop(stdin);
    let out = child.wait_with_output().await.expect("mysql cli");
    assert!(
        out.status.success(),
        "mysql failed: {}\nsql: {}",
        String::from_utf8_lossy(&out.stderr),
        &sql[..sql.len().min(500)]
    );
}

pub struct TestDb {
    table: String,
}

impl TestDb {
    pub async fn new(table: &str) -> Self {
        mysql(&[
            &format!("DROP TABLE IF EXISTS `{table}`"),
            &format!(
                "CREATE TABLE `{table}` (id varchar(64) NOT NULL, org varchar(64) NOT NULL, n int NULL, flag tinyint(1) NOT NULL DEFAULT 0, doc json NULL, created datetime(3) NULL, PRIMARY KEY (id))"
            ),
        ])
        .await;
        // Vitess needs a moment to notice the new table in its schema tracker.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        Self {
            table: table.to_string(),
        }
    }

    /// Creates `table` with the given column definitions (the body of `CREATE TABLE`).
    #[allow(dead_code)]
    pub async fn create(table: &str, body: &str) -> Self {
        mysql(&[
            &format!("DROP TABLE IF EXISTS `{table}`"),
            &format!("CREATE TABLE `{table}` ({body})"),
        ])
        .await;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        Self {
            table: table.to_string(),
        }
    }

    pub async fn drop(self) {
        mysql(&[&format!("DROP TABLE IF EXISTS `{}`", self.table)]).await;
    }
}

pub async fn wait_for(
    rx: &mut mpsc::Receiver<StreamItem>,
    timeout: Duration,
    pred: impl Fn(&StreamItem) -> bool,
) -> StreamItem {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(item)) => {
                if pred(&item) {
                    return item;
                }
            }
            Ok(None) => panic!("stream closed while waiting"),
            Err(_) => panic!("timed out after {timeout:?} waiting for a matching stream item"),
        }
    }
}

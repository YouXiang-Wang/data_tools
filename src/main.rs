use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use sqlx::{MySql, MySqlPool, Pool};
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};
/// Simple program to delete records in rocket.messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value = "3306")]
    port: String,

    #[arg(short, long, default_value = "root")]
    user: String,

    #[arg(short, long, default_value = "wyx1005")]
    password: String,

    #[arg(long, default_value = "binlog_test")]
    database: String,

    #[arg(long, default_value = "t_types_test")]
    table: String,

    #[arg(short, long, default_value_t = 100)]
    count: u32,

    #[arg(short, long, default_value_t = 1)]
    batch: u8
}


struct AllTypesTest {
    id1: String,
    id2: String,
    id3: String,
    gender: String,
    bool_1: bool,
    bit_1: u8,
    int_tiny: i32,
    int_small: i32,
    int_medium: i32,
    int_int: i32,
    int_big: i64,
    pay1: f32,
    pay2: f64,
    pay3: String,
    latest_year: i32,
    latest_date: String,
    latest_time: String,
    latest_datetime: String,
    latest_timestamp: String,
    blob_tiny: Option<String>,
    blob_blob: Option<String>,
    blob_medium: Option<String>,
    text1: Option<String>,
    long_text: Option<String>
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let host = args.host;
    let port = args.port;
    let user = args.user;
    let password = args.password;
    let database = args.database;
    let table = args.table;
    let count = args.count;
    let count = args.count;
    let begin = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();

    println!("host={}, port={}, user={}, database={}, table={}, count={}, begin={}", host, port, user, database, table, count, begin);

    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        user, password, host, port , database
    );
    let pool = MySqlPool::connect(&url)
        .await
        .expect("Failed to connect to MySQL.");

    /*
    let data = vec![
        ("1", "1", 1, ""),
        ("2", "1", 1, "1"),
        ("3", "1", 1, "1"),
        ("4", "1", 1, "1")

    ];
    for row in &data {
        sqlx::query("INSERT INTO t_0 (id, id1, age, name) VALUES (?, ?, ?, ?)")
            .bind(row.0)
            .bind(row.1)
            .bind(row.2)
            .bind(row.3)
            .execute(&pool)
            .await?;
    }
     */

    for _ in 0..count {
        let id = Uuid::new_v4().to_string();
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        println!("Insert into t_types_test. id = {}, at={}", id, at);
        sqlx::query("INSERT INTO t_types_test(id1, id2, id3, gender, bool_1, bit_1, int_tiny, int_small, int_medium, int_int, int_big, pay1, pay2, pay3, latest_year, latest_date, latest_time, latest_datetime, latest_timestamp, blob_tiny, blob_blob, blob_medium, text1, long_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id)
            .bind("1")
            .bind("12345678")
            .bind("male")
            .bind(true)
            .bind(1)
            .bind(123)
            .bind(12345)
            .bind(1677215)
            .bind(26772156)
            .bind(467721565)
            .bind(0.99)
            .bind(0.9999)
            .bind("3333.50")
            .bind(2023)
            .bind("2023-06-16")
            .bind("12:01:01")
            .bind("2023-02-16 14:14:14")
            .bind("2023-06-16 23:59:59")
            .bind::<Option<Vec<u8>>>(None)
            .bind::<Option<Vec<u8>>>(None)
            .bind::<Option<Vec<u8>>>(None)
            .bind("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
            .bind("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
            .execute(&pool)
            .await?;
    }


    let end = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    println!("\n===== Insert start_at={}, end_at={}, cost={}", begin, end, end - begin);

    Ok(())
}

fn load_dir(dir: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(path_str) = path.file_name().and_then(|s| s.to_str()) {
                files.push(path_str.to_string());
            }
        }
    }
    Ok(files)
}

fn load_record(record: &str) -> Result<Vec<String>> {
    let contents = fs::read_to_string(record)?;
    let files: Vec<String> = contents.lines().map(String::from).collect();
    Ok(files)
}

fn append_to_record(path: &str, content: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(path)?;
    writeln!(file, "{}", content)?;
    Ok(())
}

async fn execute_batch_delete(id_file: &str, pool: &Pool<MySql>) -> Result<()> {
    let contents = fs::read_to_string(id_file)?;
    let ids: Vec<String> = contents.lines().map(|id|format!("'{}'", id)).collect();
    let query = format!(
        "DELETE FROM messages WHERE id IN ({})",
        ids.join(", ")
    );
    sqlx::query(&query).execute(pool).await?;
    Ok(())
}


async fn execute_batch_insert(id_file: &str, pool: &Pool<MySql>) -> Result<()> {
    let contents = fs::read_to_string(id_file)?;
    let ids: Vec<String> = contents.lines().map(|id|format!("'{}'", id)).collect();
    let query = format!(
        "DELETE FROM messages WHERE id IN ({})",
        ids.join(", ")
    );
    sqlx::query(&query).execute(pool).await?;
    Ok(())
}
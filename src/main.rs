use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use sqlx::{Execute, MySql, MySqlPool, Pool};
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};
use console::{style, Emoji};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use std::error::Error;
use sqlx::QueryBuilder;
use tokio;
use tokio::sync::Mutex;
use std::sync::Arc;
use tokio::{task, time};
use rand::{thread_rng, Rng};
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use dashmap::DashMap;
use console::Style;
use reqwest::header::HeaderMap;
use reqwest::Client;
use serde::Deserialize;

/// Simple program to delete records in rocket.messages
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long, default_value = "insert")]
    action: String,

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

    #[arg(long, default_value = "NCMReplicaGroup")]
    replica_group: String,

    #[arg(short, long, default_value_t = 100)]
    count: u32,

    #[arg(short, long, default_value_t = 1)]
    batch: u32,

    #[arg(long, default_value_t = -1)]
    rate: i32
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


#[derive(Debug, Deserialize)]
struct ApiError {
    code: String,
    apiName: Option<String>,
    message: Option<String>
}
#[derive(Debug, Deserialize)]
struct QueryResultCountResponse {
    dataServerId: String,
    host: String,
    port: u32,
    database: String,
    query: Option<String>,
    count: u64,
    apiError: Option<ApiError>

}

#[derive(Debug, Deserialize)]
struct CommonMessageResult {
    status: i32,
    message: String
}

static TRUCK: Emoji<'_, '_> = Emoji("🚚  ", "");
static LOOKING_GLASS: Emoji<'_, '_> = Emoji("🔍  ", "");

static CLIP: Emoji<'_, '_> = Emoji("🔗  ", "");
static PAPER: Emoji<'_, '_> = Emoji("📃  ", "");
static SPARKLE: Emoji<'_, '_> = Emoji("✨ ", ":-)");



static PARAMETERS: Emoji<'_, '_> = Emoji("🛠️️ ", ":-)");
static DATABASE: Emoji<'_, '_> = Emoji("🗃️ ", ":-)");
static TABLE: Emoji<'_, '_> = Emoji("📋 ", ":-)");
static HOST: Emoji<'_, '_> = Emoji("🖥️ ", ":-)");

static PORT: Emoji<'_, '_> = Emoji("🔌 ", ":-)");

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!("{} {} Resolving args...", style("[1/4]").bold().dim(), PARAMETERS);
    let action = &args.action;
    let table = &args.table;
    let host = &args.host;
    let port = &args.port;
    let user = &args.user;
    let password = &args.password;
    let database = &args.database;
    let replica_group = &args.replica_group;
    let count = args.count;
    let db_url = format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, database);
    println!("{} {} Building database connections for host=[{}], port=[{}], database=[{}], user=[{}], table=[{}]", style("[2/4]").bold().dim(), DATABASE, host, port, database, user, table);
    let pool = MySqlPool::connect(&db_url).await.expect("Failed to connect to MySQL.");

    let pool_for_insert = pool.clone();
    let pool_for_count = pool.clone();

    let key = "string";

    let current_tbl_count = get_table_count(&pool_for_count, "t_types_test").await?;
    let should_final_count = current_tbl_count as u64 + count as u64;


    let blue = Style::new().blue();
    let green = Style::new().green();

    println!("{} {} Current {} record count = {}, final count = {}", style("[3/4]").bold().dim(), TABLE, table, blue.apply_to(current_tbl_count), green.apply_to(should_final_count));

    let map: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());
    let map_for_write = map.clone();

    let client = create_client().await.unwrap(); //Client::builder().danger_accept_invalid_certs(true).build()?;

    let url = "http://127.0.0.1:18090/data/sync/table/NCMReplicaGroup/t_types_test";


    let client_for_count = client.clone();

    let get_count_thread = tokio::spawn(async move {
        loop {
            match client_for_count.get(url).send().await {
                Ok(rep) => {
                    match rep.json::<Vec<QueryResultCountResponse>>().await {
                        Ok(res) => {
                            for r in &res {
                                map_for_write.insert(r.dataServerId.to_string(), r.count);
                            }
                        },
                        Err(e) => println!("error = {}", e.to_string())

                    }
                },
                Err(e) => {
                    println!("Exception = {}", e.to_string())
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });



    let peers = get_servers(&client, &url).await;
    let mut vec = Vec::<(String, String, String)>::new();
    let mut vec_1 = Vec::<(&str, &str, &str)>::new();

    let styles = [
        ("Rough bar:", "█  ", "red"),
        ("Fine bar: ", "█▉▊▋▌▍▎▏  ", "yellow"),
        ("Vertical: ", "█▇▆▅▄▃▂▁  ", "green"),
        ("Fade in:  ", "█▓▒░  ", "blue"),
        ("Blocky:   ", "█▛▌▖  ", "magenta"),
    ];

    let progress = for (index, r)  in peers.iter().enumerate() {
        let _key_1 = format!("{}:{}", r.host, r.port);
        let _key_2 = format!("{}:{}", r.host, r.port);
        //let _key_3 = format!("{}:{}", r.host, r.port).as_str();
        vec.push((_key_1, styles.get(index % 5).unwrap_or(&(_key_2.as_str(), "█  ", "red")).2.to_string(), "".to_string()));
        //vec_1.push((_key_3, _key_3, _key_3));
    };

    let m = MultiProgress::new();



    /*
    for i in &vec {
        println!("元素1: {}, 元素2: {}, 元素3: {}", i.0, i.1, i.2);
    }
     */




    /*

    for s in styles.iter()
    {
        //let s = styles.get(3).unwrap();
        let pb = m.add(ProgressBar::new(should_final_count));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(&format!("{{prefix:.bold}}▕{{bar:.{}}}▏{{msg}}", s.2))
                .progress_chars(s.1),
        );
        pb.set_prefix(s.0);
        let count_map = map.clone();
        let h = tokio::spawn(async move {
            loop {
                match count_map.get(key) {
                    Some(count_ref) => {
                        let count = *count_ref;
                        if(count >= should_final_count) {
                            break;
                        } else {
                            pb.inc(count);
                            pb.set_message(format!("{}/{}", count, should_final_count));
                        }
                    }
                    None => {},
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            pb.finish_with_message(format!("{}/{}", should_final_count, should_final_count));
        });
        //h.await.unwrap();
    }

     */

    let insert_thread = tokio::spawn(async move {
        insert_into(&args, &pool_for_insert).await;
    });

    insert_thread.await.unwrap();

    let bar = ProgressBar::new(count as u64);
    m.add(bar);
    m.join().unwrap();


    /*
    match action.as_str() {
        "insert" => {
            let future = insert_into(&args, &pool_for_insert);
            future.await?;
        },

        "view" => {

        },
        _ => {

        }
    }
     */

    Ok(())
}


async fn get_servers(client: &Client, url: &str) -> Vec<QueryResultCountResponse> {
    match client.get(url).send().await {
        Ok(rep) => {
            match rep.json::<Vec<QueryResultCountResponse>>().await {
                Ok(res) => {
                    res
                },
                Err(e) => {
                    println!("error = {}", e.to_string());
                    Vec::<QueryResultCountResponse>::new()
                }

            }
        },
        Err(e) => {
            println!("Exception = {}", e.to_string());
            Vec::<QueryResultCountResponse>::new()
        }
    }
}

async fn create_client() -> std::result::Result<Client, reqwest::Error> {
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    Ok(client)
}
async fn insert_into(args: &Args, pool: &Pool<MySql>) -> Result<()> {
    let table = &args.table;
    let count = args.count;
    let batch = args.batch;
    let rate = args.rate;
    let _batch = if(rate <= 0) {
        if(batch > count) {
            count
        } else {
            batch
        }
    } else {
        rate as u32
    };
    let begin = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
    match table.as_str() {
        "t_types_test" => {
            if(count == 0 || batch == 0) {
                Ok(())
            } else {
                let current_tbl_count = match get_table_count(&pool, table).await {
                    Ok(count) => count,
                    Err(e) => {
                        eprintln!("Error querying table count: {}", e);
                        1
                    }
                };

                let table_clone = table.clone();
                let round = if(count % _batch == 0) {count / _batch} else {count / _batch + 1};
                let _style = ProgressStyle::default_bar()
                    .template(&format!("{{prefix:.bold}}▕{{bar:.{}}}▏{{msg}}", "red"))
                    .progress_chars("Inserting");

                let bar = ProgressBar::new(count as u64);

                for i in 0..round {
                    let mut builder = QueryBuilder::new("INSERT INTO t_types_test(id1, id2, id3, gender, bool_1, bit_1, int_tiny, int_small, int_medium, int_int, int_big, pay1, pay2, pay3, latest_year, latest_date, latest_time, latest_datetime, latest_timestamp, blob_tiny, blob_blob, blob_medium, text1, long_text) VALUES ");
                    let _begin = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
                    bar.set_prefix(format!("[{}/?]", i + 1));
                    for i in 0.._batch {
                        let id = Uuid::new_v4().to_string();
                        builder.push("(");
                        builder.push_bind(id);
                        builder.push(", ");
                        builder.push_bind("''12345678`~!@#$%^&*()_+-=[]{};:\"'\"',rn.<>?\\|");
                        builder.push(", ");
                        builder.push_bind("12345678");
                        builder.push(", ");
                        builder.push_bind("male");
                        builder.push(", ");
                        builder.push_bind(true);
                        builder.push(", ");
                        builder.push_bind(1);
                        builder.push(", ");
                        builder.push_bind(123);
                        builder.push(", ");
                        builder.push_bind(12345);
                        builder.push(", ");
                        builder.push_bind(1677215);
                        builder.push(", ");
                        builder.push_bind(26772156);
                        builder.push(", ");
                        builder.push_bind(467721565);
                        builder.push(", ");
                        builder.push_bind(0.99);
                        builder.push(", ");
                        builder.push_bind(0.9999);
                        builder.push(", ");
                        builder.push_bind("3333.50");
                        builder.push(", ");
                        builder.push_bind(2023);
                        builder.push(", ");
                        builder.push_bind("2023-06-16");
                        builder.push(", ");
                        builder.push_bind("12:01:01");
                        builder.push(", ");
                        builder.push_bind("2023-02-16 14:14:14");
                        builder.push(", ");
                        builder.push_bind("2023-06-16 23:59:59");
                        builder.push(", ");
                        builder.push_bind::<Option<Vec<u8>>>(None);
                        builder.push(", ");
                        builder.push_bind::<Option<Vec<u8>>>(None);
                        builder.push(", ");
                        builder.push_bind::<Option<Vec<u8>>>(None);
                        builder.push(", ");
                        builder.push_bind("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
                        builder.push(", ");
                        builder.push_bind("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
                        builder.push(")");
                        if i < _batch - 1 {
                            builder.push(", ");
                        }
                    }
                    let query = builder.build();
                    let sql = query.sql();
                    let _res = query.execute(pool).await;
                    match _res {
                        Ok(r) => {},
                        Err(e) => {
                            println!("Error {} while insert table={}, sql=[{}]", e.as_database_error().unwrap().to_string(), table, sql);
                        }
                    }
                    let _end = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
                    let _t = 1000 - (_end - _begin);
                    if(rate > 0 && _t > 0) {
                        tokio::time::sleep(tokio::time::Duration::from_millis(_t as u64)).await;
                    }
                    bar.inc(_batch as u64);
                }

                let end = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
                let message = format!("count={}, batch={}, expect_rate={}/s, actual_rate={}/s, begin={}, end={}, cost={}s", count, _batch, rate, (count as f32 / ((end  - begin) as f32) as f32 * 1000.00) as u32 , begin, end, ((end  - begin) as f32 / 1000.00));
                bar.finish_and_clear();
                println!("{} {} {}", style("[Done]").bold().dim(), SPARKLE, message);
                Ok(())
            }
        },
        _ => {
            println!("t_types_test");
            Ok(())
        }
    }
}

async fn get_table_count(pool: &Pool<MySql>, table_name: &str) -> Result<u64, sqlx::Error> {
    let query = format!("SELECT COUNT(*) FROM {}", table_name);
    let count: i64 = sqlx::query_scalar(&query).fetch_one(pool).await?;
    Ok(count as u64)
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



async fn test() -> Result<()> {
    println!("Hello world!");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    Ok(())
}

/*
#[tokio::main]
async fn main5() -> Result<(), Box<dyn Error>> {
    let pool = Pool::<MySql>::connect("mysql://root:wyx1005@localhost/database_name").await?;


    let pool_for_count = pool.clone();
    tokio::spawn(async move {
        loop {
            match get_table_count(&pool_for_count, "t_types_test").await {
                Ok(count) => println!("Current count: {}", count),
                Err(e) => eprintln!("Error querying table count: {}", e),
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    /// ... 批量插入数据的代码 ...

    Ok(())
}

 */
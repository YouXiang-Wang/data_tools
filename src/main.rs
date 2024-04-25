use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use anyhow::Result;
use chrono::{format, Utc, DateTime, Local, NaiveDateTime};
use clap::Parser;
use sqlx::mysql::MySqlConnectOptions;
use sqlx::{ConnectOptions, Transaction, Execute, MySql, MySqlPool, Pool, query};
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
use std::borrow::Cow;
extern crate conv;
use conv::ValueFrom;
use log::info;
use rand::{distributions::Alphanumeric};
use std::collections::HashSet;
use rand::{SeedableRng};
use rand::rngs::StdRng;
use snowflake::SnowflakeIdGenerator;
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

    #[arg(short, long, default_value = "")]
    password: String,

    #[arg(long, default_value = "binlog_test")]
    database: String,

    #[arg(long, default_value = "")]
    //#[arg(long, default_value = "/tmp/mysql.sock")]
    socket: String,

    //HostRecords
    //t_types_test
    #[arg(long, default_value = "HostRecords")]
    table: String,

    #[arg(long, default_value = "NCMReplicaGroup")]
    replica_group: String,

    #[arg(long, default_value = "FNVXCATM23001129")]
    sn: String,

    #[arg(short, long, default_value_t = 100)]
    count: u32,

    #[arg(short, long, default_value_t = 100)]
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



struct HostRecords {
    uid: String,
    adapters: Option<String>,
    agentID: Option<String>,
    agentPlatform: Option<String>,
    agentSN: Option<String>,
    agentTag: Option<String>,
    agentVersion: Option<String>,
    applications: Option<String>,
    attributes: Option<String>,
    caDbId: Option<u64>,
    creationTime: Option<u64>,
    criticality: Option<String>,
    directoryPolicyValue: Option<String>,
    disable: Option<u8>,
    domainId: Option<u64>,
    hardwareType: Option<String>,
    hostName: Option<String>,
    imageType: Option<String>,
    landscape: Option<i64>,
    lastChangeSummary: Option<String>,
    lastModifiedBy: Option<String>,
    lastModifiedDate: Option<String>,
    lastReValidation: Option<u64>,
    lastSuccessfulPoll: Option<u64>,
    loggedOnUserId: Option<String>,
    managedByMDM: Option<u8>,
    mdmCompliance: Option<u8>,
    mdmCompromised: Option<u8>,
    mdmDataProtection: Option<u8>,
    mdmPasscodePresent: Option<u8>,
    notes: Option<String>,
    offlineTime: Option<u64>,
    openPorts: Option<String>,
    os: Option<String>,
    owner: Option<String>,
    patchManagementID: Option<String>,
    patchManagementVendor: Option<String>,
    policy: Option<String>,
    reValidation: Option<u8>,
    reValidationInterval: Option<u64>,
    role: Option<String>,
    serialNumber: Option<String>,
    sourceCA: Option<String>,
    state: Option<u8>,
    status: Option<u32>,
    _type: Option<u8>,
    validForTime: Option<u64>,
    validForTimeOffLine: Option<u64>,
    versionForHost: Option<f64>,
    versionForReValidation: Option<f64>,
    vulScanDate: Option<u64>,
    vulScanStatus: Option<i32>
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
    let socket = &args.socket;
    let db_url = match socket.as_str() {
        "" => {
            format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, database)
        },
        _ => {
            //format!("mysql://{}:{}@{}:{}/{}?socket={}", user, password, host, port, database, socket)
            //format!("mysql://{}@unix({})/{}", user, socket, database)
            String::from("mysql://wangyoux@unix(/tmp/mysql.sock)/binlog_test")

        }
    };

    //MySqlPool::connect(&db_url).await.expect("Failed to connect to MySQL.")

    let port = convert_string_to_u16(port, 3306);

    let pool = {
        match socket.as_str() {
            "" => {
                println!("{} {} Building database connections for host=[{}], port=[{}], database=[{}], user=[{}], password=[{}], table=[{}]", style("[2/4]").bold().dim(), DATABASE, host, port, database, user, password, table);
                MySqlConnectOptions::new()
                    .host(host)
                    .port(port)
                    .username(user)
                    .password(password)
                    .database(database)
                    .log_statements(log::LevelFilter::Trace);

                MySqlPool::connect(&db_url).await.expect("Failed to connect to MySQL.")


            },
            _ => {
                println!("{} {} Building database connections for host=[{}], port=[{}], database=[{}], socket=[{}], table=[{}]", style("[2/4]").bold().dim(), DATABASE, host, port, database, socket, table);
                let conn_options = MySqlConnectOptions::new()
                    .username(user)
                    .database(database)
                    .socket(socket)
                    .log_statements(log::LevelFilter::Trace);

                MySqlPool::connect_with(conn_options).await.expect("Failed to connect to MySQL.")
            }
        }

    };

    let pool_for_insert = pool.clone();
    let pool_for_count = pool.clone();

    let current_tbl_count = get_table_count(&pool_for_count, table).await?;
    let should_final_count = current_tbl_count as u64 + count as u64;


    let blue = Style::new().blue();
    let green = Style::new().green();

    println!("{} {} Current {} record count = {}, final count = {}", style("[3/4]").bold().dim(), TABLE, table, blue.apply_to(current_tbl_count), green.apply_to(should_final_count));

    /*

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

    let styles = [
        ("Rough bar:", "█  ", "red"),
        ("Fine bar: ", "█▉▊▋▌▍▎▏  ", "yellow"),
        ("Vertical: ", "█▇▆▅▄▃▂▁  ", "green"),
        ("Fade in:  ", "█▓▒░  ", "blue"),
        ("Blocky:   ", "█▛▌▖  ", "magenta"),
    ];

    let len = peers.len();
    let progress = for (index, r)  in peers.iter().enumerate() {
        let _key_1 = format!("{}:{}", r.host, r.port);
        let _key_2 = format!("{}:{}", r.host, r.port);
        let _key_3 = format!("{}:{}", r.host, r.port);
        vec.push((_key_1, styles.get(index % len).unwrap_or(&(_key_2.as_str(), "█  ", "red")).2.to_string(), "".to_string()));
    };

    let m = MultiProgress::new();



    for (a, b,c) in vec {
        let pb = m.add(ProgressBar::new(should_final_count));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(&format!("{{prefix:.bold}}▕{{bar:.{}}}▏{{msg}}", c))
                .progress_chars(b.as_str()),
        );
        pb.set_prefix(a);
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
    }

     */


    let reference = "hello".trim();
    let reference = &String::from("hello");



    let insert_thread = tokio::spawn(async move {
        insert_into(&args, &pool_for_insert).await;
    });

    insert_thread.await.unwrap();

    //let bar = ProgressBar::new(count as u64);
    //m.add(bar);
    //m.join().unwrap();


    Ok(())
}

fn convert_string_to_u16(input: &String, default: u16) -> u16 {
    match input.parse::<u16>() {
        Ok(num) => num,
        Err(e) => {
            eprintln!("Error parsing string to u16: {}", e);
            default
        }
    }
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

fn generate_mac(rng: &mut impl Rng, count: usize, generated_macs: &mut HashSet<String>) -> Vec<String> {
    let mut macs = Vec::new();
    while macs.len() < count {
        let octets = (0..6).map(|_| rng.gen_range(0..256)).collect::<Vec<_>>();
        let mac = octets.iter().map(|x| format!("{:02x}", x)).collect::<Vec<_>>().join(":");
        if generated_macs.insert(mac.clone()) {
            macs.push(mac.to_uppercase());
        }
    }
    macs
}

async fn insert_into(args: &Args, pool: &Pool<MySql>) -> Result<()> {
    let database = &args.database;
    let table = &args.table;
    let full_table = format!("{}.`{}`", database, &args.table);
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

    if(count == 0 || batch == 0) {
        Ok(())
    } else {
        let round = if(count % _batch == 0) {count / _batch} else {count / _batch + 1};
        let _style = ProgressStyle::default_bar()
            .template(&format!("{{prefix:.bold}}▕{{bar:.{}}}▏{{msg}}", "red"))
            .progress_chars("Inserting");
        let bar = ProgressBar::new(count as u64);
        match table.as_str() {
            "t_types_test" => {
                for i in 0..round {
                    let mut builder = QueryBuilder::new(format!("INSERT INTO {}(id1, id2, id3, gender, bool_1, bit_1, int_tiny, int_small, int_medium, int_int, int_big, pay1, pay2, pay3, latest_year, latest_date, latest_time, latest_datetime, latest_timestamp, blob_tiny, blob_blob, blob_medium, text1, long_text) VALUES ", full_table));
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
                            println!("Error {} while insert table={}, sql=[{}]", e.as_database_error().unwrap().to_string(), full_table, sql);
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
            },

            "HostRecords" => {
                let sn = &args.sn;
                //let mut rng = rand::thread_rng();
                let mut id_generator_generator = SnowflakeIdGenerator::new(1, 1);
                let mut rng = StdRng::from_entropy();
                let mut generated_macs: HashSet<String> = HashSet::new();
                for i in 0..round {
                    let mut transaction = pool.begin().await?;
                    //let query = sqlx::query("INSERT INTO table1 (column_name) VALUES (?)");
                    let mut builder = QueryBuilder::new(format!("INSERT INTO {}(uid, adapters, agentID, agentPlatform, agentSN, agentTag, agentVersion, applications, `attributes`, caDbId, creationTime, criticality, directoryPolicyValue, disable, domainId, hardwareType, hostName, imageType, landscape, lastChangeSummary, lastModifiedBy, lastModifiedDate, lastReValidation, lastSuccessfulPoll, loggedOnUserId, managedByMDM, mdmCompliance, mdmCompromised, mdmDataProtection, mdmPasscodePresent, notes, offlineTime, openPorts, os, owner, patchManagementID, patchManagementVendor, policy, reValidation, reValidationInterval, `role`, serialNumber, sourceCA, state, status, `type`, validForTime, validForTimeOffLine, versionForHost, versionForReValidation, vulScanDate, vulScanStatus) VALUES ", full_table));
                    let _begin = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_millis();
                    bar.set_prefix(format!("[{}/?]", i + 1));
                    let mac_addresses = generate_mac(&mut rng, _batch as usize, &mut generated_macs);
                    for i in 0.._batch {
                        let time_mills = Utc::now().timestamp_millis() as u64;
                        builder.push("(");
                        let snowflake_id = id_generator_generator.real_time_generate();
                        let _id = format!("{}_{}", sn, snowflake_id.to_string());

                        //uid
                        builder.push_bind(_id);
                        builder.push(", ");

                        let _adapters = format!("[{}]", mac_addresses[i as usize]);
                        //adapters
                        builder.push_bind(_adapters);
                        builder.push(", ");

                        //agentID
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //agentPlatform
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //agentSN
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //agentTag
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //agentVersion
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //applications
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //attributes
                        builder.push_bind("[]");
                        builder.push(", ");

                        //caDbId
                        builder.push_bind(snowflake_id);
                        builder.push(", ");

                        //creationTime
                        builder.push_bind(time_mills);
                        builder.push(", ");

                        //criticality
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //directoryPolicyValue
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //disable
                        builder.push_bind(0);
                        builder.push(", ");

                        //domainId
                        builder.push_bind(-1);
                        builder.push(", ");

                        //hardwareType
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //hostName
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //imageType
                        builder.push_bind("");
                        builder.push(", ");

                        //landscape
                        builder.push_bind(345052111452i64);
                        builder.push(", ");

                        //lastChangeSummary
                        builder.push_bind("");
                        builder.push(", ");

                        //lastModifiedBy
                        builder.push_bind("SYSTEM");
                        builder.push(", ");

                        //lastModifiedDate
                        builder.push_bind(Local::now().format("%Y-%m-%d %H:%M:%S.%3f").to_string());
                        builder.push(", ");

                        //lastReValidation
                        builder.push_bind(0);
                        builder.push(", ");

                        //lastSuccessfulPoll
                        builder.push_bind(0);
                        builder.push(", ");

                        //loggedOnUserId
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //managedByMDM
                        builder.push_bind(0);
                        builder.push(", ");

                        //mdmCompliance
                        builder.push_bind(0);
                        builder.push(", ");

                        //mdmCompromised
                        builder.push_bind(0);
                        builder.push(", ");

                        //mdmDataProtection
                        builder.push_bind(0);
                        builder.push(", ");

                        //mdmPasscodePresent
                        builder.push_bind(0);
                        builder.push(", ");

                        //notes
                        builder.push_bind("");
                        builder.push(", ");

                        //offlineTime
                        builder.push_bind(0);
                        builder.push(", ");

                        //openPorts
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //os
                        builder.push_bind("Linux Debian");
                        builder.push(", ");

                        //owner
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //patchManagementID
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //patchManagementVendor
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //policy
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //reValidation
                        builder.push_bind(0);
                        builder.push(", ");

                        //reValidationInterval
                        builder.push_bind(0);
                        builder.push(", ");

                        //role
                        builder.push_bind("NAC-Default");
                        builder.push(", ");

                        //serialNumber
                        builder.push_bind::<Option<String>>(None);
                        builder.push(", ");

                        //sourceCA
                        builder.push_bind("NAC-Default");
                        builder.push(", ");

                        //state
                        builder.push_bind(0);
                        builder.push(", ");

                        //status
                        builder.push_bind(0);
                        builder.push(", ");

                        //type
                        builder.push_bind(9);
                        builder.push(", ");

                        //validForTime
                        builder.push_bind(time_mills);
                        builder.push(", ");

                        //validForTimeOffLine
                        builder.push_bind(1209600000);
                        builder.push(", ");

                        //versionForHost
                        builder.push_bind(1.6);
                        builder.push(", ");

                        //versionForReValidation
                        builder.push_bind(1);
                        builder.push(", ");

                        //vulScanDate
                        builder.push_bind(0);
                        builder.push(", ");

                        //vulScanStatus
                        builder.push_bind(-1);
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
                            println!("Error {} while insert table={}, sql=[{}]", e.as_database_error().unwrap().to_string(), full_table, sql);
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
            },
            table => {
                println!("Not support table = {}", table);
                Ok(())
            }
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

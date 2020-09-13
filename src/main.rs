use clap::{crate_description, crate_name, crate_version, value_t_or_exit, App, Arg};
use reqwest::{Client, Response};
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::task::{spawn, JoinHandle};
use tokio::time::{interval, Duration};
use toml;

// Table
const FLAG_TABLE: &str = "CREATE TABLE IF NOT EXISTS flags 
    (id INTEGER PRIMARY KEY, flag TEXT NOT NULL UNIQUE, group_id INT NOT NULL,
    sent BOOLEAN NOT NULL DEFAULT 0)";
const SELECT_UNSENT: &str = "SELECT id, flag, group_id, sent FROM flags WHERE sent = 0";
const UPDATE_SENT: &str = "UPDATE flags SET sent = 1 WHERE id = ?";

#[derive(Debug)]
struct Flag {
    id: i64,
    flag: String,
    group: u8,
    sent: bool,
}

#[derive(Debug, Deserialize)]
struct Config {
    db_path: String,
    server_url: String,
    team_token: String,
    check_interval: u8,
    flags_quota: u8,
}

fn config() -> Arc<Config> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about(crate_description!())
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("CONFIG_PATH")
                .help("Path to the Toml config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db_path")
                .short("D")
                .long("database")
                .value_name("DB_PATH")
                .help("Path to the SQLite database")
                .takes_value(true)
                .default_value("flag.db"),
        )
        .arg(
            Arg::with_name("server_url")
                .short("u")
                .long("url")
                .value_name("SERVER_URL")
                .help("URL of the POST request for challenge server")
                .takes_value(true)
                .required_unless("config"),
        )
        .arg(
            Arg::with_name("team_token")
                .short("t")
                .long("token")
                .value_name("TEAM_TOKEN")
                .help("Team token to score points")
                .takes_value(true)
                .required_unless("config"),
        )
        .arg(
            Arg::with_name("check_interval")
                .short("i")
                .long("interval")
                .value_name("CHECK_INTERVAL")
                .help("Interval for checking new flags in the database")
                .takes_value(true)
                .default_value("10"),
        )
        .arg(
            Arg::with_name("flags_quota")
                .short("f")
                .long("flags_quota")
                .value_name("FLAGS_QUOTA")
                .help("Max number of flags to send to the server per seconds")
                .takes_value(true)
                .default_value("25"),
        )
        .get_matches();

    if matches.is_present("config") {
        let config_file =
            read_config_file(matches.value_of("config").unwrap()).unwrap_or_else(|err| {
                eprintln!("[ERROR][CONFIG] {}", err);
                panic!("read_config_file");
            });
        let mut config: Config = toml::from_str(&config_file).unwrap_or_else(|err| {
            eprintln!("[ERROR][CONFIG] {}", err);
            panic!("toml::from_str");
        });

        if matches.is_present("db_path") {
            config.db_path = String::from(matches.value_of("db_path").unwrap());
        }
        if matches.is_present("server_url") {
            config.server_url = String::from(matches.value_of("server_url").unwrap());
        }
        if matches.is_present("team_token") {
            config.team_token = String::from(matches.value_of("team_token").unwrap());
        }
        if matches.is_present("check_interval") {
            config.check_interval = value_t_or_exit!(matches.value_of("check_interval"), u8);
        }
        if matches.is_present("flags_quota") {
            config.flags_quota = value_t_or_exit!(matches.value_of("flags_quota"), u8);
        }

        return Arc::new(config);
    }
    Arc::new(Config {
        db_path: String::from(matches.value_of("db_path").unwrap()),
        server_url: String::from(matches.value_of("server_url").unwrap()),
        team_token: String::from(matches.value_of("team_token").unwrap()),
        check_interval: value_t_or_exit!(matches.value_of("check_interval"), u8),
        flags_quota: value_t_or_exit!(matches.value_of("flags_quota"), u8),
    })
}

fn read_config_file(path: &str) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

fn setup(db: &Connection) -> rusqlite::Result<()> {
    // Create table flag
    db.execute(FLAG_TABLE, params![])?;
    Ok(())
}

fn get_unsent_flags(db: &Connection) -> Result<Vec<Arc<Flag>>, rusqlite::Error> {
    // Prepare query for select unsent flags
    let mut prepare = db.prepare(SELECT_UNSENT)?;
    // Map return to Flag struct
    let flags: Vec<Arc<Flag>> = prepare
        .query_map(params![], |row| {
            Ok(Flag {
                id: row.get(0)?,
                flag: row.get(1)?,
                group: row.get(2)?,
                sent: row.get(3)?,
            })
        })?
        .map(|x| Arc::new(x.unwrap()))
        .collect();
    println!("[DEBUG][GET] flags: {:#?}", flags);
    Ok(flags)
}

fn set_sent_flags(db: &Connection, id: i64) -> rusqlite::Result<()> {
    // Set the flag with the id to sent
    db.execute(UPDATE_SENT, params![id])?;
    Ok(())
}

async fn send_single_flag(
    server_url: &str,
    team_token: &str,
    flag: &Flag,
) -> Result<Response, reqwest::Error> {
    println!("[SEND] flag: {} group: {}", flag.flag, flag.group);
    let client = Client::new();
    // Send team token and flag as post request
    let parameters = [("team_token", team_token), ("flag", &flag.flag)];
    let res = client.post(server_url).form(&parameters).send().await?;
    Ok(res)
}

async fn check_response(res: Response) -> bool {
    // Check if status is success and if response tells that flag is invalid (false positives)
    if res.status().is_success() {
        match res.text().await {
            Ok(text) => {
                return !text.contains("invalid");
            }
            Err(err) => {
                eprintln!("[ERROR][CHECK] {}", err);
            }
        }
    } else {
        eprintln!(
            "[ERROR][CHECK] Response not successful, status code {}",
            res.status()
        );
    }
    false
}

async fn send_flags_with_throttle(
    sent_set: &Arc<Mutex<HashSet<i64>>>,
    flags: &Vec<Arc<Flag>>,
    config: &Arc<Config>,
) -> Vec<JoinHandle<()>> {
    // Returned vec of all the task spawned
    let mut joins: Vec<JoinHandle<()>> = Vec::with_capacity(flags.len());
    // Throttle time to lower requests per second
    let mut interval = interval(Duration::from_secs(1));

    // Send FLAGS_PER_SECOND before waiting the throttle time
    for chunk in flags.chunks(config.flags_quota as usize) {
        interval.tick().await;
        for flag in chunk {
            let sent_set = Arc::clone(sent_set);
            let flag = Arc::clone(flag);
            let config = Arc::clone(config);
            let join = spawn(async move {
                match send_single_flag(&config.server_url, &config.team_token, &flag).await {
                    Ok(res) => {
                        // If flag was sent successfully we add it to the set to
                        // be set as sent afterwards
                        if check_response(res).await {
                            let mut hash_set = sent_set.lock().unwrap();
                            hash_set.insert(flag.id);
                        }
                    }
                    Err(err) => eprintln!("[ERROR][SEND] {}", err),
                }
            });
            joins.push(join);
        }
    }
    // Return join for the tasks
    joins
}

async fn main_loop(db: &Connection, config: &Arc<Config>) {
    // Interval for checking flags to sent
    let mut interval = interval(Duration::from_secs(config.check_interval as u64));
    // Set of all the sent flags
    let sent_set: Arc<Mutex<HashSet<i64>>> = Arc::new(Mutex::new(HashSet::new()));

    loop {
        interval.tick().await;
        match get_unsent_flags(db) {
            Ok(flags) => {
                // Send all the flags and wait for all threads to finish
                let joins = send_flags_with_throttle(&sent_set, &flags, config).await;
                for join in joins {
                    if let Err(err) = join.await {
                        eprintln!("[ERROR][JOIN] {}", err);
                    }
                }
            }
            Err(err) => eprintln!("[ERROR][GET] {}", err),
        }
        // Update all the sent flags
        let mut hash_set = sent_set.lock().unwrap();
        for id in hash_set.drain() {
            if let Err(err) = set_sent_flags(db, id) {
                eprintln!("[ERROR][SET] {}", err);
            }
        }
    }
}

#[tokio::main]
async fn main() -> rusqlite::Result<()> {
    let config = config();
    let db = Connection::open(&config.db_path)?;
    setup(&db)?;
    main_loop(&db, &config).await;
    Ok(())
}

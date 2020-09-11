use reqwest;
use rusqlite::params;
use std::sync::{Arc, Mutex};
use tokio::time;

const DB_PATH: &str = "flag.db";
const SERVER_URL: &str = "<server_url>";
const TEAM_TOKEN: &str = "<team_token>";
const SLEEP_TIME: u8 = 10;
const FLAGS_PER_SECOND: u8 = 25;
// Table
const FLAG_TABLE: &str = "CREATE TABLE IF NOT EXISTS flags (flag TEXT PRIMARY KEY, group_id INT NOT NULL, sent BOOLEAN NOT NULL DEFAULT 0)";
const SELECT_UNSENT: &str = "SELECT flag, group_id, sent FROM flags WHERE sent = 0";
const UPDATE_SENT: &str = "UPDATE flags SET sent = 1 WHERE flag = ?";

#[derive(Debug)]
struct Flag {
    flag: String,
    group: u8,
    sent: bool,
}

fn setup(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    db.execute(FLAG_TABLE, params![])?;
    Ok(())
}

fn get_unsent_flags(db: &rusqlite::Connection) -> Result<Vec<Arc<Flag>>, rusqlite::Error> {
    let mut prepare = db.prepare(SELECT_UNSENT)?;
    let flags: Vec<Arc<Flag>> = prepare
        .query_map(params![], |row| {
            Ok(Flag {
                flag: row.get(0)?,
                group: row.get(1)?,
                sent: row.get(2)?,
            })
        })?
        .map(|x| Arc::new(x.unwrap()))
        .collect();
    println!("[GET] flags: {:?}", flags);
    Ok(flags)
}

// TODO: Use batch insert for all sent flags
fn set_sent_flag(db: &rusqlite::Connection, flag: &Flag) -> rusqlite::Result<()> {
    db.execute(UPDATE_SENT, params![flag.flag])?;
    Ok(())
}

async fn send_single_flag(flag: &Flag) -> Result<reqwest::Response, reqwest::Error> {
    println!("[SEND] flag: {} group: {}", flag.flag, flag.group);
    let client = reqwest::Client::new();
    let parameters = [("team_token", TEAM_TOKEN), ("flag", &flag.flag)];
    let res = client.post(SERVER_URL).form(&parameters).send().await?;
    Ok(res)
}

fn check_response(_res: reqwest::Response) -> bool {
    // TODO: Check response
    false
}

async fn send_flags_with_throttle(db: &Arc<Mutex<rusqlite::Connection>>, flags: &Vec<Arc<Flag>>) {
    let mut interval = time::interval(time::Duration::from_secs(1));
    for chunk in flags.chunks(FLAGS_PER_SECOND as usize) {
        interval.tick().await;
        for flag in chunk {
            let db = Arc::clone(db);
            let flag = Arc::clone(flag);
            tokio::task::spawn(async move {
                if let Ok(res) = send_single_flag(&flag).await {
                    if check_response(res) {
                        if let Err(err) = set_sent_flag(&db.lock().unwrap(), &flag) {
                            eprintln!("[ERROR][SET] {}", err);
                        };
                    }
                }
            });
        }
    }
}

async fn main_loop(db: &Arc<Mutex<rusqlite::Connection>>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(SLEEP_TIME as u64));
    loop {
        match get_unsent_flags(&db.lock().unwrap()) {
            Ok(flags) => {
                send_flags_with_throttle(db, &flags).await;
            }
            Err(err) => eprintln!("[ERROR][GET] {}", err),
        }
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> rusqlite::Result<()> {
    let db = Arc::new(Mutex::new(rusqlite::Connection::open(DB_PATH)?));
    setup(&db.lock().unwrap())?;
    main_loop(&db).await;
    Ok(())
}

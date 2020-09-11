use reqwest;
use rusqlite::params;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::time;

const DB_PATH: &str = "flag.db";
const SERVER_URL: &str = "http://localhost";
const TEAM_TOKEN: &str = "<team_token>";
const SLEEP_TIME: u8 = 10;
const FLAGS_PER_SECOND: u8 = 25;
// Table
const FLAG_TABLE: &str = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY, flag TEXT NOT NULL UNIQUE, group_id INT NOT NULL, sent BOOLEAN NOT NULL DEFAULT 0)";
const SELECT_UNSENT: &str = "SELECT id, flag, group_id, sent FROM flags WHERE sent = 0";
const UPDATE_SENT: &str = "UPDATE flags SET sent = 1 WHERE id = ?";

#[derive(Debug, Eq, PartialEq, Hash)]
struct Flag {
    id: i64,
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
                id: row.get(0)?,
                flag: row.get(1)?,
                group: row.get(2)?,
                sent: row.get(3)?,
            })
        })?
        .map(|x| Arc::new(x.unwrap()))
        .collect();
    println!("[GET] flags: {:?}", flags);
    Ok(flags)
}

fn set_sent_flags(db: &rusqlite::Connection, id: i64) -> rusqlite::Result<()> {
    db.execute(UPDATE_SENT, params![id])?;
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

async fn send_flags_with_throttle(
    db: &Arc<Mutex<rusqlite::Connection>>,
    sent_set: &Arc<Mutex<HashSet<i64>>>,
    flags: &Vec<Arc<Flag>>,
) {
    let mut interval = time::interval(time::Duration::from_secs(1));
    for chunk in flags.chunks(FLAGS_PER_SECOND as usize) {
        interval.tick().await;
        for flag in chunk {
            let sent_set = Arc::clone(sent_set);
            let flag = Arc::clone(flag);
            tokio::task::spawn(async move {
                match send_single_flag(&flag).await {
                    Ok(res) => {
                        if check_response(res) {
                            let mut hash_set = sent_set.lock().unwrap();
                            hash_set.insert(flag.id);
                        }
                    }
                    Err(err) => eprintln!("[ERROR][SEND] {}", err),
                }
            });
        }
    }
    let mut hash_set = sent_set.lock().unwrap();
    let db = db.lock().unwrap();
    for id in hash_set.drain() {
        if let Err(err) = set_sent_flags(&db, id) {
            eprintln!("[ERROR][SET] {}", err);
        }
    }
}

async fn main_loop(db: &Arc<Mutex<rusqlite::Connection>>, sent_set: &Arc<Mutex<HashSet<i64>>>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(SLEEP_TIME as u64));
    loop {
        match get_unsent_flags(&db.lock().unwrap()) {
            Ok(flags) => {
                send_flags_with_throttle(db, sent_set, &flags).await;
            }
            Err(err) => eprintln!("[ERROR][GET] {}", err),
        }
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> rusqlite::Result<()> {
    let db = Arc::new(Mutex::new(rusqlite::Connection::open(DB_PATH)?));
    let sent_set: Arc<Mutex<HashSet<i64>>> = Arc::new(Mutex::new(HashSet::new()));
    setup(&db.lock().unwrap())?;
    main_loop(&db, &sent_set).await;
    Ok(())
}

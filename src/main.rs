use reqwest;
use rusqlite::params;
use std::sync::Arc;
use tokio::time;

const DB_PATH: &str = "flag.db";
const SERVER_URL: &str = "<server_url>";
const TEAM_TOKEN: &str = "<team_token>";
const SLEEP_TIME: u8 = 10;
const FLAGS_PER_SECOND: u8 = 25;
// Table
const FLAG_TABLE: &str = "CREATE TABLE IF NOT EXISTS flags (flag TEXT PRIMARY KEY, group_id INT NOT NULL, sent BOOLEAN NOT NULL DEFAULT 0)";

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
    let mut prepare = db.prepare("SELECT flag, group_id, sent FROM flags WHERE sent = 0")?;
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

async fn send_single_flag(flag: Arc<Flag>) -> Result<reqwest::Response, reqwest::Error> {
    println!("[SEND] flag: {} group: {}", flag.flag, flag.group);
    let client = reqwest::Client::new();
    let parameters = [("team_token", TEAM_TOKEN), ("flag", &flag.flag)];
    let res = client.post(SERVER_URL).form(&parameters).send().await?;
    Ok(res)
}

async fn send_flags_with_throttle(flags: &Vec<Arc<Flag>>) {
    let mut interval = time::interval(time::Duration::from_secs(1));
    for chunk in flags.chunks(FLAGS_PER_SECOND as usize) {
        interval.tick().await;
        for flag in chunk {
            let f = Arc::clone(flag);
            println!("Start thread {:?}", f);
            tokio::task::spawn(async { send_single_flag(f).await });
        }
    }
}

async fn main_loop(db: &rusqlite::Connection) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(SLEEP_TIME as u64));
    loop {
        match get_unsent_flags(&db) {
            Ok(flags) => {
                send_flags_with_throttle(&flags).await;
            }
            Err(err) => eprintln!("{}", err),
        }
        interval.tick().await;
    }
}

#[tokio::main]
async fn main() -> rusqlite::Result<()> {
    let db = rusqlite::Connection::open(DB_PATH)?;
    setup(&db)?;
    main_loop(&db).await;
    Ok(())
}

use rusqlite::{Connection, Result};

const DB_PATH: &str = "flag.db";
const FLAG_TABLE: &str =
    "CREATE TABLE flags IF NOT EXISTS (flag TEXT PRIMARY KEY, group INT NOT NULL, sent BOOLEAN NOT NULL DEFAULT 0)";

#[derive(Debug)]
struct Flag<'a> {
    flag: &'a str,
    group: u8,
    sent: bool,
}

fn setup() {}

fn main() -> Result<()> {
    let db = Connection::open(DB_PATH)?;
    println!("{}", db.is_autocommit());
    Ok(())
}

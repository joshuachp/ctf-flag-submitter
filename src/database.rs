use super::{Flag, FLAG_STATUS};

use postgres;
use rusqlite;
use std::collections::HashSet;
use std::sync::Arc;

// Queries
// Crete table flags(id, flag, group_id, status, received_time, sent_time) the
// received_time/sent_time is set automatically throw a SQL query and is not
// used by the application but only for debug
const FLAG_TABLE_SQLITE: &str = "CREATE TABLE IF NOT EXISTS flags 
    (id INTEGER PRIMARY KEY, flag TEXT NOT NULL UNIQUE, group_id INT NOT NULL,
    status INT2 NOT NULL DEFAULT 0 CHECK (status < 4),
    received_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, sent_time TEXT)";
const FLAG_TABLE_POSTGRESQL: &str = "CREATE TABLE IF NOT EXISTS flags 
    (id SERIAL PRIMARY KEY, flag TEXT NOT NULL UNIQUE, group_id INT NOT NULL,
    status INT2 NOT NULL DEFAULT 0 CHECK (status < 4),
    received_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, sent_time TEXT)";
// Get all the flags with status unsent
const SELECT_UNSENT: &str = "SELECT id, flag, group_id, status FROM flags WHERE status = 0";
// Set the flag status to sent and update sent_time stamp
const UPDATE_SENT: &str = "UPDATE flags SET status = 1,
    sent_time = CURRENT_TIMESTAMP WHERE id = ?";
// Set the flag status to invalid
const UPDATE_INVALID: &str = "UPDATE flags SET status = 2 WHERE id = ?";

pub struct Sqlite {
    pub db: rusqlite::Connection,
}

pub struct Postgres {
    pub db: postgres::Client,
}

pub trait Database<T> {
    fn setup(&mut self) -> Result<(), T>;
    fn get_unsent_flags(&mut self) -> Result<Vec<Arc<Flag>>, T>;
    fn set_sent_flags(&mut self, sent_set: &mut HashSet<i64>) -> Result<(), T>;
    fn set_invalid_flags(&mut self, invalid_set: &mut HashSet<i64>) -> Result<(), T>;
}

impl Database<rusqlite::Error> for Sqlite {
    fn setup(&mut self) -> rusqlite::Result<()> {
        println!("[SETUP] Creating SQLite tables");
        // Create table flag
        &self.db.execute_batch(FLAG_TABLE_SQLITE)?;
        Ok(())
    }

    fn get_unsent_flags(&mut self) -> rusqlite::Result<Vec<Arc<Flag>>> {
        // Prepare query for select unsent flags
        let mut prepare = self.db.prepare(SELECT_UNSENT)?;
        // Map return to Flag struct
        let flags: Vec<Arc<Flag>> = prepare
            .query_map(rusqlite::params![], |row| {
                let status: i32 = row.get(3)?;
                Ok(Arc::new(Flag {
                    id: row.get(0)?,
                    flag: row.get(1)?,
                    group: row.get(2)?,
                    status: FLAG_STATUS[status as usize],
                }))
            })?
            .map(|x| x.unwrap())
            .collect();
        println!("[GET] flags: {:#?}", flags);
        Ok(flags)
    }

    fn set_sent_flags(&mut self, flag_set: &mut HashSet<i64>) -> rusqlite::Result<()> {
        let transaction = self.db.transaction()?;
        for id in flag_set.drain() {
            println!("[SET] Set flag with id {} as sent", id);
            // Set the flag with the id to sent
            transaction.execute(UPDATE_SENT, rusqlite::params![id])?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn set_invalid_flags(&mut self, invalid_set: &mut HashSet<i64>) -> rusqlite::Result<()> {
        let transaction = self.db.transaction()?;
        for id in invalid_set.drain() {
            println!("[SET] Set flag with id {} as invalid", id);
            // Set the flag with the id to sent
            transaction.execute(UPDATE_INVALID, rusqlite::params![id])?;
        }
        transaction.commit()?;
        Ok(())
    }
}

impl Database<postgres::Error> for Postgres {
    fn setup(&mut self) -> Result<(), postgres::Error> {
        println!("[SETUP] Creating SQLite tables");
        // Create table flag
        &self.db.execute(FLAG_TABLE_POSTGRESQL, &[])?;
        Ok(())
    }

    fn get_unsent_flags(&mut self) -> Result<Vec<Arc<Flag>>, postgres::Error> {
        // Map return to Flag struct
        let flags: Vec<Arc<Flag>> = self
            .db
            .query(SELECT_UNSENT, &[])?
            .iter()
            .map(|row| {
                let status: i32 = row.get(3);
                Arc::new(Flag {
                    id: row.get(0),
                    flag: row.get(1),
                    group: row.get(2),
                    status: FLAG_STATUS[status as usize],
                })
            })
            .collect();
        println!("[GET] flags: {:#?}", flags);
        Ok(flags)
    }

    fn set_sent_flags(&mut self, sent_set: &mut HashSet<i64>) -> Result<(), postgres::Error> {
        let mut transaction = self.db.transaction()?;
        for id in sent_set.drain() {
            println!("[SET] Set flag with id {} as sent", id);
            // Set the flag with the id to sent
            transaction.execute(UPDATE_SENT, &[&id])?;
        }
        transaction.commit()?;
        Ok(())
    }
    fn set_invalid_flags(&mut self, invalid_set: &mut HashSet<i64>) -> Result<(), postgres::Error> {
        let mut transaction = self.db.transaction()?;
        for id in invalid_set.drain() {
            println!("[SET] Set flag with id {} as sent", id);
            // Set the flag with the id to sent
            transaction.execute(UPDATE_INVALID, &[&id])?;
        }
        transaction.commit()?;
        Ok(())
    }
}

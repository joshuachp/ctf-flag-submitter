use super::Flag;

use postgres;
use rusqlite;
use std::collections::HashSet;
use std::sync::Arc;

// Queries
const FLAG_TABLE: &str = "CREATE TABLE IF NOT EXISTS flags 
    (id INTEGER PRIMARY KEY, flag TEXT NOT NULL UNIQUE, group_id INT NOT NULL,
    sent BOOLEAN NOT NULL DEFAULT 0)";
const SELECT_UNSENT: &str = "SELECT id, flag, group_id, sent FROM flags WHERE sent = 0";
const UPDATE_SENT: &str = "UPDATE flags SET sent = 1 WHERE id = ?";

pub struct Sqlite {
    pub db: rusqlite::Connection,
}

pub struct Postgres {
    pub db: postgres::Client,
}

pub trait Database<T> {
    fn setup(&mut self) -> Result<(), T>;
    fn get_unsent_flags(&mut self) -> Result<Vec<Arc<Flag>>, T>;
    fn set_sent_flags(&mut self, flag_set: &mut HashSet<i64>) -> Result<(), T>;
}

impl Database<rusqlite::Error> for Sqlite {
    fn setup(&mut self) -> rusqlite::Result<()> {
        println!("[SETUP] Creating SQLite tables");
        // Create table flag
        &self.db.execute_batch(FLAG_TABLE)?;
        Ok(())
    }

    fn get_unsent_flags(&mut self) -> rusqlite::Result<Vec<Arc<Flag>>> {
        // Prepare query for select unsent flags
        let mut prepare = self.db.prepare(SELECT_UNSENT)?;
        // Map return to Flag struct
        let flags: Vec<Arc<Flag>> = prepare
            .query_map(rusqlite::params![], |row| {
                Ok(Arc::new(Flag {
                    id: row.get(0)?,
                    flag: row.get(1)?,
                    group: row.get(2)?,
                    sent: row.get(3)?,
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
}

impl Database<postgres::Error> for Postgres {
    fn setup(&mut self) -> Result<(), postgres::Error> {
        println!("[SETUP] Creating SQLite tables");
        // Create table flag
        &self.db.execute(FLAG_TABLE, &[])?;
        Ok(())
    }

    fn get_unsent_flags(&mut self) -> Result<Vec<Arc<Flag>>, postgres::Error> {
        // Map return to Flag struct
        let flags: Vec<Arc<Flag>> = self
            .db
            .query(SELECT_UNSENT, &[])?
            .iter()
            .map(|row| {
                Arc::new(Flag {
                    id: row.get(0),
                    flag: row.get(1),
                    group: row.get(2),
                    sent: row.get(3),
                })
            })
            .collect();
        println!("[GET] flags: {:#?}", flags);
        Ok(flags)
    }

    fn set_sent_flags(&mut self, flag_set: &mut HashSet<i64>) -> Result<(), postgres::Error> {
        let mut transaction = self.db.transaction()?;
        for id in flag_set.drain() {
            println!("[SET] Set flag with id {} as sent", id);
            // Set the flag with the id to sent
            transaction.execute(UPDATE_SENT, &[&id])?;
        }
        transaction.commit()?;
        Ok(())
    }
}

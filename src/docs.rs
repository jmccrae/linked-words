use rusqlite;
use std::path::Path;
use std::io::{BufRead,BufReader};
use std::fs::File;
use regex::Regex;
use serde_json;
use flate2::read::GzDecoder;

#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Doc {
    pub id : String,
    pub url : String,
    pub title : String
}

#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct CollectionStatistics {
    pub lang : String,
    pub collection : String,
    pub tokens : u32,
    pub types : u32
}

pub struct DocIndex(String);

impl DocIndex {
    pub fn new<P: AsRef<Path> + Clone + ToString>(p : P) -> Result<DocIndex, DocError> {
        let conn = rusqlite::Connection::open(p.clone())?;
        conn.execute("CREATE TABLE IF NOT EXISTS docs(
            id TEXT NOT NULL UNIQUE,
            lang TEXT NOT NULL,
            collection TEXT NOT NULL,
            json TEXT NOT NULL)", &[])?;
        conn.execute("CREATE TABLE IF NOT EXISTS stats(
            lang TEXT NOT NULL,
            collection TEXT NOT NULL,
            tokens INTEGER NOT NULL,
            type INTEGER NOT NULL)", &[])?;
        Ok(DocIndex(p.to_string()))
    }


    pub fn load<P2: AsRef<Path>>(&self, file : P2, stats_file : P2, 
                                 lang : &str, collection : &str) 
                                -> Result<(),DocError> {
        let mut conn = rusqlite::Connection::open(self.0.clone())?;
        let file = BufReader::new(GzDecoder::new(File::open(file)?)?);
        {
            let tx = conn.transaction()?;
            let line_regex =  Regex::new("([0-9a-z]{16}) <doc id=\"(.*)\" url=\"(.*)\" title=\"(.*)\">").expect("Bad regex");

            for line in file.lines() {
                let line = line?;
                match line_regex.captures(&line) {
                    Some(captures) => {
                        let id = captures.get(1).unwrap().as_str();
                        let json = serde_json::to_string(&Doc {
                            id: captures.get(2).unwrap().as_str().to_owned(),
                            url: captures.get(3).unwrap().as_str().to_owned(),
                            title: captures.get(4).unwrap().as_str().to_owned()
                        })?;
                        tx.execute("INSERT INTO docs VALUES (?, ?, ?, ?)",
                            &[&id.to_string(), &lang.to_string(), 
                              &collection.to_string(), &json])?;
                    },
                    None => {
                        eprintln!("Ignoring line:  {}", line);
                    }
                }
            }
            tx.commit()?;
        }
        let file = BufReader::new(File::open(stats_file)?);
        let stats : CollectionStatistics = serde_json::from_reader(file)?;
        conn.execute("INSERT INTO stats VALUES (?, ?, ?, ?)",
            &[&stats.lang, &stats.collection, &stats.tokens, &stats.types])?;
        Ok(())
    }

    pub fn get(&self, id : &str, lang : &str) -> Result<Doc,DocError> {
        let conn = rusqlite::Connection::open(self.0.clone())?;
        let json : String = conn.query_row(
            "SELECT json FROM docs WHERE id=? AND lang=?", 
            &[&id.clone(), &lang.clone()],
            |row| { row.get(0) })?;
        Ok(serde_json::from_str(&json)?)
    }

}

quick_error! {
    #[derive(Debug)]
    pub enum DocError {
        Io(err : ::std::io::Error) {
            from()
            display("I/O Error {}", err)
            cause(err)
        }
        Json(err : serde_json::Error) {
            from()
            display("Json Error: {}", err)
            cause(err)
        }
        SQLite(err : rusqlite::Error) {
            from()
            display("SQL Error {}", err)
            cause(err)
        }
    }
}

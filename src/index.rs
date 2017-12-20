use fst;
use fst::{MapBuilder,Map};
use std::path::Path;
use std::fs::File;
use std::io::{BufRead,BufReader,BufWriter};
use fst_regex::Regex;
use fst::IntoStreamer;
use fst::Streamer;
use std::str::from_utf8;
use flate2::read::GzDecoder;
use std::collections::HashMap;
use serde_json;

#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct Language {
    pub code : String,
    pub english : String,
    pub native : String
}

pub fn read_languages<P : AsRef<Path>>(json_file : P)
       -> Result<Vec<Language>, IndexError> {
    let json_file = File::open(json_file)?;
    let languages = serde_json::from_reader(json_file)?;
    Ok(languages)
}

pub fn open_all(languages : &Vec<Language>) 
        -> Result<HashMap<String, Vec<Map>>, IndexError> {
    let mut result = HashMap::new();
    for lang in languages {
        let path2 = format!("{}wiki.fst", lang.code);
        let path = Path::new(&path2);
        if path.exists() {
            result.insert(lang.code.clone(), vec![open_index(&path)?]);
        } else {
            eprintln!("No data for {} ({})", lang.english, lang.code);
        }
    }
    Ok(result)
}

pub fn load_file<P : AsRef<Path>>(file_name : P, index : P) -> Result<(), IndexError> {
    let input = BufReader::new(GzDecoder::new(File::open(file_name)?)?);
    let output = BufWriter::new(File::create(index)?);
    let mut map_builder = MapBuilder::new(output)?;

    for line in input.lines() {
        let line = line?;
        let id = u64::from_str_radix(&line[(line.len()-16)..line.len()],16)?;
        let ref content = line[..(line.len() -16)];
        map_builder.insert(content, id).
            unwrap_or_else(|e| { eprintln!("Error with line: {}", e); });
    }
    map_builder.finish()?;
    Ok(())
}

pub fn open_index<P : AsRef<Path>>(index : P) -> Result<Map, IndexError> {
    let set = Map::from_path(index)?;
    Ok(set)
}

pub fn search_index(corpus : &Map, word : &str, offset : usize, limit : usize) -> Result<Vec<(String, u32, u64)>, String> {
    let regex = Regex::new(&format!(".* {} .*", word))
        .map_err(|e| format!("Could not make regex: {}", e))?;
    let mut res_stream = corpus.search(regex).into_stream();
    let mut search_results = Vec::new();
    let mut i = 0;
    while i < offset {
        if let None = res_stream.next() {
            return Ok(Vec::new())
        }
        i += 1;
    }
    i = 0;
    while i < limit {
        if let Some(a) = res_stream.next() {
            let (x,y) = a;
            search_results.push((from_utf8(x)
                                 .map_err(|e| format!("Unicode decode: {}", e))?
                                 .to_string(), y));
        } else {
            break;
        }
        i += 1;
    }
    let mut r = Vec::new();
    for x in search_results {
        let line = (x.0[..(x.0.len() - 8)]).to_string();
        let line_key = u32::from_str_radix(&x.0[(x.0.len()-8)..x.0.len()], 16)
            .map_err(|e| format!("Bad key in line: {}", e))?;
        r.push((line, line_key, x.1));
    }
    Ok(r)

}

pub fn autocomplete_search(corpus : &Map, word : &str, limit : usize) -> 
    Result<Vec<String>, String> {
    let regex = Regex::new(&format!(".* {}.*", word))
        .map_err(|e| format!("Could not make regex: {}", e))?;
    let mut res_stream = corpus.search(regex).into_stream();
    let mut results = Vec::new();
    while results.len() < limit {
        if let Some(a) = res_stream.next() {
            let (x,_) = a;
            let mut sentence = from_utf8(x)
                .map_err(|e| format!("Unicode decode: {}", e))?.to_string();
            let i1 = sentence.find(word).unwrap_or(sentence.len());
            sentence.drain(..i1);
            let i2 = sentence.find(" ").unwrap_or(sentence.len());
            let word = sentence[..i2].to_string();
            if !results.contains(&word) {
                results.push(word.clone());
            }
        } else {
            return Ok(results);
        }
    }
    Ok(results)
}


quick_error! {
    #[derive(Debug)]
    pub enum IndexError {
        Io(err: ::std::io::Error) { 
            from()
            display("I/O error: {}", err)
            cause(err)
        }
        Fst(err: fst::Error) {
            from()
            display("FST error: {}", err)
            cause(err)
        }
        Num(err: ::std::num::ParseIntError) {
            from()
            display("Numeric error: {}", err)
            cause(err)
        }
        Json(err: serde_json::Error) {
            from()
            display("Json error: {}", err)
            cause(err)
        }
    }
}

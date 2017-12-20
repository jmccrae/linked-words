#![feature(plugin,custom_derive)]
#![plugin(rocket_codegen)]
extern crate rocket;
extern crate fst;
extern crate fst_regex;
#[macro_use]
extern crate quick_error;
extern crate rand;
extern crate handlebars;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate rusqlite;
extern crate regex;
extern crate flate2;
extern crate clap;

mod index;
mod docs;

use std::collections::HashMap;
use fst::Map;
use rocket::{State, Response};
use std::io::Cursor;
use handlebars::Handlebars;
//use rocket::http::hyper::header::{Location,CacheDirective,CacheControl};
use rocket::http::{ContentType, Status};
use std::fs::File;
use index::{search_index, autocomplete_search};
use docs::DocIndex;
use clap::{App, Arg, SubCommand};

struct WLLState {
    by_lang : HashMap<String,Vec<Map>>,
    hbars : Handlebars,
    docs : DocIndex,
    lang_selects : String
}

#[derive(Clone,Debug,Serialize)]
struct Page {
    body : String, 
    title : String,
    scripts : String
}

#[derive(Clone,Debug,Serialize)]
struct SearchResultWord {
    word : String,
    focus : bool
}

#[derive(Clone,Debug,Serialize)]
struct SearchResultSentence {
    words : Vec<SearchResultWord>,
    line_id : String,
    doc_id : String,
    lang : String
}

#[derive(Clone,Debug,Serialize)]
struct SearchResults {
    query_word : String,
    matches : Vec<SearchResultSentence>,
    lang : String
}

#[derive(FromForm)]
struct AutocompleteForm {
    term : String
}

#[get("/autocomplete/<lang>?<ac>")]
fn autocomplete<'r>(state : State<WLLState>,
                     lang : String, ac : AutocompleteForm) -> Result<Response<'r>, String> {
    let mut results = Vec::new();
    match state.by_lang.get(&lang) {
        Some(corpora) => {
            for corpus in corpora {
                results.extend(autocomplete_search(&corpus, &ac.term, 10)?);
            }
        },
        None => {}
    }
    let res_str = serde_json::to_string(&results)
        .map_err(|e| format!("Could not convert to JSON: {}", e))?;
    Ok(Response::build()
       .sized_body(Cursor::new(res_str))
       .finalize())
}

#[derive(FromForm,Clone)]
struct OffsetLimit {
    offset : usize,
    limit : usize
}


#[get("/w/<lang>/<word>")]
fn get_word<'r>(state : State<WLLState>,
                lang : String, word : String) -> Result<Response<'r>, String> {
    get_word_offset(state, lang, word, None)
}


#[get("/w/<lang>/<word>?<offset_limit>")]
fn get_word_offset<'r>(state : State<WLLState>,
                lang : String, word : String,
                offset_limit : Option<OffsetLimit>) -> Result<Response<'r>, String> {
    match state.by_lang.get(&lang) {
        Some(corpora) => {
            let mut results = SearchResults {
                query_word: word.clone(),
                matches: Vec::new(),
                lang : lang.clone()
            };
            for corpus in corpora {
                for res in search_index(corpus, &word, 
                                        offset_limit.clone().map(|x| x.offset).unwrap_or(0), 
                                        offset_limit.clone().map(|x| x.limit+1).unwrap_or(21))? {
                    results.matches.push(SearchResultSentence {
                        words: res.0.split(" ").map(|x| 
                           SearchResultWord {
                                word: x.to_string(),
                                focus: x == word
                            }).collect(),
                        line_id: format!("{:08x}", res.1),
                        doc_id: format!("{:16x}", res.2),
                        lang : lang.clone()
                    });
                }
            }
            let result_html = state.hbars.render("search_results", &results)
                .map_err(|e| format!("Could not apply Handlebars: {}", e))?;
            let page = state.hbars.render("page", &Page {
                title: format!("Linked Words - Search Results - {}", word),
                body: result_html,
                scripts: "".to_string()
            }).map_err(|e| format!("Could not apply Handlebars: {}", e))?;
            Ok(Response::build()
                .sized_body(Cursor::new(page))
                .finalize())
        },
        None => {
            Ok(Response::build()
                .status(Status::NotFound)
                .finalize())
        }
    }
}

#[get("/doc/<lang>/<id>")]
fn get_doc<'r>(state : State<WLLState>, id : String, lang : String) 
    -> Result<Response<'r>,String> {
    match state.docs.get(&id, &lang) {
        Ok(doc) => {
            let body = state.hbars.render("docs", &doc)
                .map_err(|e| format!("Could not apply Handlebars: {}", e))?;
            let page = state.hbars.render("page", &Page {
                title: format!("Linked Words - Doc - {}", doc.title),
                body: body,
                scripts: "".to_string()
            }).map_err(|e| format!("Could not apply Handlebars: {}", e))?;
            Ok(Response::build()
               .sized_body(Cursor::new(page))
               .finalize())
        },
        Err(err) => {
            let err_msg = format!("{}", err);
            if err_msg == "SQL Error Query returned no rows" {
                Ok(Response::build()
                   .status(Status::NotFound)
                   .finalize())
            } else {
                eprintln!("{}", err_msg);
                Ok(Response::build()
                   .status(Status::InternalServerError)
                   .finalize())
            }
        }
    }
}

#[get("/static/<resource>")]
fn get_resource<'r>(resource : String) -> Result<Response<'r>, String> {
    if resource == "logo.png" {
        Ok(Response::build()
           .header(ContentType::PNG)
           //.header(CacheControl(vec![CacheDirective::MaxAge(86400u32)]))
           .sized_body(File::open("src/logo.png").unwrap())
           .finalize())
    } else if resource == "style.css" {
        Ok(Response::build()
           .header(ContentType::CSS)
           //.header(CacheControl(vec![CacheDirective::MaxAge(86400u32)]))
           .sized_body(File::open("src/style.css").unwrap())
           .finalize())
    } else if resource == "jquery.autocomplete.min.js" {
        Ok(Response::build()
           .header(ContentType::JavaScript)
           //.header(CacheControl(vec![CacheDirective::MaxAge(86400u32)]))
           .sized_body(File::open("src/jquery.autocomplete.min.js").unwrap())
           .finalize())
    } else {
        Ok(Response::build()
            .status(Status::NotFound)
            .finalize())
    }
}

#[derive(Serialize)]
struct NoData;

fn make_lang_selects(languages : &Vec<index::Language>,
                     corpora : &HashMap<String, Vec<Map>>) -> String {
    let mut selects = String::new();
    for code in corpora.keys() {
        match languages.iter().find(|x| x.code == *code) {
            Some(l) => {
                selects.push_str(&format!("<option value=\"{}\">{} ({})</option>\n",
                                          l.code, l.english, l.native));
            },
            None => {}
        }
    }
    selects
}

#[derive(Clone,Debug,Serialize)]
struct IndexPage {
    langs: String
}

#[get("/")]
fn index<'r>(state : State<WLLState>) -> Result<Response<'r>, String> {
    let page = state.hbars.render("page", &Page {
        title: "Linked Words".to_string(),
        body: state.hbars.render("index", &IndexPage {
            langs: state.lang_selects.clone()
        }).unwrap(),
        scripts: state.hbars.render("index-scripts", &NoData).unwrap()
    }).map_err(|e| format!("Could not apply Handlebars: {}", e))?;
    Ok(Response::build()
       .sized_body(Cursor::new(page))
       .finalize())
}

fn main() {
    let matches = App::new("linked-words")
        .version("0.1")
        .author("John P. McCrae <john@mccr.ae>")
        .about("Web Server for words.linguistic-lod.org")
        .subcommand(SubCommand::with_name("load")
                    .about("Load a corpus collection")
                    .arg(Arg::with_name("name")
                         .help("The name of the collection")
                         .required(true)
                         .index(1))
                    .arg(Arg::with_name("lang")
                         .help("The language of the collection (ISO 639 code)")
                         .required(true)
                         .index(2))
                    .arg(Arg::with_name("nofst")
                         .help("Don't load the FST model")
                         .long("no-fst"))
                    .arg(Arg::with_name("nodb")
                         .help("Don't load the Doc Index")
                         .long("no-docs")))
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("load") {
        let name = matches.value_of("name").expect("No name");
        let lang = matches.value_of("lang").expect("No lang");
        if !matches.is_present("nofst") {
            eprintln!("Loading FST");
            index::load_file(&format!("{}.sorted.gz", name), 
                             &format!("{}.fst", name))
                .unwrap_or_else(|e| panic!(format!("{}", e)));
        }
        
        if !matches.is_present("nodb") {
            eprintln!("Loading Doc Index");
            let doc_index = DocIndex::new("docs.db").unwrap();
            doc_index.load(&format!("{}.docs.gz", name),
                &format!("{}.stats.json", name),
                &lang, &name)
                .unwrap();
        }
    } else {
        let languages = index::read_languages("languages.json")
            .expect("Could not read languages");
        let corpora = index::open_all(&languages)
            .expect("Could not load corpus");

        let doc_index = DocIndex::new("docs.db").unwrap();

        let mut hbars = Handlebars::new();
        hbars.register_template_string("page", include_str!("page.hbs"))
            .expect("Could not load page.hbs");
        hbars.register_template_string("search_results", 
                                       include_str!("search_results.hbs"))
            .expect("Could not load search_results.hbs");
        hbars.register_template_string("index",
                                       include_str!("index.hbs"))
            .expect("Could not load index.hbs");
        hbars.register_template_string("index-scripts",
                                       include_str!("index-scripts.hbs"))
            .expect("Could not load index-scripts.hbs");
        hbars.register_template_string("docs",
                                       include_str!("docs.hbs"))
            .expect("Could not load docs.hbs");

        let lang_selects = make_lang_selects(&languages, &corpora);

        let state = WLLState {
            by_lang: corpora,
            hbars: hbars,
            docs: doc_index,
            lang_selects: lang_selects
        };
        rocket::ignite()
            .manage(state)
            .mount("/", routes![index, get_word, get_word_offset,
                   get_doc,
                   get_resource, autocomplete]).launch();
    }
}

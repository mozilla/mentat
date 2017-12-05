#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

extern crate hyper;
extern crate tokio_core;
extern crate futures;
extern crate serde;
extern crate serde_json;
extern crate mentat_db;
extern crate rusqlite;

use std::io::{self, Write};
use tokio_core::reactor::Core;
use hyper::Client;
use hyper::{Method, Request, Body, StatusCode, Error as HyperError};
use hyper::header::{ContentLength, ContentType};
use futures::{future, Future, Stream};

pub mod schema;

// static BASE: str = "https://mentat.dev.lcip.org/mentatsync/0.1";

// PUTs
// static TRANSACTION: str = r#"/{}/transactions/{}"#;
// static CHUNK: str = r#"/{}/chunks/{}"#;

// GETs
// get "/{user}/transactions"
//     "?from={trid}"
//     "?limit={limit}"

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        IOError(std::io::Error);
        UriError(hyper::error::UriError);
        HttpError(hyper::Error);
        SqlError(rusqlite::Error);
    }

    errors {
        // TODO expand into real errors
        TolstoyError {
            description("generic error")
            display("generic error TODO")
        }
    }
}

pub type TolstoyResult = Result<()>;

// general flow for upload
// - transactions from mentat -> chunks
// - for chunk in chunks: put_chunk
// - put_transaction
// - put_head

// general flow for download
// - get head - or just get transactions since "last sync"?
// - 

pub fn synchronize(conn: &mut rusqlite::Connection) -> TolstoyResult {
    // Ensure our internal tables are in place.
    schema::ensure_current_version(conn)?;

    // get transactions from the current head.

    Ok(())
}

fn put_chunk(user_id: String, chunk_id: String, payload: String) -> TolstoyResult {
    let uri = format!("https://mentat.dev.lcip.org/mentatsync/0.1/{}/chunks/{}", user_id, chunk_id);

    do_put(uri, payload, StatusCode::Created)
}

#[derive(Serialize)]
struct SerializedTransaction {
    parent: String,
    chunks: Vec<String>
}

fn put_transaction(user_id: String, transaction_id: String, parent: String, chunks: Vec<String>) -> TolstoyResult {
    // {"parent": uuid, "chunks": [chunk1, chunk2...]}
    let transaction = SerializedTransaction {
        parent: parent,
        chunks: chunks
    };

    let uri = format!("https://mentat.dev.lcip.org/mentatsync/0.1/{}/transactions/{}", user_id, transaction_id);
    let json = serde_json::to_string(&transaction).unwrap();

    do_put(uri, json, StatusCode::Created)
}

#[derive(Serialize)]
struct SerializedHead {
    head: String
}

fn put_head(user_id: String, transaction_id: String) -> TolstoyResult {
    // {"head": uuid}
    let head = SerializedHead {
        head: transaction_id
    };

    let uri = format!("https://mentat.dev.lcip.org/mentatsync/0.1/{}/head", user_id);
    let json = serde_json::to_string(&head).unwrap();

    do_put(uri, json, StatusCode::NoContent)
}

fn do_put(uri: String, payload: String, expected: StatusCode) -> TolstoyResult {
    let mut core = Core::new()?;
    let client = Client::new(&core.handle());

    let uri = uri.parse()?;

    let mut req = Request::new(Method::Put, uri);
    req.headers_mut().set(ContentType::json());
    req.headers_mut().set(ContentLength(payload.len() as u64));
    req.set_body(payload);

    let put = client.request(req).and_then(|res| {
        let status_code = res.status();

        if status_code != expected {
            future::err(HyperError::Status)
        } else {
            // body will be empty...
            future::ok(())
        }
    });

    core.run(put)?;
    Ok(())
}

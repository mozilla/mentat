// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std::collections::{
    BTreeMap,
};
use std::convert::{AsRef};
use std::collections::btree_map::{
    Entry,
};
use std::path::{
    Path,
};
use std::sync::{
    Arc,
    RwLock,
};

use rusqlite;

use mentat_db::{
    make_connection,
};

use conn::{
    Conn,
};

use errors::*;

use store::{
    Store,
};

/// A process is only permitted to have one open handle to each database. This manager
/// exists to enforce that constraint: don't open databases directly.
lazy_static! {
    static ref MANAGER: RwLock<Stores> = RwLock::new(Stores::new());
}

/// A struct to store a tuple of a path to a store
/// and the connection to that store. We stores these things
/// together to ensure that two stores at different paths cannot
/// be opened with the same name.
struct StoreConnection {
    conn: Arc<Conn>,
    file_path: String,
}

impl StoreConnection {
    fn new<T>(path: T, maybe_encryption_key: Option<&str>) -> Result<(StoreConnection, rusqlite::Connection)> where T: AsRef<Path> {
        let path = path.as_ref().to_path_buf();
        let os_str = path.as_os_str();
        let file_path = if os_str.is_empty() {
            "file::memory:?cache=shared"
        } else {
            os_str.to_str().unwrap()
        };
        StoreConnection::new_connection(file_path, maybe_encryption_key)
    }

    fn new_named_in_memory_connection(name: &str, maybe_encryption_key: Option<&str>) -> Result<(StoreConnection, rusqlite::Connection)> {
        let file = format!("file::{}?mode=memory&cache=shared", name);
        StoreConnection::new_connection(&file, maybe_encryption_key)
    }

    fn new_connection(file: &str, maybe_encryption_key: Option<&str>) -> Result<(StoreConnection, rusqlite::Connection)> {
        let mut sqlite = make_connection(file.as_ref(), maybe_encryption_key)?;
        Ok((StoreConnection {
            conn: Arc::new(Conn::connect(&mut sqlite)?),
            file_path: file.to_string(),
        }, sqlite))
    }

    fn store(& mut self) -> Result<Store> {
        let sqlite = make_connection(&self.file_path.as_ref(), None)?;
        Store::new(self.conn.clone(), sqlite)
    }

    fn encrypted_store(& mut self, encryption_key: &str) -> Result<Store> {
        let sqlite = make_connection(&self.file_path.as_ref(), Some(encryption_key))?;
        Store::new(self.conn.clone(), sqlite)
    }

    fn store_with_connection(& mut self, sqlite: rusqlite::Connection) -> Result<Store> {
        Store::new(self.conn.clone(), sqlite)
    }
}

/// Stores keeps a reference to a Conn that has been opened for a store
/// along with the path to the store and a key that uniquely identifies
/// that store. The key is stored as a String so that multiple in memory stores
/// can be named and uniquely identified.
pub struct Stores {
    connections: BTreeMap<String, StoreConnection>,
}

impl Stores {
    fn new() -> Stores {
        Stores {
            connections: Default::default(),
        }
    }

    pub fn singleton() -> &'static RwLock<Stores> {
        &*MANAGER
    }

    fn is_store_open(name: &str) -> bool {
        Stores::singleton().read().unwrap().is_open(&name)
    }

    pub fn open_store<T>(path: T) -> Result<Store> where T: AsRef<Path> {
        let path_ref = path.as_ref();
        let name: String = path_ref.to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.open(&name, path_ref)
    }

    pub fn open_named_in_memory_store(name: &str) -> Result<Store> {
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.to_string()))?.open(name, "")
    }

    #[cfg(feature = "sqlcipher")]
    pub fn open_store_with_key<T>(path: T, encryption_key: &str) -> Result<Store> where T: AsRef<Path> {
        let path_ref = path.as_ref();
        let name: String = path_ref.to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.open_with_key(&name, path_ref, encryption_key)
    }

    pub fn get_store<T>(path: T) -> Result<Option<Store>> where T: AsRef<Path> {
        let name: String = path.as_ref().to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.get(&name)
    }

    #[cfg(feature = "sqlcipher")]
    pub fn get_store_with_key<T>(path: T, encryption_key: &str) -> Result<Option<Store>> where T: AsRef<Path> {
        let name: String = path.as_ref().to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.get_with_key(&name, encryption_key)
    }

    pub fn get_named_in_memory_store(name: &str) -> Result<Option<Store>> {
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.to_string()))?.get(name)
    }

    pub fn connect_store< T>(path: T) -> Result<Store> where T: AsRef<Path> {
        let name: String = path.as_ref().to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.connect(&name)
    }

    #[cfg(feature = "sqlcipher")]
    pub fn connect_store_with_key< T>(path: T, encryption_key: &str) -> Result<Store> where T: AsRef<Path> {
        let name: String = path.as_ref().to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.connect_with_key(&name, encryption_key)
    }

    pub fn connect_named_in_memory_store(name: &str) -> Result<Store> {
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.to_string()))?.connect(name)
    }

    pub fn close_store<T>(path: T) -> Result<()> where T: AsRef<Path> {
        let name: String = path.as_ref().to_string_lossy().into();
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.clone()))?.close(&name)
    }

    pub fn close_named_in_memory_store(name: &str) -> Result<()> {
        Stores::singleton().write().map_err(|_| MentatError::StoresLockPoisoned(name.to_string()))?.close(name)
    }
}

impl Stores {
    // Returns true if there exists an entry for the provided name in the connections map.
    // This does not guarentee that the weak reference we hold to the Conn is still valid.
    fn is_open(&self, name: &str) -> bool {
        self.connections.contains_key(name)
    }

    // Open a store with an existing connection if available, or
    // create a new connection if not.
    pub fn open<T>(&mut self, name: &str, path: T) -> Result<Store> where T: AsRef<Path> {
        match self.connections.entry(name.to_string()) {
            Entry::Occupied(mut entry) => {
                let connection = entry.get_mut();
                connection.store()
            },
            Entry::Vacant(entry) =>{
                let path = path.as_ref().to_path_buf();

                let (mut store_connection, connection) = if !name.is_empty() && path.as_os_str().is_empty() {
                    StoreConnection::new_named_in_memory_connection(name, None)?
                } else {
                    StoreConnection::new(path, None)?
                };
                let store = store_connection.store_with_connection(connection);
                entry.insert(store_connection);
                store
            },
        }
    }

    // Open an encrypted store with an existing connection if available, or
    // create a new connection if not.
    #[cfg(feature = "sqlcipher")]
    pub fn open_with_key<T>(&mut self, name: &str, path: T, encryption_key: &str) -> Result<Store> where T: AsRef<Path> {
        match self.connections.entry(name.to_string()) {
            Entry::Occupied(mut entry) => {
                let connection = entry.get_mut();
                connection.store()
            },
            Entry::Vacant(entry) =>{
                let path = path.as_ref().to_path_buf();

                let (mut store_connection, connection) = if !name.is_empty() && path.as_os_str().is_empty() {
                    StoreConnection::new_named_in_memory_connection(name, Some(encryption_key))?
                }  else {
                    StoreConnection::new(path, Some(encryption_key))?
                };
                let store = store_connection.store_with_connection(connection);
                entry.insert(store_connection);
                store
            },
        }
    }

    // Returns a store with an existing connection to path, if available, or None if a
    // store at the provided path has not yet been opened.
    pub fn get(&mut self, name: &str) -> Result<Option<Store>> {
        self.connections.get_mut(name)
                        .map_or(Ok(None), |store_conn| store_conn.store()
                                                                 .map(|s| Some(s)))
    }

    // Returns an encrypted store with an existing connection to path, if available, or None if a
    // store at the provided path has not yet been opened.
    #[cfg(feature = "sqlcipher")]
    pub fn get_with_key(&mut self, name: &str, encryption_key: &str) -> Result<Option<Store>> {
        self.connections.get_mut(name)
                        .map_or(Ok(None), |store_conn| store_conn.encrypted_store(encryption_key)
                                                                 .map(|s| Some(s)))
    }

    // Creates a new store on an existing connection with a new rusqlite connection.
    // Equivalent to forking an existing store.
    pub fn connect(&mut self, name: &str) -> Result<Store> {
        self.connections.get_mut(name)
                        .ok_or(MentatError::StoreNotFound(name.to_string()).into())
                        .and_then(|store_conn| store_conn.store())
    }

    // Creates a new store on an existing connection with a new encrypted rusqlite connection.
    // Equivalent to forking an existing store.
    #[cfg(feature = "sqlcipher")]
    pub fn connect_with_key(&mut self, name: &str, encryption_key: &str) -> Result<Store> {
        self.connections.get_mut(name)
                        .ok_or(MentatError::StoreNotFound(name.to_string()).into())
                        .and_then(|store_conn| store_conn.encrypted_store(encryption_key))
    }

    // Drops the weak reference we have stored to an opened store there is no more than
    // one Store with a reference to the Conn for the provided path.
    pub fn close(&mut self, name: &str) -> Result<()> {
        self.connections.remove(name);
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use conn::{
        Queryable,
    };

    #[test]
    fn test_stores_open_new_store() {
        let name = "test.db";
        let _store = Stores::open_store(name).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));
    }

    #[test]
    fn test_stores_open_new_named_in_memory_store() {
        let name = "test_stores_open_new_named_in_memory_store";
        let _store = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));
    }

    #[test]
    fn test_stores_open_existing_store() {
        let name = "test_stores_open_existing_store";
        let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));
        let store2 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert!(Arc::ptr_eq(&store1.conn(), &store2.conn()));
    }

    #[test]
    fn test_stores_get_open_store() {
        let name = "test_stores_get_open_store";
        let store = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));
        let store_ref = Stores::get_named_in_memory_store(name).expect("Expected a store to be fetched").expect("store");
        assert!(Arc::ptr_eq(&store.conn(), &store_ref.conn()));
    }

    #[test]
    fn test_stores_get_closed_store() {
        match Stores::get_named_in_memory_store("test_stores_get_closed_store").expect("Expected a store to be fetched") {
            None => (),
            Some(_) => panic!("Store is not open and so none should be returned"),
        }
    }

    #[test]
    fn test_stores_connect_open_store() {
        let name = "test_stores_connect_open_store";
        let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));
        {
            // connect to an existing store
            let store2 = Stores::connect_named_in_memory_store(name).expect("expected a new store");
            assert!(Arc::ptr_eq(&store1.conn(), &store2.conn()));

            // get the existing store
            let store3 = Stores::get_named_in_memory_store(name).expect("Expected a store to be fetched").unwrap();
            assert!(Arc::ptr_eq(&store2.conn(), &store3.conn()));
        }

        // connect to the store again
        let store4 = Stores::connect_named_in_memory_store(name).expect("expected a new store");
        assert!(Arc::ptr_eq(&store1.conn(), &store4.conn()));
    }

    #[test]
    fn test_stores_connect_closed_store() {
        let name = "test_stores_connect_closed_store";
        let err = Stores::connect_named_in_memory_store(name).err();
        match err.unwrap() {
            MentatError::StoreNotFound(message) => { assert_eq!(name, message); },
            x => panic!("expected Store Not Found error, got {:?}", x),
        }
    }

    #[test]
    fn test_stores_close_store_with_one_reference() {
        let name = "test_stores_close_store_with_one_reference";
        let store = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert_eq!(3,  Arc::strong_count(&store.conn()));

        assert!(Stores::close_named_in_memory_store(name).is_ok());

        assert!(Stores::get_named_in_memory_store(name).expect("expected an empty result").is_none())
    }

    #[test]
    fn test_stores_close_store_with_multiple_references() {
        let name = "test_stores_close_store_with_multiple_references";

        let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));

        let store2 = Stores::connect_named_in_memory_store(name).expect("expected a connected store");
        assert!(Arc::ptr_eq(&store1.conn(), &store2.conn()));

        Stores::close_named_in_memory_store(name).expect("succeeded");
        assert!(Stores::is_store_open(name) == false);
    }

    #[test]
    fn test_stores_close_unopened_store() {
        let name = "test_stores_close_unopened_store";

        Stores::close_named_in_memory_store(name).expect("succeeded");
        assert!(Stores::is_store_open(name) == false);
    }

    #[test]
    fn test_stores_connect_perform_mutable_operations() {
        let path = "test.db";
        let mut store1 = Stores::open_store(path).expect("Expected a store to be opened");
        {
            let mut in_progress = store1.begin_transaction().expect("begun");
            in_progress.transact(r#"[
                {  :db/ident       :foo/bar
                    :db/cardinality :db.cardinality/one
                    :db/index       true
                    :db/unique      :db.unique/identity
                    :db/valueType   :db.type/long },
                {  :db/ident       :foo/baz
                    :db/cardinality :db.cardinality/one
                    :db/valueType   :db.type/boolean }
                {  :db/ident       :foo/x
                    :db/cardinality :db.cardinality/many
                    :db/valueType   :db.type/long }]"#).expect("transact");

            in_progress.commit().expect("commit");
        }

        // Forking an open store leads to a ref count of 2 on the shared conn.
        // We should be able to perform write operations on this connection.
        let mut store2 = Stores::connect_store(path).expect("expected a new store");
        let mut in_progress = store2.begin_transaction().expect("begun");
        in_progress.transact(r#"[
            {:foo/bar 15, :foo/baz false, :foo/x [1, 2, 3]}
            {:foo/bar 99, :foo/baz true}
            {:foo/bar -2, :foo/baz true}
            ]"#).expect("transact");
        in_progress.commit().expect("commit");

        // We should be able to see the changes made on `store2` on `store1`
        let result = store1.q_once(r#"[:find ?e . :where [?e :foo/baz false]]"#, None).expect("succeeded");
        assert!(result.into_scalar().expect("succeeded").is_some());
    }

    #[test]
    #[cfg(feature = "sqlcipher")]
    fn test_open_store_with_key() {
        let secret_key = "key";
        let name = "../fixtures/v1encrypted.db";
        let _store = Stores::open_store_with_key(name, secret_key).expect("Expected a store to be opened");
        assert!(Stores::is_store_open(name));
    }
}

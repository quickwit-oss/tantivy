use schema::Term;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};



pub struct SuscribeHandle {
    client_id: usize,
    clients: Arc<RwLock<HashMap<usize, u64>>>,
}

impl Drop for SuscribeHandle {
    fn drop(&mut self) {
        self.clients
            .write()
            .unwrap()
            .remove(&self.client_id);
    }
}

struct ClientSuscriptionRegister {
    clients: Arc<RwLock<HashMap<usize, u64>>>,
    client_id_autoinc: AtomicUsize,
}

impl Default for ClientSuscriptionRegister {
    fn default() -> ClientSuscriptionRegister {
        ClientSuscriptionRegister {
            clients: Arc::new(RwLock::new(HashMap::new())),
            client_id_autoinc: AtomicUsize::new(0),
        }
    }
}

impl ClientSuscriptionRegister {
    fn acquire_client_id(&mut self) -> usize {
        self.client_id_autoinc.fetch_add(1, Ordering::SeqCst)
    }

    fn suscribe(&mut self, opstamp: u64) -> SuscribeHandle {
        let client_id = self.acquire_client_id();
        self.clients
            .write()
            .unwrap()
            .insert(client_id, opstamp);
        SuscribeHandle {
            client_id: client_id,
            clients: self.clients.clone(),
        }
    }

}


pub struct DeleteQueue {
    operations: Vec<DeleteOperation>,
    client_subscription_register: ClientSuscriptionRegister,   
}

impl DeleteQueue {
    pub fn push(&mut self, opstamp: u64, term: Term) {
        self.operations.push(DeleteOperation {
            opstamp: opstamp,
            term: term
        });
    }

    pub fn suscribe(&mut self, opstamp: u64) -> SuscribeHandle {
        self.client_subscription_register.suscribe(opstamp)
    }
}

impl Default for DeleteQueue {
    fn default() -> DeleteQueue {
        DeleteQueue {
            operations: Vec::new(),
            client_subscription_register: ClientSuscriptionRegister::default(),
        }
    }
}

struct DeleteOperation {
    opstamp: u64,
    term: Term,
}

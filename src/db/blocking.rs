use std::collections::VecDeque;
use tokio::{sync::mpsc, time::Instant};
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct StreamNotification {
    pub key: String,
    pub item: super::stream_types::StreamItem,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ListNotification {
    pub key: String,
}

#[derive(Debug)]
pub enum ClientSender {
    Stream(mpsc::Sender<StreamNotification>),
    List(mpsc::Sender<ListNotification>),
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BlockedClient {
    id: String,
    key: String,
    blocked_since: Instant,
    sender: ClientSender,
    xread_start: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BlockingQueue {
    waiting_clients: std::collections::HashMap<String, VecDeque<BlockedClient>>,
}

impl BlockingQueue {
    pub fn new() -> Self {
        Self {
            waiting_clients: std::collections::HashMap::new(),
        }
    }

    pub fn add_blocked_xread_client(
        &mut self,
        key: String,
        start: String,
        sender: mpsc::Sender<StreamNotification>,
    ) -> String {
        let client_id = Uuid::new_v4().to_string();
        let client = BlockedClient {
            id: client_id.clone(),
            key: key.clone(),
            blocked_since: Instant::now(),
            sender: ClientSender::Stream(sender),
            xread_start: Some(start),
        };
        self.waiting_clients
            .entry(key)
            .or_default()
            .push_back(client);
        client_id
    }

    pub fn add_blocked_lpop_client(
        &mut self,
        key: String,
        sender: mpsc::Sender<ListNotification>,
    ) -> String {
        let client_id = Uuid::new_v4().to_string();
        let blocked_client = BlockedClient {
            id: client_id.clone(),
            key: key.clone(),
            blocked_since: Instant::now(),
            sender: ClientSender::List(sender),
            xread_start: None,
        };
        self.waiting_clients
            .entry(key)
            .or_default()
            .push_back(blocked_client);
        client_id
    }

    pub fn remove_blocked_client(&mut self, client_id: &str, key: &str) {
        if let Some(queue) = self.waiting_clients.get_mut(key) {
            queue.retain(|client| client.id != client_id);
            if queue.is_empty() {
                self.waiting_clients.remove(key);
            }
        }
    }

    pub fn notify_lpop_clients(&mut self, key: &str) {
        if let Some(queue) = self.waiting_clients.get_mut(key) {
            let notification = ListNotification {
                key: key.to_string(),
            };
            let mut clients_to_retain = VecDeque::new();
            for client in queue.drain(..) {
                match &client.sender {
                    ClientSender::List(sender) => {
                        if sender.try_send(notification.clone()).is_ok() {
                            clients_to_retain.push_back(client);
                        }
                    }
                    ClientSender::Stream(_) => {
                        clients_to_retain.push_back(client);
                    }
                }
            }
            *queue = clients_to_retain;
        }
    }

    pub fn notify_xread_clients(&mut self, key: &str, item: super::stream_types::StreamItem) {
        if let Some(queue) = self.waiting_clients.get_mut(key) {
            let notification = StreamNotification {
                key: key.to_string(),
                item,
            };
            let mut clients_to_retain = VecDeque::new();
            for client in queue.drain(..) {
                match &client.sender {
                    ClientSender::Stream(sender) => {
                        if sender.try_send(notification.clone()).is_ok() {
                            clients_to_retain.push_back(client);
                        }
                    }
                    ClientSender::List(_) => {
                        clients_to_retain.push_back(client);
                    }
                }
            }
            *queue = clients_to_retain;
        }
    }
}

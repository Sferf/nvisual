use std::{future::Future, hash::{Hash, Hasher}, pin::Pin, sync::{atomic::AtomicU64, atomic::Ordering, Arc}};

use tokio::sync::Mutex;

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

pub struct Task {
    task: Mutex<Pin<Box<dyn Future<Output=()> + Send>>>,
    parent: Option<u64>,
    childs_non_completed: AtomicU64,
    id: u64,
}

impl Task {
    pub fn new<F>(parent: Option<u64>, fut: F, childs_count: u64) -> Arc<Self> 
    where 
        F: Future<Output = ()> + Send + 'static

    {
        let id = Self::next_id();
        let task = Arc::new(Self {
            task: Mutex::new(Box::pin(fut)),
            parent: parent,
            childs_non_completed: AtomicU64::new(childs_count),
            id,
        });
        if let Some(parent_id) = parent {
            println!("Created new task: {} with parent: {}", task.get_id(), parent_id);
        } else {
            println!("Created new task: {}", task.get_id());
        }
        task
    }

    fn next_id() -> u64 {
        NEXT_TASK_ID.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub async fn run(self: Arc<Self>) -> u64 {
        println!("Running: {}", self.id);

        let mut fut_guard = self.task.lock().await;
        fut_guard.as_mut().await;

        self.id
    }

    pub async fn reduce_child(&self) {
        self.childs_non_completed.fetch_sub(1, Ordering::SeqCst);
    }

    pub async fn get_parent_id(&self) -> Option<u64> {
        self.parent
    }

    pub fn is_ready(&self) -> bool {
        self.childs_non_completed.load(Ordering::SeqCst) == 0
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Task {}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

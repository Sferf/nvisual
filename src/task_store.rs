use std::{collections::{HashMap}, sync::Arc, u64};
use tokio::sync::Mutex as TokioMutex;


use crate::task::Task;


pub struct TaskStore {
    tasks: TokioMutex<HashMap<u64, Arc<Task>>>,
}

impl TaskStore {
    pub fn new() -> Self {
        Self {
            tasks: TokioMutex::new(HashMap::new())
        }
    }

    pub async fn add_task(&self, task: Arc<Task>) {
        let mut map = self.tasks.lock().await;
        map.insert(task.get_id(), task);
    }

    pub async fn remove_task(&self, task: &Arc<Task>) {
        let mut map = self.tasks.lock().await;
        map.remove(&task.get_id());
    }

    pub async fn reduce_childs(&self, id: u64) -> Option<u64> {
        let map = self.tasks.lock().await;
        map[&id].reduce_child().await;
        None
    }

    pub async fn get_parent_id(&self, id: u64) -> Option<u64> {
        let map = self.tasks.lock().await;
        map[&id].get_parent_id().await
    }

    pub async fn get_if_ready(&self, id: u64) -> Option<Arc<Task>> {
        let map = self.tasks.lock().await;
        if map[&id].is_ready() {
            return Some(map[&id].clone())
        }
        None
    }

    pub async fn all_tasks(&self) -> Vec<Arc<Task>> {
        let map = self.tasks.lock().await;
        map.values().cloned().collect()
    }

}

mod task_store;
mod task;

use std::{sync::{Arc}};

use tokio::{runtime::{Runtime}};

use futures::stream::{FuturesUnordered, StreamExt};
use task::Task;
use task_store::TaskStore;

use async_recursion::async_recursion;

#[async_recursion]
async fn gen_recursive(parent: Option<u64>, deep: u64, size: u64, store: Arc<TaskStore>) {
    let parent_id = match parent {
        None => {
            let task_head = Task::new(
                None,
                async {
                    println!("Inside task");
                },
                size,
            );
            let id = task_head.get_id();
            store.add_task(task_head).await;
            id
        },
        Some(id) => id,
        
    };

    for i in 1..=size {
        if i == 1 && deep != 0 {
            let task = Task::new(
                Some(parent_id),
                async move {
                    println!("HARD WORK");
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000*size - 1000*i)).await;
                },
                size
            );
            gen_recursive(Some(task.get_id()), deep - 1, size, Arc::clone(&store)).await;
            store.add_task(task).await;
        } else {
            let task = Task::new(
                Some(parent_id),
                async move {
                    println!("HARD WORK");
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000*size - 1000*i)).await;
                },
                0
            );
            store.add_task(task).await;
        }
    }
}

fn main() {
    let store = Arc::new(TaskStore::new());

    let mut active_tasks = FuturesUnordered::new();

    let rt = Runtime::new().unwrap();
    let size = 5;
    let deep = 5;

    rt.block_on( async {
        gen_recursive(None, deep, size, Arc::clone(&store)).await;

        for task in store.all_tasks().await {
            if task.is_ready() {
                println!("Ready: {}", task.get_id());
                active_tasks.push(tokio::spawn(task.run()));
            }
        }

        while let Some(done_task) = active_tasks.next().await {
            match done_task {
                Ok(id) => {
                    println!("Finished: {}", id);
                    let parent_id_opt = store.get_parent_id(id).await;
                    if let Some(parent_id) = parent_id_opt {
                        store.reduce_childs(parent_id).await;
                        let task_opt = store.get_if_ready(parent_id).await;
                        if let Some(task) = task_opt {
                            println!("Prepare new task: {}", task.get_id());
                            active_tasks.push(tokio::spawn(task.run()));
                        }
                    }
                },
                Err(e) => println!("Task panic {}", e),
            }
        }
    });

}


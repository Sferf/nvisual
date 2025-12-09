mod task_store;
mod task;

use std::{sync::Arc, time::Instant};
use tokio::runtime::Builder;

use futures::stream::{FuturesUnordered, StreamExt};
use task::Task;
use task_store::TaskStore;

use async_recursion::async_recursion;

fn heavy_load_ms(ms: u64) {
    let start = std::time::Instant::now();
    while start.elapsed().as_millis() < ms as u128 {
        std::hint::spin_loop();
    }
}

async fn gen_simple(store: Arc<TaskStore>) {
    let parent_task = Task::new(
        None,
        async move {
            println!("<<<<<<<<<<<<< FINAL PARENT");
            heavy_load_ms(100);
        },
        2 
    );
    let first_child_task = Task::new(
        Some(parent_task.get_id()),
        async move {
            println!("FIRST CHILD");
            heavy_load_ms(10000);
        },
        1 
    );
    let second_child_task = Task::new(
        Some(parent_task.get_id()),
        async move {
            println!("SECOND CHILD");
            heavy_load_ms(100);
        },
        1 
    );
    let first_child_child_task = Task::new(
        Some(first_child_task.get_id()),
        async move {
            println!("FIRST CHILD CHILD");
            heavy_load_ms(100);
        },
        0 
    );
    let second_child_child_task = Task::new(
        Some(second_child_task.get_id()),
        async move {
            println!("SECOND CHILD CHILD");
            heavy_load_ms(10000);
        },
        0 
    );
    store.add_task(parent_task).await;
    store.add_task(first_child_task).await;
    store.add_task(second_child_task).await;
    store.add_task(first_child_child_task).await;
    store.add_task(second_child_child_task).await;
}

#[async_recursion]
async fn gen_recursive(parent: Option<u64>, deep: u64, size: u64, store: Arc<TaskStore>) {
    let parent_id = match parent {
        None => {
            let task_head = Task::new(
                None,
                async {
                    println!("<<<<<<<<<<<<< FINAL PARENT");
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
        let time = if i == size {
            100 * size
        } else {
            200
        };
        let mut task = Task::new(
            Some(parent_id),
            async move {
                println!("HARD WORK");
                heavy_load_ms(time);
            },
            size
        );
        if i == 1 && deep != 0 {
            gen_recursive(Some(task.get_id()), deep - 1, size, Arc::clone(&store)).await;
        } else if i == 2 && deep >= 2 {
            gen_recursive(Some(task.get_id()), deep - 2, size, Arc::clone(&store)).await;
            
        } else {
            task = Task::new(
                Some(parent_id),
                async move {
                    println!("HARD WORK");
                    heavy_load_ms(time);
                },
                0
            );
        }
        store.add_task(task).await;
    }
}

fn main() {
    let rt = Builder::new_multi_thread()
    .worker_threads(4)
    .enable_all()
    .build()
    .unwrap();

    let store = Arc::new(TaskStore::new());

    let mut active_tasks = FuturesUnordered::new();

    let size = 6;
    let deep = 6;

    let start_time = Instant::now();
    rt.block_on( async {
        // gen_recursive(None, deep, size, Arc::clone(&store)).await;
        gen_simple(Arc::clone(&store)).await;

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
    let elapsed_time = start_time.elapsed();

    let second_store = Arc::new(TaskStore::new());

    let mut second_active_tasks = FuturesUnordered::new();

    let second_start_time = Instant::now();
    rt.block_on( async {
        // gen_recursive(None, deep, size, Arc::clone(&second_store)).await;
        gen_simple(Arc::clone(&second_store)).await;

        for task in second_store.all_tasks().await {
            if task.is_ready() {
                println!("Ready: {}", task.get_id());
                second_active_tasks.push(tokio::spawn(task.run()));
            }
        }

        let mut is_done = false;

        while !is_done {
            let mut done_tasks: Vec<u64> = Vec::new();

            while let Some(done_task) = second_active_tasks.next().await {
                match done_task {
                    Ok(id) => {
                        println!("Finished: {}", id);
                        done_tasks.push(id);
                    },
                    Err(e) => println!("Task panic {}", e),
                }
            }

            if done_tasks.len() == 0 {
                is_done = true;
            }
            for task_id in done_tasks {
                let parent_id_opt = second_store.get_parent_id(task_id).await;
                if let Some(parent_id) = parent_id_opt {
                    second_store.reduce_childs(parent_id).await;
                    let task_opt = second_store.get_if_ready(parent_id).await;
                    if let Some(task) = task_opt {
                        println!("Prepare new task: {}", task.get_id());
                        second_active_tasks.push(tokio::spawn(task.run()));
                    }
                }
            }
        }

    });
    let second_elapsed_time = second_start_time.elapsed();

    println!(">>>>>>> Time for normal solution: {:.2?}", elapsed_time);
    println!(">>>>>>> Time for basic solution: {:.2?}", second_elapsed_time);
}


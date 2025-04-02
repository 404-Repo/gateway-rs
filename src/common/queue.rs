use foldhash::fast::RandomState;
use scc::HashMap;
use scc::Queue;
use std::hash::Hash;
use uuid::Uuid;

use crate::api::HasUuid;

const HASHMAP_START_CAPACITY: usize = 1024;

#[derive(Clone)]
struct QueueEntry<T> {
    item: T,
    count: usize,
}

pub struct DupQueue<T> {
    dup: usize,
    q: Queue<QueueEntry<T>>,
    sent: HashMap<(Uuid, String), (), RandomState>,
}

impl<T: Clone + Hash + HasUuid + 'static> DupQueue<T> {
    pub fn new(dup: usize) -> Self {
        Self {
            dup,
            q: Queue::default(),
            sent: HashMap::with_capacity_and_hasher(HASHMAP_START_CAPACITY, RandomState::default()),
        }
    }

    pub fn push(&self, item: T) {
        self.q.push(QueueEntry {
            item,
            count: self.dup,
        });
    }

    pub fn pop(&self, num: usize, hotkey: &str) -> Vec<T> {
        let mut result = Vec::with_capacity(num);
        while result.len() < num {
            match self.q.pop_if(|entry| {
                let key = (*entry.item.id(), hotkey.to_string());
                !self.sent.contains(&key)
            }) {
                Ok(Some(shared_entry)) => {
                    let mut entry = (*shared_entry).clone();
                    let hotkey_str = hotkey.to_string();
                    let key = (*entry.item.id(), hotkey_str.clone());
                    // Try to insert the key into the map; if it was absent, then process.
                    if self.sent.insert(key.clone(), ()).is_ok() {
                        result.push(entry.item.clone());
                        entry.count -= 1;
                        // If there are duplicates remaining, push the entry back;
                        // otherwise remove the key.
                        if entry.count > 0 {
                            self.q.push((*entry).clone());
                        } else {
                            self.sent.remove(&key);
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        result
    }

    pub fn dup(&self) -> usize {
        self.dup
    }

    pub fn len(&self) -> usize {
        self.q.len()
    }
}

#[cfg(test)]
mod tests {
    use super::DupQueue;
    use crate::api::Task;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;
    use uuid::Uuid;

    fn create_task(prompt: &str) -> Task {
        Task {
            id: Uuid::new_v4(),
            prompt: prompt.to_string(),
        }
    }

    #[test]
    fn test_single_push_pop() {
        // With dup = 3, an element may be delivered to up to 3 distinct hotkeys.
        let queue = DupQueue::new(3);
        let task = create_task("Task 1");
        queue.push(task.clone());

        // Hotkey "a" receives the task.
        assert_eq!(queue.pop(3, "a"), vec![task.clone()]);
        // Subsequent calls with the same hotkey "a" yield nothing.
        assert_eq!(queue.pop(3, "a"), Vec::<Task>::new());
        // New hotkeys "b" and "c" obtain the task.
        assert_eq!(queue.pop(3, "b"), vec![task.clone()]);
        assert_eq!(queue.pop(3, "c"), vec![task.clone()]);
        // Duplication allowance exhausted; new hotkey "d" gets nothing.
        assert_eq!(queue.pop(3, "d"), Vec::<Task>::new());
    }

    #[test]
    fn test_multiple_pushes() {
        // With dup = 2, each task can be delivered to 2 distinct hotkeys.
        let queue = DupQueue::new(2);
        let task1 = create_task("Task 1");
        let task2 = create_task("Task 2");
        queue.push(task1.clone());
        queue.push(task2.clone());
        // Pushing a duplicate of task1 (same content and UUID).
        // To simulate a duplicate, we push the same task instance.
        queue.push(task1.clone());

        let mut res = queue.pop(2, "key1");
        // Sorting by prompt for a deterministic comparison.
        res.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1, task2];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_pop_more_than_exists() {
        let queue = DupQueue::new(4);
        let task1 = create_task("Task A");
        let task2 = create_task("Task B");
        queue.push(task1.clone());
        queue.push(task2.clone());
        let mut res = queue.pop(3, "alpha");
        res.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1, task2];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_requeue_duplicates_in_pop() {
        let queue = DupQueue::new(3);
        let task1 = create_task("hello");
        let task2 = create_task("world");
        queue.push(task1.clone());
        queue.push(task2.clone());
        // For hotkey "key1", both tasks are delivered initially.
        let mut res1 = queue.pop(2, "key1");
        res1.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1.clone(), task2.clone()];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res1, expected);

        // A subsequent pop with "key1" returns nothing.
        let res2 = queue.pop(2, "key1");
        assert!(res2.is_empty());
        // A new hotkey "key2" can receive them.
        let mut res3 = queue.pop(2, "key2");
        res3.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res3, expected);
    }

    #[test]
    fn test_concurrent_push_pop() {
        let queue = Arc::new(DupQueue::new(3));
        let q_producer = Arc::clone(&queue);

        // Producer thread pushes tasks with distinct prompts.
        let producer = thread::spawn(move || {
            for i in 0..10 {
                let task = create_task(&format!("Task {}", i));
                q_producer.push(task);
            }
        });
        producer.join().unwrap();

        // Spawn consumer threads each using a unique hotkey.
        let mut handles = Vec::new();
        for idx in 0..3 {
            let q = Arc::clone(&queue);
            let hotkey = format!("h{}", idx);
            handles.push(thread::spawn(move || {
                let res = q.pop(5, &hotkey);
                // Each pop call returns distinct tasks for that hotkey.
                let set: HashSet<_> = res.iter().map(|t| t.id.clone()).collect();
                assert_eq!(set.len(), res.len());
                res
            }));
        }
        for handle in handles {
            let _ = handle.join().unwrap();
        }
    }

    #[test]
    fn test_internal_hashset_cleared_after_final_delivery() {
        // Use dup = 1 so that the pushed task is delivered exactly once.
        let queue = DupQueue::new(1);
        let task = create_task("Final Task");
        queue.push(task.clone());

        // For a new hotkey the task should be delivered.
        let res = queue.pop(1, "test_hotkey");
        assert_eq!(res, vec![task]);
        // Since the task was delivered for the only allowed time,
        // the internal hashset 'sent' should now be empty.
        assert!(
            queue.sent.is_empty(),
            "Internal hashset should be cleared after final delivery."
        );
    }
}

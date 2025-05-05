use foldhash::fast::RandomState;
use foldhash::HashSetExt as _;
use scc::HashMap;
use scc::Queue;
use std::hash::Hash;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::api::HasUuid;

const SENTMAP_START_CAPACITY: usize = 1024;
const EXPIREDMAP_START_CAPACITY: usize = 8;

#[derive(Clone)]
struct QueueEntry<T> {
    item: T,
    count: usize,
    timestamp: Instant,
}

struct Inner<T> {
    dup: usize,
    q: Queue<QueueEntry<T>>,
    sent: HashMap<(Uuid, String), (), RandomState>,
    ttl: Duration,
}

#[derive(Clone)]
pub struct DupQueue<T> {
    inner: Arc<Inner<T>>,
}

pub struct DupQueueBuilder {
    dup: usize,
    ttl: Duration,
    cleanup_interval: Duration,
}

impl DupQueueBuilder {
    pub fn dup(mut self, dup: usize) -> Self {
        self.dup = dup;
        self
    }

    pub fn ttl(mut self, ttl: u64) -> Self {
        self.ttl = Duration::from_secs(ttl);
        self
    }

    pub fn cleanup_interval(mut self, interval: u64) -> Self {
        self.cleanup_interval = Duration::from_secs(interval);
        self
    }

    pub fn build<T>(self) -> DupQueue<T>
    where
        T: Clone + Hash + HasUuid + Send + Sync + 'static,
    {
        let inner: Arc<Inner<T>> = Arc::new(Inner {
            dup: self.dup,
            q: Queue::default(),
            sent: HashMap::with_capacity_and_hasher(SENTMAP_START_CAPACITY, RandomState::default()),
            ttl: self.ttl,
        });
        let cleanup_interval = self.cleanup_interval;
        let weak_inner = Arc::downgrade(&inner);
        thread::spawn(move || {
            while let Some(inner) = weak_inner.upgrade() {
                thread::sleep(cleanup_interval);

                let mut expired_ids = foldhash::HashSet::with_capacity(EXPIREDMAP_START_CAPACITY);
                while let Ok(Some(shared)) = inner
                    .q
                    .pop_if(|entry| entry.timestamp.elapsed() > inner.ttl)
                {
                    expired_ids.insert(*shared.item.id());
                }

                if !expired_ids.is_empty() {
                    inner
                        .sent
                        .retain(|(uuid, _hotkey), _| !expired_ids.contains(uuid));
                }
            }
        });
        DupQueue { inner }
    }
}

impl<T> DupQueue<T>
where
    T: Clone + Hash + HasUuid + Send + Sync + 'static,
{
    pub fn builder() -> DupQueueBuilder {
        DupQueueBuilder {
            dup: 1,
            ttl: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(1),
        }
    }

    pub fn push(&self, item: T) {
        let entry = QueueEntry {
            item,
            count: self.inner.dup,
            timestamp: Instant::now(),
        };
        self.inner.q.push(entry);
    }

    pub fn pop(&self, num: usize, hotkey: &str) -> Vec<T> {
        let mut result = Vec::with_capacity(num);
        while result.len() < num {
            match self.inner.q.pop_if(|entry| {
                let key = (*entry.item.id(), hotkey.to_string());
                !self.inner.sent.contains(&key)
            }) {
                Ok(Some(shared)) => {
                    let mut entry = (*shared).clone();
                    let key_str = hotkey.to_string();
                    let key = (*entry.item.id(), key_str.clone());
                    if self.inner.sent.insert(key.clone(), ()).is_ok() {
                        result.push(entry.item.clone());
                        entry.count -= 1;
                        if entry.count > 0 {
                            self.inner.q.push((*entry).clone());
                        } else {
                            self.inner.sent.remove(&key);
                        }
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        result
    }

    pub fn dup(&self) -> usize {
        self.inner.dup
    }

    pub fn len(&self) -> usize {
        self.inner.q.len()
    }
}

#[cfg(test)]
mod tests {
    use super::DupQueue;
    use crate::api::Task;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
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
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(3)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
        let task = create_task("Task 1");
        queue.push(task.clone());

        // Hotkey "a" receives the task.
        assert_eq!(queue.pop(3, "a"), vec![task.clone()]);
        // Subsequent calls with the same hotkey "a" yield nothing.
        assert!(queue.pop(3, "a").is_empty());
        // New hotkeys "b" and "c" obtain the task.
        assert_eq!(queue.pop(3, "b"), vec![task.clone()]);
        assert_eq!(queue.pop(3, "c"), vec![task.clone()]);
        // Duplication allowance exhausted; new hotkey "d" gets nothing.
        assert!(queue.pop(3, "d").is_empty());
    }

    #[test]
    fn test_multiple_pushes() {
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(2)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
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
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(4)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
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
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(3)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
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
        assert!(queue.pop(2, "key1").is_empty());
        // A new hotkey "key2" can receive them again.
        let mut res3 = queue.pop(2, "key2");
        res3.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res3, expected);
    }

    #[test]
    fn test_concurrent_push_pop() {
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(3)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
        let q_producer = Arc::clone(&queue);
        let producer = thread::spawn(move || {
            for i in 0..10 {
                q_producer.push(create_task(&format!("Task {}", i)));
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
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_cleanup_removes_old_entries() {
        let queue = DupQueue::<Task>::builder()
            .dup(1)
            .ttl(1)
            .cleanup_interval(1)
            .build();
        let task = create_task("Old Task");
        queue.push(task);
        thread::sleep(Duration::from_secs(2));
        assert!(queue.pop(1, "any").is_empty());
    }
}

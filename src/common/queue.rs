use foldhash::fast::RandomState;
use foldhash::HashSetExt as _;
use scc::{HashMap, Queue};
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
    item: Arc<T>,
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

    pub fn ttl(mut self, ttl_secs: u64) -> Self {
        self.ttl = Duration::from_secs(ttl_secs);
        self
    }

    pub fn cleanup_interval(mut self, interval_secs: u64) -> Self {
        self.cleanup_interval = Duration::from_secs(interval_secs);
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
            item: Arc::new(item),
            count: self.inner.dup,
            timestamp: Instant::now(),
        };
        self.inner.q.push(entry);
    }

    pub fn pop(&self, num: usize, hotkey: &str) -> Vec<(T, Option<Duration>)> {
        let mut result = Vec::with_capacity(num);
        let initial_len = self.inner.q.len();
        let mut scanned = 0;

        // Try up to initial_len, or until we've got num results
        while result.len() < num && scanned < initial_len {
            match self.inner.q.pop() {
                Some(shared) => {
                    scanned += 1;
                    let mut entry = (*shared).clone();
                    let key = (*entry.item.id(), hotkey.to_string());

                    // If this hotkey hasn't seen it yet, mark & deliver
                    if !self.inner.sent.contains(&key) {
                        let _ = self.inner.sent.insert(key.clone(), ());
                        entry.count -= 1;
                        let dur = if entry.count == 0 {
                            Some(Instant::now() - entry.timestamp)
                        } else {
                            None
                        };
                        result.push(((*entry.item).clone(), dur));
                    }

                    // If there are still duplicates left, put it back on the queue
                    if entry.count > 0 {
                        self.inner.q.push((*entry).clone());
                    }
                }
                None => break, // queue’s empty
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    fn create_task(prompt: &str) -> Task {
        Task {
            id: Uuid::new_v4(),
            prompt: prompt.to_string(),
        }
    }

    #[test]
    fn test_single_push_pop() {
        // With dup = 3, an element may be delivered to up to 3 distinct hotkeys. (only the final delivery returns Some(duration))
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(3)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
        let task = create_task("Task 1");
        queue.push(task.clone());

        // Hotkey "a" receives the task. (count goes 3→2; duration should be None)
        let res_a = queue.pop(3, "a");
        assert_eq!(res_a.len(), 1);
        let (item_a, dur_a) = &res_a[0];
        assert_eq!(item_a, &task);
        assert!(dur_a.is_none());

        // Subsequent calls with the same hotkey "a" yield nothing.
        assert!(queue.pop(3, "a").is_empty());

        // New hotkeys "b" and "c" obtain the task.
        //  - for "b": count goes 2→1; duration None
        //  - for "c": count goes 1→0; duration Some(_) (final delivery)
        let res_b = queue.pop(3, "b");
        assert_eq!(res_b.len(), 1);
        let (item_b, dur_b) = &res_b[0];
        assert_eq!(item_b, &task);
        assert!(dur_b.is_none());

        let res_c = queue.pop(3, "c");
        assert_eq!(res_c.len(), 1);
        let (item_c, dur_c) = &res_c[0];
        assert_eq!(item_c, &task);
        assert!(dur_c.is_some(), "expected a duration on the final delivery");

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
        queue.push(task1.clone());

        let res = queue.pop(2, "key1");
        let mut res_tasks: Vec<Task> = res.iter().map(|(task, _)| task.clone()).collect();
        res_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1, task2];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res_tasks, expected);
        for (_, duration) in res {
            assert!(duration.is_none());
        }
    }

    #[test]
    fn test_pop_more_than_exists() {
        for &dup in &[1usize, 4usize] {
            let queue = Arc::new(
                DupQueue::<Task>::builder()
                    .dup(dup)
                    .ttl(300)
                    .cleanup_interval(1)
                    .build(),
            );
            let task1 = create_task("Task A");
            let task2 = create_task("Task B");
            queue.push(task1.clone());
            queue.push(task2.clone());
            let res = queue.pop(50, "alpha");
            let mut res_tasks: Vec<Task> = res.iter().map(|(task, _)| task.clone()).collect();
            res_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
            let mut expected = vec![task1.clone(), task2.clone()];
            expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
            assert_eq!(res_tasks, expected);
            for (_, duration) in &res {
                if dup == 1 {
                    assert!(duration.is_some());
                } else {
                    assert!(duration.is_none());
                }
            }
            if dup == 1 {
                assert!(queue.pop(3, "beta").is_empty());
            }

            let res = queue.pop(50, "alpha");
            assert!(res.is_empty());
        }
    }

    #[test]
    fn heavy_contention_pop() {
        let q = Arc::new(
            DupQueue::<Task>::builder()
                .dup(3)
                .ttl(10)
                .cleanup_interval(1)
                .build(),
        );

        let number_of_tasks = 10_000;
        let producers = 2;
        let consumers = 4;
        let barrier = Arc::new(Barrier::new(4 + producers));

        for _ in 0..producers {
            let q_producer = Arc::clone(&q);
            let b_producer = Arc::clone(&barrier);
            thread::spawn(move || {
                b_producer.wait();
                for i in 0..number_of_tasks {
                    q_producer.push(create_task(&i.to_string()));
                }
            });
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(4);
        let b_consumer = Arc::clone(&barrier);

        for _ in 0..consumers {
            let q_consumer = Arc::clone(&q);
            let b_consumer = Arc::clone(&b_consumer);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                b_consumer.wait();
                let mut idle_start: Option<Instant> = None;
                loop {
                    if counter.load(Ordering::Relaxed) == producers * number_of_tasks {
                        break;
                    }
                    let got = q_consumer.pop(7, "hot");
                    if !got.is_empty() {
                        counter.fetch_add(got.len(), Ordering::Relaxed);
                        idle_start = None;
                    } else {
                        idle_start.get_or_insert_with(Instant::now);
                        if idle_start.unwrap().elapsed() >= Duration::from_secs(5) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(50));
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert_eq!(counter.load(Ordering::Relaxed), producers * number_of_tasks);
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
        let res1 = queue.pop(2, "key1");
        let mut res1_tasks: Vec<Task> = res1.iter().map(|(task, _)| task.clone()).collect();
        res1_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1.clone(), task2.clone()];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res1_tasks, expected);
        for (_, duration) in res1 {
            assert!(duration.is_none());
        }
        // A subsequent pop with "key1" returns nothing.
        assert!(queue.pop(2, "key1").is_empty());
        // A new hotkey "key2" can receive them again.
        let res3 = queue.pop(2, "key2");
        let mut res3_tasks: Vec<Task> = res3.iter().map(|(task, _)| task.clone()).collect();
        res3_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res3_tasks, expected);
        for (_, duration) in res3 {
            assert!(duration.is_none());
        }
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

        let mut handles = Vec::new();
        for idx in 0..3 {
            let q = Arc::clone(&queue);
            let hotkey = format!("h{}", idx);
            handles.push(thread::spawn(move || {
                let res = q.pop(5, &hotkey);
                // Each pop call returns distinct tasks for that hotkey.
                let set: HashSet<_> = res.iter().map(|(task, _)| task.id.clone()).collect();
                assert_eq!(set.len(), res.len());
                for (_, duration) in &res {
                    assert!(duration.is_none());
                }
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

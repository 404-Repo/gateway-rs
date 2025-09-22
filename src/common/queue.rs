use foldhash::fast::RandomState;
use foldhash::HashSetExt as _;
use scc::{HashMap, Queue};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::{self, JoinHandle};
use uuid::Uuid;

use crate::api::HasUuid;
use crate::bittensor::hotkey::Hotkey;

const SENTMAP_START_CAPACITY: usize = 1024;
const EXPIREDMAP_START_CAPACITY: usize = 8;
const DEFAULT_DUP_COUNT: usize = 1;
const DEFAULT_TTL_SECS: u64 = 300;
const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 1;

#[derive(Clone)]
struct QueueEntry<T> {
    item: T,
    count: usize,
    timestamp: Instant,
}

struct Inner<T> {
    dup: usize,
    q: Queue<QueueEntry<T>>,
    sent: HashMap<(Uuid, Hotkey), (), RandomState>,
    ttl: Duration,
    len: AtomicUsize,
}

#[derive(Clone)]
pub struct DupQueue<T> {
    inner: Arc<Inner<T>>,
    _cleanup_task: Arc<JoinHandle<()>>,
}

impl<T> Drop for DupQueue<T> {
    fn drop(&mut self) {
        self._cleanup_task.abort();
    }
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
            len: AtomicUsize::new(0),
        });
        let cleanup_interval = self.cleanup_interval;
        let weak_inner = Arc::downgrade(&inner);
        let handle = task::spawn(async move {
            while let Some(inner) = weak_inner.upgrade() {
                tokio::time::sleep(cleanup_interval).await;

                let mut expired_ids = foldhash::HashSet::with_capacity(EXPIREDMAP_START_CAPACITY);
                while let Ok(Some(shared)) = inner
                    .q
                    .pop_if(|entry| entry.timestamp.elapsed() > inner.ttl)
                {
                    expired_ids.insert(*shared.item.id());
                    inner.len.fetch_sub(1, Ordering::Relaxed);
                }

                if !expired_ids.is_empty() {
                    inner
                        .sent
                        .retain_async(|(uuid, _hotkey), _| !expired_ids.contains(uuid))
                        .await;
                }
            }
        });
        DupQueue {
            inner,
            _cleanup_task: Arc::new(handle),
        }
    }
}

impl<T> DupQueue<T>
where
    T: Clone + Hash + HasUuid + Send + Sync + 'static,
{
    pub fn builder() -> DupQueueBuilder {
        DupQueueBuilder {
            dup: DEFAULT_DUP_COUNT,
            ttl: Duration::from_secs(DEFAULT_TTL_SECS),
            cleanup_interval: Duration::from_secs(DEFAULT_CLEANUP_INTERVAL_SECS),
        }
    }

    pub fn push(&self, item: T) {
        let entry = QueueEntry {
            item,
            count: self.inner.dup,
            timestamp: Instant::now(),
        };
        self.inner.q.push(entry);
        self.inner.len.fetch_add(1, Ordering::Relaxed);
    }

    pub fn pop(&self, num: usize, hotkey: &Hotkey) -> Vec<(T, Option<Duration>)> {
        let mut result = Vec::with_capacity(num);

        // Capture the current queue length once so we don't iterate indefinitely
        let initial_len = self.inner.q.len();

        for _ in 0..initial_len {
            if result.len() >= num {
                break;
            }

            let Some(shared) = self.inner.q.pop() else {
                break;
            };

            // We removed an item from the queue, update our length counter
            self.inner.len.fetch_sub(1, Ordering::Relaxed);

            let mut entry = (*shared).clone();
            let key = (*entry.item.id(), hotkey.clone());

            // Deliver to this hotkey only once
            if !self.inner.sent.contains_sync(&key) {
                let _ = self.inner.sent.insert_sync(key, ());
                entry.count -= 1;

                let dur = (entry.count == 0).then(|| Instant::now() - entry.timestamp);

                result.push((entry.item.clone(), dur));
            }

            // Re-queue if there are more duplicates left
            if entry.count > 0 {
                self.inner.len.fetch_add(1, Ordering::Relaxed);
                self.inner.q.push((*entry).clone());
            }
        }

        result
    }

    #[allow(dead_code)]
    pub fn dup(&self) -> usize {
        self.inner.dup
    }

    pub fn len(&self) -> usize {
        self.inner.len.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::DupQueue;
    use crate::api::Task;
    use crate::bittensor::hotkey::Hotkey;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::Barrier;
    use tokio::task;
    use uuid::Uuid;

    fn create_task(prompt: &str) -> Task {
        Task {
            id: Uuid::new_v4(),
            prompt: Some(Arc::new(prompt.to_string())),
            image: None,
        }
    }

    #[tokio::test]
    async fn test_single_push_pop() {
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
        let hk_a: Hotkey = Hotkey::from_bytes(&[1u8; 32]);
        let res_a = queue.pop(3, &hk_a);
        assert_eq!(res_a.len(), 1);
        let (item_a, dur_a) = &res_a[0];
        assert_eq!(item_a, &task);
        assert!(dur_a.is_none());

        // Subsequent calls with the same hotkey "a" yield nothing.
        assert!(queue.pop(3, &hk_a).is_empty());

        // New hotkeys "b" and "c" obtain the task.
        //  - for "b": count goes 2→1; duration None
        //  - for "c": count goes 1→0; duration Some(_) (final delivery)
        let hk_b: Hotkey = Hotkey::from_bytes(&[2u8; 32]);
        let res_b = queue.pop(3, &hk_b);
        assert_eq!(res_b.len(), 1);
        let (item_b, dur_b) = &res_b[0];
        assert_eq!(item_b, &task);
        assert!(dur_b.is_none());

        let hk_c: Hotkey = Hotkey::from_bytes(&[3u8; 32]);
        let res_c = queue.pop(3, &hk_c);
        assert_eq!(res_c.len(), 1);
        let (item_c, dur_c) = &res_c[0];
        assert_eq!(item_c, &task);
        assert!(dur_c.is_some(), "expected a duration on the final delivery");

        // Duplication allowance exhausted; new hotkey "d" gets nothing.
        let hk_d: Hotkey = Hotkey::from_bytes(&[4u8; 32]);
        assert!(queue.pop(3, &hk_d).is_empty());
    }

    #[tokio::test]
    async fn test_multiple_pushes() {
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

        let hk_1: Hotkey = Hotkey::from_bytes(&[5u8; 32]);
        let res = queue.pop(2, &hk_1);
        let mut res_tasks: Vec<Task> = res.iter().map(|(task, _)| task.clone()).collect();
        res_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1, task2];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res_tasks, expected);
        for (_, duration) in res {
            assert!(duration.is_none());
        }
    }

    #[tokio::test]
    async fn test_pop_more_than_exists() {
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
            let hk_alpha: Hotkey = Hotkey::from_bytes(&[6u8; 32]);
            let res = queue.pop(50, &hk_alpha);
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
                let hk_beta: Hotkey = Hotkey::from_bytes(&[7u8; 32]);
                assert!(queue.pop(3, &hk_beta).is_empty());
            }

            let res = queue.pop(50, &hk_alpha);
            assert!(res.is_empty());
        }
    }

    #[tokio::test]
    async fn heavy_contention_pop() {
        let q = DupQueue::<Task>::builder()
            .dup(1)
            .ttl(10)
            .cleanup_interval(1)
            .build();

        let number_of_tasks = 10_000;
        let producers = 2;
        let consumers = 4;
        let barrier = Arc::new(Barrier::new(4 + producers));

        for _ in 0..producers {
            let q_producer = q.clone();
            let b_producer = Arc::clone(&barrier);
            task::spawn(async move {
                b_producer.wait().await;
                for i in 0..number_of_tasks {
                    q_producer.push(create_task(&i.to_string()));
                }
            });
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(4);
        let b_consumer = Arc::clone(&barrier);

        for _ in 0..consumers {
            let q_consumer = q.clone();
            let b_consumer = Arc::clone(&b_consumer);
            let counter = Arc::clone(&counter);
            handles.push(task::spawn(async move {
                b_consumer.wait().await;
                let mut idle_start: Option<Instant> = None;
                loop {
                    if counter.load(Ordering::Relaxed) == producers * number_of_tasks {
                        break;
                    }
                    let hk_a = Hotkey::from_bytes(&[8u8; 32]);
                    let got = q_consumer.pop(7, &hk_a);
                    if !got.is_empty() {
                        counter.fetch_add(got.len(), Ordering::Relaxed);
                        idle_start = None;
                    } else {
                        idle_start.get_or_insert_with(Instant::now);
                        if idle_start.unwrap().elapsed() >= Duration::from_secs(5) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.expect("Task panicked");
        }

        assert_eq!(q.len(), 0);
        assert_eq!(counter.load(Ordering::Relaxed), producers * number_of_tasks);

        let q = DupQueue::<Task>::builder()
            .dup(1)
            .ttl(2)
            .cleanup_interval(1)
            .build();

        for i in 0..100 {
            q.push(create_task(&i.to_string()));
        }
        assert_eq!(q.len(), 100);

        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(q.len(), 0);
    }

    #[tokio::test]
    async fn test_requeue_duplicates_in_pop() {
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
        let hk_1: Hotkey = Hotkey::from_bytes(&[9u8; 32]);
        let res1 = queue.pop(2, &hk_1);
        let mut res1_tasks: Vec<Task> = res1.iter().map(|(task, _)| task.clone()).collect();
        res1_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        let mut expected = vec![task1.clone(), task2.clone()];
        expected.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res1_tasks, expected);
        for (_, duration) in res1 {
            assert!(duration.is_none());
        }
        // A subsequent pop with "key1" returns nothing.
        assert!(queue.pop(2, &hk_1).is_empty());
        // A new hotkey "key2" can receive them again.
        let hk_2: Hotkey = Hotkey::from_bytes(&[10u8; 32]);
        let res3 = queue.pop(2, &hk_2);
        let mut res3_tasks: Vec<Task> = res3.iter().map(|(task, _)| task.clone()).collect();
        res3_tasks.sort_by(|a, b| a.prompt.cmp(&b.prompt));
        assert_eq!(res3_tasks, expected);
        for (_, duration) in res3 {
            assert!(duration.is_none());
        }
    }

    #[tokio::test]
    async fn test_concurrent_push_pop() {
        let queue = Arc::new(
            DupQueue::<Task>::builder()
                .dup(3)
                .ttl(300)
                .cleanup_interval(1)
                .build(),
        );
        let q_producer = Arc::clone(&queue);
        let producer = task::spawn(async move {
            for i in 0..10 {
                q_producer.push(create_task(&format!("Task {}", i)));
            }
        });
        producer.await.unwrap();

        let mut handles = Vec::new();
        for idx in 0..3 {
            let q = Arc::clone(&queue);
            let hotkey: Hotkey = match idx {
                0 => Hotkey::from_bytes(&[11u8; 32]),
                1 => Hotkey::from_bytes(&[12u8; 32]),
                _ => Hotkey::from_bytes(&[13u8; 32]),
            };
            handles.push(task::spawn(async move {
                let res = q.pop(5, &hotkey);
                // Each pop call returns distinct tasks for that hotkey.
                let set: HashSet<_> = res.iter().map(|(task, _)| task.id).collect();
                assert_eq!(set.len(), res.len());
                for (_, duration) in &res {
                    assert!(duration.is_none());
                }
                res
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_cleanup_removes_old_entries() {
        let queue = DupQueue::<Task>::builder()
            .dup(1)
            .ttl(1)
            .cleanup_interval(1)
            .build();
        let task = create_task("Old Task");
        queue.push(task);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let hk: Hotkey = Hotkey::from_bytes(&[14u8; 32]);
        assert!(queue.pop(1, &hk).is_empty());
    }
}

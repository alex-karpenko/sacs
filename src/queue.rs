use crate::{
    event::{Event, EventId},
    ControlChannel, Error, Result,
};
use std::fmt::Debug;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::SystemTime,
};
use tokio::{
    select,
    sync::{RwLock, RwLockWriteGuard},
    time::{sleep, Duration},
};
use tracing::{debug, instrument};

/// Default size of Queue control channel
const QUEUE_CONTROL_CHANNEL_SIZE: usize = 1024;
/// Time to sleep control loop if queue is empty, actually value doesn't matter
const EMPTY_QUEUE_SLEEP_DURATION_SECONDS: u64 = 60 * 60;

/// Events queue behavior
pub trait EventTimeQueue: Debug {
    /// Blocks execution until next event time.
    /// Returns Result with event and remove it from queue.
    async fn next(&self) -> Result<Event>;
    /// Blocks execution until next event time or timeout exceeded.
    /// Returns Option with Result with event and remove it from queue, or None if timeout.
    #[allow(dead_code)]
    async fn try_next(&self, timeout: Duration) -> Option<Result<Event>> {
        select! {
            biased;
            next_job = self.next() => Some(next_job),
            _ = sleep(timeout) => None,
        }
    }

    /// Insert event into queue at event's time position.
    async fn insert(&self, event: Event) -> Result<()>;
    /// Insert event into queue at current time.
    #[allow(dead_code)]
    async fn push(&self, id: EventId) -> Result<()> {
        self.insert(Event::with_id(id)).await
    }

    /// Remove event from the queue at it's time position.
    #[allow(dead_code)]
    async fn remove(&self, event: &Event) -> Result<()>;
    /// Removes all event's instances at all times.
    async fn pop(&self, id: &EventId) -> Result<Vec<SystemTime>>;

    /// Breaks control loop (next/try_next)
    async fn shutdown(&self);
}

/// Simple (but enough) implementation of EventTimeQueue
#[derive(Debug)]
pub(crate) struct Queue {
    // Indexes of events by id and time
    index: RwLock<QueueIndex>,
    // Channel to send update queue events and shutdown
    control_channel: ControlChannel<ChangeStateEvent>,
}

#[derive(Debug)]
struct QueueIndex {
    by_time: BTreeMap<SystemTime, BTreeSet<EventId>>,
    by_id: HashMap<EventId, BTreeSet<SystemTime>>,
}

impl QueueIndex {
    fn new() -> Self {
        Self {
            by_time: BTreeMap::<SystemTime, BTreeSet<EventId>>::new(),
            by_id: HashMap::<EventId, BTreeSet<SystemTime>>::new(),
        }
    }

    #[instrument("clear_queue_indexes", skip(self))]
    fn clear(&mut self) {
        debug!(
            time_index_size = self.by_time.len(),
            id_index_size = self.by_id.len(),
        );
        self.by_time.clear();
        self.by_id.clear();
    }
}

/// Queue state change events
#[derive(Debug)]
enum ChangeStateEvent {
    // Send it each time when insert or remove event
    QueueUpdated,
    // Stop control loop and return error
    Shutdown,
}

impl Queue {
    /// Helper method to remove event from the queue.
    /// It helps to avoid deadlocks during multiply remove operations.
    #[instrument(skip(index))]
    async fn remove_event_from_index(
        event: &Event,
        index: &mut RwLockWriteGuard<'_, QueueIndex>,
    ) -> Result<()> {
        let ids = index.by_time.get(&event.time());
        let times = index.by_id.get(&event.id());

        // Event is present in both indexes - it's OK, remove it
        if ids.is_some() && times.is_some() {
            debug!("remove event from indexes");
            // Remove from time index
            let ids = index.by_time.get_mut(&event.time()).unwrap();
            ids.remove(&event.id());
            if ids.is_empty() {
                index.by_time.remove(&event.time());
            }

            // Remove from id index
            let event_times = index.by_id.get_mut(&event.id()).unwrap();
            event_times.remove(&event.time());

            if event_times.is_empty() {
                index.by_id.remove(&event.id());
            }

            Ok(())
        } else if ids.is_none() && times.is_none() {
            // Event is absent in both indexes:
            // it's strange (we try to delete non-existent event), but this is not an error.
            debug!("event not found");
            return Ok(());
        } else {
            // There is some inconsistency in indexes - looks like a BUG
            debug!("inconsistent indexes discovered");
            return Err(Error::InconsistentQueueContent);
        }
    }
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            index: RwLock::new(QueueIndex::new()),
            control_channel: ControlChannel::new(QUEUE_CONTROL_CHANNEL_SIZE),
        }
    }
}

impl EventTimeQueue for Queue {
    #[instrument(skip(self))]
    async fn next(&self) -> Result<Event> {
        loop {
            // Calculate sleep time for next loop iteration
            let sleep_for = {
                let mut index = self.index.write().await;
                // Get first time in queue and check if it's in the past, return event if so instead of calculating time to sleep
                if let Some((top_time, top_ids)) = index.by_time.first_key_value() {
                    let now = SystemTime::now();
                    if now >= *top_time {
                        // Construct event from the top of the queue id/time
                        // Get first id from the list if there are many ids
                        let event =
                            Event::new(top_ids.first().unwrap().clone(), top_time.to_owned());
                        // Remove it from queue
                        Queue::remove_event_from_index(&event, &mut index).await?;
                        // And break loop returning event
                        debug!(next_event_to_process = ?event);
                        return Ok(event);
                    } else {
                        // Or calculate sleep time from now to the next event
                        top_time.duration_since(now)?
                    }
                } else {
                    // Queue is empty - sleep for a while
                    Duration::from_secs(EMPTY_QUEUE_SLEEP_DURATION_SECONDS)
                }
            };
            debug!(next_event_delay = ?sleep_for);

            // Sleep but listen to the control channel
            select! {
                biased;
                control_event = self.control_channel.receive() => {
                    debug!(control_event_received = ?control_event);
                    if let Some(control_event) = control_event {
                        match control_event {
                            ChangeStateEvent::Shutdown => return Err(Error::ShutdownRequested),
                            // Run loop from the beginning
                            ChangeStateEvent::QueueUpdated => {},
                        }
                    }
                },
                // Run loop from the beginning
                _ = sleep(sleep_for) => {},
            }
        }
    }

    #[instrument(skip(self))]
    async fn insert(&self, event: Event) -> Result<()> {
        debug!("insert new event");
        {
            let mut index = self.index.write().await;

            // Insert into index by time
            if let Some(ids) = index.by_time.get_mut(&event.time()) {
                ids.insert(event.id());
            } else {
                let ids = BTreeSet::from([event.id()]);
                index.by_time.insert(event.time(), ids);
            }

            // Insert into index by job id
            if let Some(event_time) = index.by_id.get_mut(&event.id()) {
                event_time.insert(event.time());
            } else {
                let event_time = BTreeSet::from([event.time()]);
                index.by_id.insert(event.id(), event_time);
            }
        }

        self.control_channel
            .send_event(ChangeStateEvent::QueueUpdated)
            .await
    }

    #[instrument(skip(self))]
    async fn remove(&self, event: &Event) -> Result<()> {
        debug!("remove event");
        {
            let mut index = self.index.write().await;
            Queue::remove_event_from_index(event, &mut index).await?;
        }

        self.control_channel
            .send_event(ChangeStateEvent::QueueUpdated)
            .await
    }

    #[instrument(skip(self))]
    async fn pop(&self, id: &EventId) -> Result<Vec<SystemTime>> {
        debug!("purge all events");
        let mut return_times: Vec<SystemTime> = Vec::new();
        {
            let mut index = self.index.write().await;
            let event_times = index.by_id.get_mut(id);
            if let Some(event_times) = event_times {
                debug!(events_to_purge = event_times.len());
                // Loop over event's times and remove each one
                // But preserve list of times to return it
                return_times = event_times.iter().copied().collect();
                for event_time in &return_times {
                    Queue::remove_event_from_index(
                        &Event::new(id.clone(), *event_time),
                        &mut index,
                    )
                    .await?;
                }
            }
        }

        self.control_channel
            .send_event(ChangeStateEvent::QueueUpdated)
            .await?;
        Ok(return_times)
    }

    #[instrument(skip(self))]
    async fn shutdown(&self) {
        debug!("clear queue");
        self.index.write().await.clear();

        debug!("send shutdown event");
        let _result = self
            .control_channel
            .send_event(ChangeStateEvent::Shutdown)
            .await;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[tokio::test]
    async fn basic() {
        let queue = Queue::default();
        let id = EventId::default();
        let time = SystemTime::now();
        let event_now = Event::new(id.clone(), time);
        let event_1s = Event::new(
            id.clone(),
            time.checked_add(Duration::from_secs(1)).unwrap(),
        );
        let event_2s = Event::new(
            id.clone(),
            time.checked_add(Duration::from_secs(2)).unwrap(),
        );

        queue.insert(event_now.clone()).await.unwrap();
        queue.insert(event_1s.clone()).await.unwrap();
        queue.insert(event_2s.clone()).await.unwrap();

        let next = queue.next().await.unwrap();
        assert_eq!(next, event_now);

        let next = queue.try_next(Duration::from_millis(100)).await;
        assert!(next.is_none());

        let next = queue.next().await.unwrap();
        assert_eq!(next, event_1s);
        assert!(SystemTime::now() > next.time());

        let next = queue.try_next(Duration::from_millis(100)).await;
        assert!(next.is_none());

        let next = queue
            .try_next(Duration::from_millis(1000))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next, event_2s);
        assert!(SystemTime::now() > next.time());
    }

    #[tokio::test]
    async fn insert_remove() {
        let queue = Queue::default();
        let id_1 = EventId::default();
        let id_2 = EventId::default();
        let time = SystemTime::now();

        let event_1_0s = Event::new(id_1.clone(), time);
        let event_2_0s = Event::new(id_2.clone(), time);
        let event_1s = Event::new(
            id_1.clone(),
            time.checked_add(Duration::from_secs(1)).unwrap(),
        );
        let event_2s = Event::new(
            id_1.clone(),
            time.checked_add(Duration::from_secs(2)).unwrap(),
        );

        // Test insert of two events at the same time
        queue.insert(event_1_0s.clone()).await.unwrap();
        queue.insert(event_2_0s.clone()).await.unwrap();
        let next_1 = queue.next().await.unwrap();
        let next_2 = queue.next().await.unwrap();
        let mut set_got = HashSet::new();
        let mut set_exp = HashSet::new();
        set_got.insert(next_1);
        set_got.insert(next_2);
        set_exp.insert(event_1_0s);
        set_exp.insert(event_2_0s);
        assert_eq!(set_exp, set_got);

        // Remove the first event when 2nd later is present
        queue.insert(event_1s.clone()).await.unwrap();
        queue.insert(event_2s.clone()).await.unwrap();
        let remove_1s = async {
            sleep(Duration::from_millis(500)).await;
            let _ = queue.remove(&event_1s).await;
            sleep(Duration::from_millis(2000)).await;
        };

        let next = select! {
            next = queue.next() => { Some(next.unwrap()) },
            _ = remove_1s => { None },
        };

        assert!(next.is_some());
        assert_eq!(next.clone().unwrap(), event_2s);
        assert!(SystemTime::now() > next.unwrap().time());

        queue.remove(&event_1s).await.unwrap();
    }

    #[tokio::test]
    async fn push_pop() {
        let queue = Queue::default();
        let id_1 = EventId::default();
        let id_2 = EventId::default();

        queue.push(id_1.clone()).await.unwrap();
        queue.push(id_2.clone()).await.unwrap();

        let next = queue.next().await.unwrap();
        assert_eq!(next.id(), id_1);

        let next = queue.next().await.unwrap();
        assert_eq!(next.id(), id_2);

        queue.push(id_1.clone()).await.unwrap();
        queue.push(id_1.clone()).await.unwrap();
        queue.push(id_1.clone()).await.unwrap();
        queue.push(id_2.clone()).await.unwrap();

        let removed = queue.pop(&id_1).await.unwrap().len();
        assert_eq!(removed, 3);

        let next = queue.next().await.unwrap();
        assert_eq!(next.id(), id_2);
    }

    #[tokio::test]
    async fn shutdown() {
        async fn get_next_event(queue: &Queue) -> Option<Result<Event>> {
            select! {
                e = queue.next() => { Some(e) },
                _ = async {
                    sleep(Duration::from_millis(500)).await;
                    queue.shutdown().await;
                    sleep(Duration::from_millis(1000)).await;
                } => { None },
            }
        }

        let queue = Queue::default();

        // Shutdown on empty queue
        let next = get_next_event(&queue).await;
        assert!(next.is_some());
        let next = next.unwrap();
        assert!(matches!(next, Err(Error::ShutdownRequested)));

        // Shutdown with events in queue
        let id = EventId::default();
        let time = SystemTime::now();
        let event_1s = Event::new(
            id.clone(),
            time.checked_add(Duration::from_secs(1)).unwrap(),
        );
        queue.insert(event_1s.clone()).await.unwrap();
        let next = get_next_event(&queue).await;

        assert!(next.is_some());
        let next = next.unwrap();
        assert!(matches!(next, Err(Error::ShutdownRequested)));
    }
}

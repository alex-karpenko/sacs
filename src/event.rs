use crate::task::TaskId;
use chrono::{DateTime, Local};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub(crate) struct EventId {
    pub id: Uuid,
}

impl Default for EventId {
    fn default() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

impl From<Uuid> for EventId {
    fn from(value: Uuid) -> Self {
        Self { id: value }
    }
}

impl From<&Uuid> for EventId {
    fn from(value: &Uuid) -> Self {
        Self { id: *value }
    }
}

impl From<EventId> for Uuid {
    fn from(value: EventId) -> Self {
        value.id
    }
}

impl From<&EventId> for Uuid {
    fn from(value: &EventId) -> Self {
        value.id
    }
}

#[derive(PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub(crate) struct Event {
    pub id: EventId,
    time: SystemTime,
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let time_str = format!("{}", DateTime::<Local>::from(self.time));
        f.debug_struct("Event")
            .field("id", &self.id)
            .field("time", &time_str)
            .finish()
    }
}

impl Event {
    pub fn new(id: EventId, time: SystemTime) -> Self {
        Self { id, time }
    }

    #[allow(dead_code)]
    pub fn with_id(id: EventId) -> Self {
        Self {
            id,
            time: SystemTime::now(),
        }
    }

    pub fn with_time(time: SystemTime) -> Self {
        Self {
            id: EventId::default(),
            time,
        }
    }

    pub fn id(&self) -> EventId {
        self.id.clone()
    }

    pub fn time(&self) -> SystemTime {
        self.time
    }
}

impl Default for Event {
    fn default() -> Self {
        Self::new(EventId::default(), SystemTime::now())
    }
}

impl From<SystemTime> for Event {
    fn from(value: SystemTime) -> Self {
        Self::with_time(value)
    }
}

impl From<&SystemTime> for Event {
    fn from(value: &SystemTime) -> Self {
        Self::with_time(*value)
    }
}

impl From<Event> for EventId {
    fn from(value: Event) -> EventId {
        value.id
    }
}

impl From<TaskId> for EventId {
    fn from(value: TaskId) -> EventId {
        Self { id: value.id }
    }
}

impl From<&Event> for EventId {
    fn from(value: &Event) -> Self {
        value.id.clone()
    }
}

impl From<Event> for SystemTime {
    fn from(value: Event) -> SystemTime {
        value.time
    }
}

impl From<&Event> for SystemTime {
    fn from(value: &Event) -> Self {
        value.time
    }
}

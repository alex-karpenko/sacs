use crate::task::TaskId;
use chrono::{DateTime, Local};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub(crate) struct EventId {
    pub id: String,
}

impl Default for EventId {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().into(),
        }
    }
}

impl From<Uuid> for EventId {
    fn from(value: Uuid) -> Self {
        Self { id: value.into() }
    }
}

impl From<&Uuid> for EventId {
    fn from(value: &Uuid) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<String> for EventId {
    fn from(value: String) -> Self {
        Self { id: value }
    }
}

impl From<&String> for EventId {
    fn from(value: &String) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<&str> for EventId {
    fn from(value: &str) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<EventId> for String {
    fn from(value: EventId) -> Self {
        value.id
    }
}

impl From<&EventId> for String {
    fn from(value: &EventId) -> Self {
        value.id.to_owned()
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
    pub fn new(id: impl Into<EventId>, time: SystemTime) -> Self {
        Self {
            id: id.into(),
            time,
        }
    }

    pub fn with_id(id: impl Into<EventId>) -> Self {
        Self {
            id: id.into(),
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

#[cfg(test)]
mod test {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn type_convertors() {
        let uuid_id = Uuid::new_v4();
        let str_id = uuid_id.to_string();
        let event_id = EventId { id: str_id.clone() };

        assert_eq!(EventId::from(uuid_id), event_id);
        assert_eq!(EventId::from(&uuid_id), event_id);
        assert_eq!(EventId::from(str_id.clone()), event_id);
        assert_eq!(EventId::from(&str_id), event_id);
        assert_eq!(
            EventId::from("EVENT_ID"),
            EventId {
                id: "EVENT_ID".to_string()
            }
        );

        let id = String::from("TEST");
        let now = SystemTime::now();
        let event = Event::new(id.clone(), now);
        let task_id = TaskId::from(id.clone());

        assert_eq!(String::from(&event_id), str_id);
        assert_eq!(String::from(event_id), str_id);

        assert_eq!(Event::from(now).time, now);
        assert_eq!(Event::from(&now).time, now);

        assert_eq!(EventId::from(event.clone()).id, id);
        assert_eq!(EventId::from(&event).id, id);

        assert_eq!(EventId::from(task_id).id, id);

        assert_eq!(SystemTime::from(event.clone()), now);
        assert_eq!(SystemTime::from(&event), now);
    }

    #[test]
    fn constructors() {
        let id = String::from("TEST");
        let now = SystemTime::now();

        assert_ne!(EventId::default(), EventId::default());
        assert_eq!(Event::with_id(id.clone()).id(), EventId::from(id.clone()));
        assert_eq!(Event::with_time(now.clone()).time(), now);
    }

    #[test]
    fn debug_formatter() {
        let id = String::from("TEST");
        let now = SystemTime::now();

        assert_eq!(
            format!("{:?}", Event::new(id, now)),
            format!(
                "Event {{ id: EventId {{ id: \"TEST\" }}, time: \"{}\" }}",
                DateTime::<Local>::from(now)
            )
        );
    }
}

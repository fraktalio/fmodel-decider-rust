use std::marker::PhantomData;
#[cfg(feature = "single-threaded")]
use std::rc::Rc;
#[cfg(not(feature = "single-threaded"))]
use std::sync::Arc;

// ============================================================================
// Multi-threaded Projection (default)
// ============================================================================

/// # `Projection` — Pure state evolution (multi-threaded)
///
/// Enforces `Send + Sync` bounds on all behavioral components and uses `Arc`
/// for cheap cloning when composing projections.
///
/// A `Projection` represents pure state evolution logic:
/// - `evolve` applies a single **event** to a **state**
/// - `initial_state` produces the initial state
/// - No decision-making, no command handling
///
/// Implements `ViewTrait<S, S, E>`. Does not implement `DeciderTrait`,
/// `EventComputationTrait`, or `StateComputationTrait`.
#[cfg(not(feature = "single-threaded"))]
pub struct Projection<S, E, EvolveFn, InitFn>
where
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
{
    /// Evolution function: `(state, event) -> new_state`
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    _marker: PhantomData<(S, E)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<S, E, EvolveFn, InitFn> Projection<S, E, EvolveFn, InitFn>
where
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
{
    /// Creates a new thread-safe `Projection`.
    pub fn new(evolve: EvolveFn, initial_state: InitFn) -> Self {
        Self {
            evolve,
            initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the internal state type `S` to a new state type `S2`.
    pub fn map_state<S2, F1, F2>(
        self,
        f1: F1,
        f2: F2,
    ) -> Projection<S2, E, impl Fn(&S2, &E) -> S2 + Send + Sync, impl Fn() -> S2 + Send + Sync>
    where
        F1: Fn(&S2) -> S + Send + Sync,
        F2: Fn(&S) -> S2 + Send + Sync,
    {
        let f1 = Arc::new(f1);
        let f2 = Arc::new(f2);

        let f1_evolve = Arc::clone(&f1);
        let f2_evolve = Arc::clone(&f2);
        let f2_init = Arc::clone(&f2);

        let evolve = move |s2: &S2, e: &E| {
            let s = f1_evolve(s2);
            let new_s = (self.evolve)(&s, e);
            f2_evolve(&new_s)
        };

        let initial_state = move || {
            let s = (self.initial_state)();
            f2_init(&s)
        };

        Projection {
            evolve,
            initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the event type `E` to a new event type `E2`.
    pub fn map_event<E2, F>(
        self,
        f: F,
    ) -> Projection<S, E2, impl Fn(&S, &E2) -> S + Send + Sync, InitFn>
    where
        F: Fn(&E2) -> E + Send + Sync,
    {
        let f = Arc::new(f);

        let evolve = move |s: &S, e2: &E2| {
            let e = f(e2);
            (self.evolve)(s, &e)
        };

        Projection {
            evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Merges two projections into one.
    ///
    /// Unlike `combine` on deciders (which uses `Sum<E, E2>`), `merge` keeps the **same
    /// event type `E`** — both projections evolve on every event.
    pub fn merge<S2, EvolveFn2, InitFn2>(
        self,
        other: Projection<S2, E, EvolveFn2, InitFn2>,
    ) -> Projection<
        (S, S2),
        E,
        impl Fn(&(S, S2), &E) -> (S, S2) + Send + Sync,
        impl Fn() -> (S, S2) + Send + Sync,
    >
    where
        S2: Send + Sync,
        EvolveFn2: Fn(&S2, &E) -> S2 + Send + Sync,
        InitFn2: Fn() -> S2 + Send + Sync,
    {
        let evolve = move |s: &(S, S2), e: &E| ((self.evolve)(&s.0, e), (other.evolve)(&s.1, e));
        let initial_state = move || ((self.initial_state)(), (other.initial_state)());
        Projection::new(evolve, initial_state)
    }
}

// ============================================================================
// Single-threaded Projection
// ============================================================================

/// # `Projection` — Pure state evolution (single-threaded)
///
/// Does not impose `Send + Sync` bounds. Uses `Rc` instead of `Arc` for lower overhead.
#[cfg(feature = "single-threaded")]
pub struct Projection<S, E, EvolveFn, InitFn>
where
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
{
    /// Evolution function
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    _marker: PhantomData<(S, E)>,
}

#[cfg(feature = "single-threaded")]
impl<S, E, EvolveFn, InitFn> Projection<S, E, EvolveFn, InitFn>
where
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
{
    /// Creates a new single-threaded `Projection`.
    pub fn new(evolve: EvolveFn, initial_state: InitFn) -> Self {
        Self {
            evolve,
            initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the internal state type without introducing thread-safety overhead.
    pub fn map_state<S2, F1, F2>(
        self,
        f1: F1,
        f2: F2,
    ) -> Projection<S2, E, impl Fn(&S2, &E) -> S2, impl Fn() -> S2>
    where
        F1: Fn(&S2) -> S,
        F2: Fn(&S) -> S2,
    {
        let f1 = Rc::new(f1);
        let f2 = Rc::new(f2);

        let f1_evolve = Rc::clone(&f1);
        let f2_evolve = Rc::clone(&f2);
        let f2_init = Rc::clone(&f2);

        let evolve = move |s2: &S2, e: &E| {
            let s = f1_evolve(s2);
            let new_s = (self.evolve)(&s, e);
            f2_evolve(&new_s)
        };

        let initial_state = move || {
            let s = (self.initial_state)();
            f2_init(&s)
        };

        Projection {
            evolve,
            initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the event type `E` to a new event type `E2` without thread-safety overhead.
    pub fn map_event<E2, F>(self, f: F) -> Projection<S, E2, impl Fn(&S, &E2) -> S, InitFn>
    where
        F: Fn(&E2) -> E,
    {
        let f = Rc::new(f);

        let evolve = move |s: &S, e2: &E2| {
            let e = f(e2);
            (self.evolve)(s, &e)
        };

        Projection {
            evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Merges two projections into one without thread-safety overhead.
    pub fn merge<S2, EvolveFn2, InitFn2>(
        self,
        other: Projection<S2, E, EvolveFn2, InitFn2>,
    ) -> Projection<(S, S2), E, impl Fn(&(S, S2), &E) -> (S, S2), impl Fn() -> (S, S2)>
    where
        EvolveFn2: Fn(&S2, &E) -> S2,
        InitFn2: Fn() -> S2,
    {
        let evolve = move |s: &(S, S2), e: &E| ((self.evolve)(&s.0, e), (other.evolve)(&s.1, e));
        let initial_state = move || ((self.initial_state)(), (other.initial_state)());
        Projection::new(evolve, initial_state)
    }
}

// ============================================================================
// ViewTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<S, E, EvolveFn, InitFn> crate::ViewTrait<S, S, E> for Projection<S, E, EvolveFn, InitFn>
where
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
{
    fn evolve(&self, state: &S, event: &E) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

#[cfg(feature = "single-threaded")]
impl<S, E, EvolveFn, InitFn> crate::ViewTrait<S, S, E> for Projection<S, E, EvolveFn, InitFn>
where
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
{
    fn evolve(&self, state: &S, event: &E) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ViewTrait;
    use std::collections::HashMap;
    #[cfg(not(feature = "single-threaded"))]
    use std::sync::Arc;

    #[derive(Debug, PartialEq, Clone)]
    enum Event {
        UserRegistered { id: u32, name: String },
        UserUpdated { id: u32, name: String },
        UserDeleted { id: u32 },
    }

    type UserDirectory = HashMap<u32, String>;

    fn make_projection() -> Projection<
        UserDirectory,
        Event,
        impl Fn(&UserDirectory, &Event) -> UserDirectory,
        impl Fn() -> UserDirectory,
    > {
        Projection::new(
            |state: &UserDirectory, event: &Event| {
                let mut new_state = state.clone();
                match event {
                    Event::UserRegistered { id, name } => {
                        new_state.insert(*id, name.clone());
                    }
                    Event::UserUpdated { id, name } => {
                        new_state.insert(*id, name.clone());
                    }
                    Event::UserDeleted { id } => {
                        new_state.remove(id);
                    }
                }
                new_state
            },
            || HashMap::new(),
        )
    }

    #[test]
    fn basic_projection_flow() {
        let projection = make_projection();

        let state0 = projection.initial_state();
        assert!(state0.is_empty());

        let state1 = projection.evolve(
            &state0,
            &Event::UserRegistered {
                id: 1,
                name: "Alice".to_string(),
            },
        );
        assert_eq!(state1.get(&1), Some(&"Alice".to_string()));
        assert_eq!(state1.len(), 1);

        let state2 = projection.evolve(
            &state1,
            &Event::UserUpdated {
                id: 1,
                name: "Alice Smith".to_string(),
            },
        );
        assert_eq!(state2.get(&1), Some(&"Alice Smith".to_string()));
        assert_eq!(state2.len(), 1);

        let state3 = projection.evolve(&state2, &Event::UserDeleted { id: 1 });
        assert!(state3.is_empty());
    }

    #[test]
    fn multiple_users_projection() {
        let projection = make_projection();

        let mut state = projection.initial_state();

        state = projection.evolve(
            &state,
            &Event::UserRegistered {
                id: 1,
                name: "Alice".to_string(),
            },
        );
        state = projection.evolve(
            &state,
            &Event::UserRegistered {
                id: 2,
                name: "Bob".to_string(),
            },
        );
        state = projection.evolve(
            &state,
            &Event::UserRegistered {
                id: 3,
                name: "Charlie".to_string(),
            },
        );

        assert_eq!(state.len(), 3);

        state = projection.evolve(
            &state,
            &Event::UserUpdated {
                id: 2,
                name: "Robert".to_string(),
            },
        );

        assert_eq!(state.len(), 3);
        assert_eq!(state.get(&2), Some(&"Robert".to_string()));

        state = projection.evolve(&state, &Event::UserDeleted { id: 1 });

        assert_eq!(state.len(), 2);
        assert_eq!(state.get(&1), None);
        assert_eq!(state.get(&2), Some(&"Robert".to_string()));
        assert_eq!(state.get(&3), Some(&"Charlie".to_string()));
    }

    #[test]
    fn map_state_flow() {
        let projection = make_projection();

        let mapped = projection.map_state(
            |count: &usize| {
                let mut dir = HashMap::new();
                for i in 0..*count {
                    dir.insert(i as u32, format!("User{}", i));
                }
                dir
            },
            |dir: &UserDirectory| dir.len(),
        );

        let state0 = mapped.initial_state();
        assert_eq!(state0, 0);

        let state1 = mapped.evolve(
            &state0,
            &Event::UserRegistered {
                id: 1,
                name: "Alice".to_string(),
            },
        );
        assert_eq!(state1, 1);
    }

    #[test]
    fn map_event_flow() {
        let projection = make_projection();

        #[derive(Debug, PartialEq)]
        enum StringEvent {
            UserAction(String),
        }

        let mapped = projection.map_event(|se: &StringEvent| match se {
            StringEvent::UserAction(s) => {
                if s.starts_with("register:") {
                    let name = s.strip_prefix("register:").unwrap();
                    Event::UserRegistered {
                        id: 1,
                        name: name.to_string(),
                    }
                } else if s.starts_with("update:") {
                    let name = s.strip_prefix("update:").unwrap();
                    Event::UserUpdated {
                        id: 1,
                        name: name.to_string(),
                    }
                } else {
                    Event::UserDeleted { id: 1 }
                }
            }
        });

        let state0 = mapped.initial_state();
        assert!(state0.is_empty());

        let state1 = mapped.evolve(
            &state0,
            &StringEvent::UserAction("register:Alice".to_string()),
        );
        assert_eq!(state1.get(&1), Some(&"Alice".to_string()));

        let state2 = mapped.evolve(
            &state1,
            &StringEvent::UserAction("update:Alice Smith".to_string()),
        );
        assert_eq!(state2.get(&1), Some(&"Alice Smith".to_string()));

        let state3 = mapped.evolve(&state2, &StringEvent::UserAction("delete".to_string()));
        assert!(state3.is_empty());
    }

    #[test]
    fn composition_flow() {
        let projection = make_projection();

        let composed = projection.map_event(|s: &String| {
            if s.starts_with("add:") {
                let name = s.strip_prefix("add:").unwrap();
                Event::UserRegistered {
                    id: 1,
                    name: name.to_string(),
                }
            } else {
                Event::UserDeleted { id: 1 }
            }
        });

        let state0 = composed.initial_state();
        assert!(state0.is_empty());

        let state1 = composed.evolve(&state0, &"add:Alice".to_string());
        assert_eq!(state1.len(), 1);
        assert_eq!(state1.get(&1), Some(&"Alice".to_string()));

        let state2 = composed.evolve(&state1, &"remove".to_string());
        assert!(state2.is_empty());
    }

    #[test]
    fn view_trait_usage() {
        let projection = make_projection();

        fn use_view_trait<V>(view: &V, events: &[Event]) -> UserDirectory
        where
            V: ViewTrait<UserDirectory, UserDirectory, Event>,
        {
            let mut state = view.initial_state();
            for event in events {
                state = view.evolve(&state, event);
            }
            state
        }

        let events = vec![
            Event::UserRegistered {
                id: 1,
                name: "Alice".to_string(),
            },
            Event::UserRegistered {
                id: 2,
                name: "Bob".to_string(),
            },
            Event::UserUpdated {
                id: 1,
                name: "Alice Smith".to_string(),
            },
        ];

        let final_state = use_view_trait(&projection, &events);
        assert_eq!(final_state.len(), 2);
        assert_eq!(final_state.get(&1), Some(&"Alice Smith".to_string()));
        assert_eq!(final_state.get(&2), Some(&"Bob".to_string()));
    }

    #[test]
    #[cfg(feature = "single-threaded")]
    fn single_threaded_mapping_functions() {
        let projection = make_projection();

        let mapped = projection
            .map_state(
                |count: &usize| {
                    let mut dir = HashMap::new();
                    for i in 0..*count {
                        dir.insert(i as u32, format!("User{}", i));
                    }
                    dir
                },
                |dir: &UserDirectory| dir.len(),
            )
            .map_event(|s: &String| Event::UserRegistered {
                id: 1,
                name: s.clone(),
            });

        let state0 = mapped.initial_state();
        assert_eq!(state0, 0);

        let state1 = mapped.evolve(&state0, &"Alice".to_string());
        assert_eq!(state1, 1);
    }

    #[test]
    #[cfg(not(feature = "single-threaded"))]
    fn projection_multithreaded_send_sync() {
        use std::thread;

        let projection = Projection::new(
            |state: &UserDirectory, event: &Event| {
                let mut new_state = state.clone();
                match event {
                    Event::UserRegistered { id, name } => {
                        new_state.insert(*id, name.clone());
                    }
                    _ => {}
                }
                new_state
            },
            || HashMap::new(),
        );

        let projection = Arc::new(projection);

        let handles: Vec<_> = (1..=4)
            .map(|i| {
                let projection_ref = projection.clone();
                thread::spawn(move || {
                    let state = projection_ref.initial_state();
                    projection_ref.evolve(
                        &state,
                        &Event::UserRegistered {
                            id: i,
                            name: format!("User{}", i),
                        },
                    )
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        for (i, result) in results.iter().enumerate() {
            let expected_id = (i + 1) as u32;
            assert_eq!(result.len(), 1);
            assert_eq!(
                result.get(&expected_id),
                Some(&format!("User{}", expected_id))
            );
        }
    }

    // ------------------------------
    // Merge tests
    // ------------------------------

    #[test]
    fn merge_initial_state() {
        let counter = Projection::new(|state: &u32, _event: &Event| state + 1, || 0u32);

        let merged = make_projection().merge(counter);
        let state = merged.initial_state();
        assert_eq!(state, (HashMap::new(), 0));
    }

    #[test]
    fn merge_evolve_both() {
        let counter = Projection::new(|state: &u32, _event: &Event| state + 1, || 0u32);

        let merged = make_projection().merge(counter);
        let mut state = merged.initial_state();

        let event = Event::UserRegistered {
            id: 1,
            name: "Alice".to_string(),
        };
        state = merged.evolve(&state, &event);

        assert_eq!(state.0.get(&1), Some(&"Alice".to_string()));
        assert_eq!(state.1, 1);

        let event2 = Event::UserDeleted { id: 1 };
        state = merged.evolve(&state, &event2);

        assert!(state.0.is_empty());
        assert_eq!(state.1, 2);
    }
}

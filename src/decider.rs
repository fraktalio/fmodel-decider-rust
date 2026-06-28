use crate::{EventComputationTrait, StateComputationTrait};
use std::convert::Infallible;
use std::marker::PhantomData;
#[cfg(feature = "single-threaded")]
use std::rc::Rc;
#[cfg(not(feature = "single-threaded"))]
use std::sync::Arc;

// ============================================================================
// Multi-threaded AggregateDecider (default)
// ============================================================================

/// # `AggregateDecider` — Traditional Aggregate Pattern (multi-threaded)
///
/// The main entry point for building **traditional DDD aggregates** with strong consistency boundaries.
///
/// ## Key Constraint: Ei = Eo = E
///
/// Unlike `DCBDecider`, this type enforces that **input events = output events = E**.
/// This constraint enables:
/// - **State-stored computation**: Events can be applied back to state via `evolve`
/// - **Event-sourced computation**: Events can be replayed to reconstruct state
/// - **Strong consistency**: All events belong to the same aggregate boundary
///
/// ## Threading
///
/// This variant enforces `Send + Sync` bounds on all behavioral components and uses `Arc`
/// for cheap cloning of transformation functions when composing deciders.
/// For single-threaded use, enable the `single-threaded` feature.
///
/// ## Trait Implementations
///
/// - ✅ **`EventComputationTrait<C, E, E>`**: Event-sourced computation model
/// - ✅ **`StateComputationTrait<C, S>`**: State-stored computation model
#[cfg(not(feature = "single-threaded"))]
pub struct AggregateDecider<
    C,
    S,
    E,
    DecideFn,
    EvolveFn,
    InitFn,
    ResultEvents,
    ResultError = Infallible,
> where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
{
    /// Decision function: `(command, state) -> events`
    pub decide: DecideFn,
    /// Evolution function: `(state, event) -> new_state`
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    _marker: PhantomData<(C, S, E)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
{
    /// Creates a new thread-safe `AggregateDecider`.
    pub fn new(decide: DecideFn, evolve: EvolveFn, initial_state: InitFn) -> Self {
        Self {
            decide,
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
    ) -> AggregateDecider<
        C,
        S2,
        E,
        impl Fn(&C, &S2) -> Result<ResultEvents, ResultError> + Send + Sync,
        impl Fn(&S2, &E) -> S2 + Send + Sync,
        impl Fn() -> S2 + Send + Sync,
        ResultEvents,
        ResultError,
    >
    where
        F1: Fn(&S2) -> S + Send + Sync,
        F2: Fn(&S) -> S2 + Send + Sync,
    {
        let f1 = Arc::new(f1);
        let f2 = Arc::new(f2);

        let f1_decide = Arc::clone(&f1);
        let f1_evolve = Arc::clone(&f1);
        let f2_evolve = Arc::clone(&f2);
        let f2_init = Arc::clone(&f2);

        let decide = move |c: &C, s2: &S2| {
            let s = f1_decide(s2);
            (self.decide)(c, &s)
        };

        let evolve = move |s2: &S2, e: &E| {
            let s = f1_evolve(s2);
            let new_s = (self.evolve)(&s, e);
            f2_evolve(&new_s)
        };

        let initial_state = move || {
            let s = (self.initial_state)();
            f2_init(&s)
        };

        AggregateDecider {
            decide,
            evolve,
            initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the command type `C` to a new command type `C2`.
    pub fn map_command<C2, F>(
        self,
        f: F,
    ) -> AggregateDecider<
        C2,
        S,
        E,
        impl Fn(&C2, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
        EvolveFn,
        InitFn,
        ResultEvents,
        ResultError,
    >
    where
        F: Fn(&C2) -> C + Send + Sync,
    {
        let f = Arc::new(f);

        let decide = move |c2: &C2, s: &S| {
            let c = f(c2);
            (self.decide)(&c, s)
        };

        AggregateDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the event type `E` to a new event type `E2`.
    pub fn map_event<E2, F1, F2, ResultEvents2>(
        self,
        f1: F1,
        f2: F2,
    ) -> AggregateDecider<
        C,
        S,
        E2,
        impl Fn(&C, &S) -> Result<ResultEvents2, ResultError> + Send + Sync,
        impl Fn(&S, &E2) -> S + Send + Sync,
        InitFn,
        ResultEvents2,
        ResultError,
    >
    where
        F1: Fn(&E2) -> E + Send + Sync,
        F2: Fn(&E) -> E2 + Send + Sync,
        E2: Send + Sync,
        ResultEvents: IntoIterator<Item = E>,
        ResultEvents2: IntoIterator<Item = E2> + Send + Sync + FromIterator<E2>,
    {
        let f1 = Arc::new(f1);
        let f2 = Arc::new(f2);

        let f1_evolve = Arc::clone(&f1);
        let f2_decide = Arc::clone(&f2);

        let decide = move |c: &C, s: &S| -> Result<ResultEvents2, ResultError> {
            let events = (self.decide)(c, s)?;
            let mapped_events: ResultEvents2 = events.into_iter().map(|e| f2_decide(&e)).collect();
            Ok(mapped_events)
        };

        let evolve = move |s: &S, e2: &E2| {
            let e = f1_evolve(e2);
            (self.evolve)(s, &e)
        };

        AggregateDecider {
            decide,
            evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the error type `ResultError` to a new error type `ResultError2`.
    pub fn map_error<ResultError2, F>(
        self,
        f: F,
    ) -> AggregateDecider<
        C,
        S,
        E,
        impl Fn(&C, &S) -> Result<ResultEvents, ResultError2> + Send + Sync,
        EvolveFn,
        InitFn,
        ResultEvents,
        ResultError2,
    >
    where
        F: Fn(ResultError) -> ResultError2 + Send + Sync,
    {
        let f = Arc::new(f);

        let decide = move |c: &C, s: &S| -> Result<ResultEvents, ResultError2> {
            (self.decide)(c, s).map_err(|e| f(e))
        };

        AggregateDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Combines two deciders into one bigger decider.
    ///
    /// The combined decider uses:
    /// - `Sum<C, C2>` for commands (sum type — a command targets either the first or second decider)
    /// - `(S, S2)` for state (product type — both states are maintained)
    /// - `Sum<E, E2>` for events (sum type — an event belongs to either the first or second decider)
    /// - `Vec<Sum<E, E2>>` for the result events collection
    ///
    /// The `Clone` bound on `S` and `S2` is required because `evolve` updates only one
    /// half of the state tuple and must clone the other half.
    #[allow(clippy::type_complexity)]
    pub fn combine<C2, S2, E2, DecideFn2, EvolveFn2, InitFn2, ResultEvents2, ResultError2>(
        self,
        other: AggregateDecider<
            C2,
            S2,
            E2,
            DecideFn2,
            EvolveFn2,
            InitFn2,
            ResultEvents2,
            ResultError2,
        >,
    ) -> AggregateDecider<
        crate::Sum<C, C2>,
        (S, S2),
        crate::Sum<E, E2>,
        impl Fn(
            &crate::Sum<C, C2>,
            &(S, S2),
        ) -> Result<Vec<crate::Sum<E, E2>>, crate::Sum<ResultError, ResultError2>>
        + Send
        + Sync,
        impl Fn(&(S, S2), &crate::Sum<E, E2>) -> (S, S2) + Send + Sync,
        impl Fn() -> (S, S2) + Send + Sync,
        Vec<crate::Sum<E, E2>>,
        crate::Sum<ResultError, ResultError2>,
    >
    where
        S: Clone + Send + Sync,
        S2: Clone + Send + Sync,
        C2: Send + Sync,
        E: Send + Sync,
        E2: Send + Sync,
        DecideFn2: Fn(&C2, &S2) -> Result<ResultEvents2, ResultError2> + Send + Sync,
        EvolveFn2: Fn(&S2, &E2) -> S2 + Send + Sync,
        InitFn2: Fn() -> S2 + Send + Sync,
        ResultEvents2: IntoIterator<Item = E2> + Send + Sync,
        ResultError2: Send + Sync,
        ResultError: Send + Sync,
    {
        let decide1 = Arc::new(self.decide);
        let decide2 = Arc::new(other.decide);
        let evolve1 = Arc::new(self.evolve);
        let evolve2 = Arc::new(other.evolve);

        let evolve1_clone = Arc::clone(&evolve1);
        let evolve2_clone = Arc::clone(&evolve2);

        let decide = move |c: &crate::Sum<C, C2>, s: &(S, S2)| match c {
            crate::Sum::First(c1) => decide1(c1, &s.0)
                .map(|evts| evts.into_iter().map(crate::Sum::First).collect())
                .map_err(crate::Sum::First),
            crate::Sum::Second(c2) => decide2(c2, &s.1)
                .map(|evts| evts.into_iter().map(crate::Sum::Second).collect())
                .map_err(crate::Sum::Second),
        };

        let evolve = move |s: &(S, S2), e: &crate::Sum<E, E2>| match e {
            crate::Sum::First(e1) => (evolve1_clone(&s.0, e1), s.1.clone()),
            crate::Sum::Second(e2) => (s.0.clone(), evolve2_clone(&s.1, e2)),
        };

        let initial_state = move || ((self.initial_state)(), (other.initial_state)());

        AggregateDecider::new(decide, evolve, initial_state)
    }
}

// ============================================================================
// Single-threaded AggregateDecider
// ============================================================================

/// # `AggregateDecider` — Traditional Aggregate Pattern (single-threaded)
///
/// The main entry point for building **traditional DDD aggregates** with strong consistency boundaries.
///
/// ## Key Constraint: Ei = Eo = E
///
/// Unlike `DCBDecider`, this type enforces that **input events = output events = E**.
/// This constraint enables:
/// - **State-stored computation**: Events can be applied back to state via `evolve`
/// - **Event-sourced computation**: Events can be replayed to reconstruct state
/// - **Strong consistency**: All events belong to the same aggregate boundary
///
/// ## Threading
///
/// This variant does not impose `Send + Sync` bounds and uses `Rc` instead of `Arc`,
/// avoiding atomic reference counting overhead. For multi-threaded use, disable the
/// `single-threaded` feature (the default).
///
/// ## Trait Implementations
///
/// - ✅ **`EventComputationTrait<C, E, E>`**: Event-sourced computation model
/// - ✅ **`StateComputationTrait<C, S>`**: State-stored computation model
#[cfg(feature = "single-threaded")]
pub struct AggregateDecider<
    C,
    S,
    E,
    DecideFn,
    EvolveFn,
    InitFn,
    ResultEvents,
    ResultError = Infallible,
> where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = E>,
{
    /// Decision function
    pub decide: DecideFn,
    /// Evolution function
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    _marker: PhantomData<(C, S, E)>,
}

#[cfg(feature = "single-threaded")]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = E>,
{
    /// Creates a new single-threaded `AggregateDecider`.
    pub fn new(decide: DecideFn, evolve: EvolveFn, initial_state: InitFn) -> Self {
        Self {
            decide,
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
    ) -> AggregateDecider<
        C,
        S2,
        E,
        impl Fn(&C, &S2) -> Result<ResultEvents, ResultError>,
        impl Fn(&S2, &E) -> S2,
        impl Fn() -> S2,
        ResultEvents,
        ResultError,
    >
    where
        F1: Fn(&S2) -> S,
        F2: Fn(&S) -> S2,
    {
        let f1 = Rc::new(f1);
        let f2 = Rc::new(f2);

        let f1_decide = Rc::clone(&f1);
        let f1_evolve = Rc::clone(&f1);
        let f2_evolve = Rc::clone(&f2);
        let f2_init = Rc::clone(&f2);

        let decide = move |c: &C, s2: &S2| {
            let s = f1_decide(s2);
            (self.decide)(c, &s)
        };

        let evolve = move |s2: &S2, e: &E| {
            let s = f1_evolve(s2);
            let new_s = (self.evolve)(&s, e);
            f2_evolve(&new_s)
        };

        let initial_state = move || {
            let s = (self.initial_state)();
            f2_init(&s)
        };

        AggregateDecider {
            decide,
            evolve,
            initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the command type `C` to a new command type `C2` without thread-safety overhead.
    pub fn map_command<C2, F>(
        self,
        f: F,
    ) -> AggregateDecider<
        C2,
        S,
        E,
        impl Fn(&C2, &S) -> Result<ResultEvents, ResultError>,
        EvolveFn,
        InitFn,
        ResultEvents,
        ResultError,
    >
    where
        F: Fn(&C2) -> C,
    {
        let f = Rc::new(f);

        let decide = move |c2: &C2, s: &S| {
            let c = f(c2);
            (self.decide)(&c, s)
        };

        AggregateDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the event type `E` to a new event type `E2` without thread-safety overhead.
    pub fn map_event<E2, F1, F2, ResultEvents2>(
        self,
        f1: F1,
        f2: F2,
    ) -> AggregateDecider<
        C,
        S,
        E2,
        impl Fn(&C, &S) -> Result<ResultEvents2, ResultError>,
        impl Fn(&S, &E2) -> S,
        InitFn,
        ResultEvents2,
        ResultError,
    >
    where
        F1: Fn(&E2) -> E,
        F2: Fn(&E) -> E2,
        ResultEvents: IntoIterator<Item = E>,
        ResultEvents2: IntoIterator<Item = E2> + FromIterator<E2>,
    {
        let f1 = Rc::new(f1);
        let f2 = Rc::new(f2);

        let f1_evolve = Rc::clone(&f1);
        let f2_decide = Rc::clone(&f2);

        let decide = move |c: &C, s: &S| -> Result<ResultEvents2, ResultError> {
            let events = (self.decide)(c, s)?;
            let mapped_events: ResultEvents2 = events.into_iter().map(|e| f2_decide(&e)).collect();
            Ok(mapped_events)
        };

        let evolve = move |s: &S, e2: &E2| {
            let e = f1_evolve(e2);
            (self.evolve)(s, &e)
        };

        AggregateDecider {
            decide,
            evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the error type `ResultError` to a new error type `ResultError2` without thread-safety overhead.
    pub fn map_error<ResultError2, F>(
        self,
        f: F,
    ) -> AggregateDecider<
        C,
        S,
        E,
        impl Fn(&C, &S) -> Result<ResultEvents, ResultError2>,
        EvolveFn,
        InitFn,
        ResultEvents,
        ResultError2,
    >
    where
        F: Fn(ResultError) -> ResultError2,
    {
        let f = Rc::new(f);

        let decide = move |c: &C, s: &S| -> Result<ResultEvents, ResultError2> {
            (self.decide)(c, s).map_err(|e| f(e))
        };

        AggregateDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Combines two deciders into one bigger decider without thread-safety overhead.
    ///
    /// See the multi-threaded `AggregateDecider::combine` for full documentation.
    #[allow(clippy::type_complexity)]
    pub fn combine<C2, S2, E2, DecideFn2, EvolveFn2, InitFn2, ResultEvents2, ResultError2>(
        self,
        other: AggregateDecider<
            C2,
            S2,
            E2,
            DecideFn2,
            EvolveFn2,
            InitFn2,
            ResultEvents2,
            ResultError2,
        >,
    ) -> AggregateDecider<
        crate::Sum<C, C2>,
        (S, S2),
        crate::Sum<E, E2>,
        impl Fn(
            &crate::Sum<C, C2>,
            &(S, S2),
        ) -> Result<Vec<crate::Sum<E, E2>>, crate::Sum<ResultError, ResultError2>>,
        impl Fn(&(S, S2), &crate::Sum<E, E2>) -> (S, S2),
        impl Fn() -> (S, S2),
        Vec<crate::Sum<E, E2>>,
        crate::Sum<ResultError, ResultError2>,
    >
    where
        S: Clone,
        S2: Clone,
        DecideFn2: Fn(&C2, &S2) -> Result<ResultEvents2, ResultError2>,
        EvolveFn2: Fn(&S2, &E2) -> S2,
        InitFn2: Fn() -> S2,
        ResultEvents2: IntoIterator<Item = E2>,
    {
        let decide1 = Rc::new(self.decide);
        let decide2 = Rc::new(other.decide);
        let evolve1 = Rc::new(self.evolve);
        let evolve2 = Rc::new(other.evolve);

        let evolve1_clone = Rc::clone(&evolve1);
        let evolve2_clone = Rc::clone(&evolve2);

        let decide = move |c: &crate::Sum<C, C2>, s: &(S, S2)| match c {
            crate::Sum::First(c1) => decide1(c1, &s.0)
                .map(|evts| evts.into_iter().map(crate::Sum::First).collect())
                .map_err(crate::Sum::First),
            crate::Sum::Second(c2) => decide2(c2, &s.1)
                .map(|evts| evts.into_iter().map(crate::Sum::Second).collect())
                .map_err(crate::Sum::Second),
        };

        let evolve = move |s: &(S, S2), e: &crate::Sum<E, E2>| match e {
            crate::Sum::First(e1) => (evolve1_clone(&s.0, e1), s.1.clone()),
            crate::Sum::Second(e2) => (s.0.clone(), evolve2_clone(&s.1, e2)),
        };

        let initial_state = move || ((self.initial_state)(), (other.initial_state)());

        AggregateDecider::new(decide, evolve, initial_state)
    }
}

fn fold_events<S, E, F>(mut state: S, events: &[E], evolve: &F) -> S
where
    F: Fn(&S, &E) -> S,
{
    for event in events {
        state = evolve(&state, event);
    }
    state
}

// ============================================================================
// ViewTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> crate::ViewTrait<S, S, E>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
{
    fn evolve(&self, state: &S, event: &E) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> crate::ViewTrait<S, S, E>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = E>,
{
    fn evolve(&self, state: &S, event: &E) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

// ============================================================================
// DeciderTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    crate::DeciderTrait<C, S, S, E, E>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn decide(&self, command: &C, state: &S) -> Result<Self::Events, Self::Error> {
        (self.decide)(command, state)
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    crate::DeciderTrait<C, S, S, E, E>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = E>,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn decide(&self, command: &C, state: &S) -> Result<Self::Events, Self::Error> {
        (self.decide)(command, state)
    }
}

// ============================================================================
// EventComputationTrait and StateComputationTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> EventComputationTrait<C, E, E>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn compute_new_events(
        &self,
        current_events: &[E],
        command: &C,
    ) -> Result<Self::Events, Self::Error> {
        let state = fold_events((self.initial_state)(), current_events, &self.evolve);
        (self.decide)(command, &state)
    }
}

#[cfg(not(feature = "single-threaded"))]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> StateComputationTrait<C, S>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
{
    type Error = ResultError;

    fn compute_new_state(&self, current_state: Option<S>, command: &C) -> Result<S, Self::Error> {
        let base_state = current_state.unwrap_or_else(|| (self.initial_state)());
        let events = (self.decide)(command, &base_state)?;
        Ok(events
            .into_iter()
            .fold(base_state, |state, event| (self.evolve)(&state, &event)))
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> EventComputationTrait<C, E, E>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = E>,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn compute_new_events(
        &self,
        current_events: &[E],
        command: &C,
    ) -> Result<Self::Events, Self::Error> {
        let state = fold_events((self.initial_state)(), current_events, &self.evolve);
        (self.decide)(command, &state)
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> StateComputationTrait<C, S>
    for AggregateDecider<C, S, E, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = E>,
{
    type Error = ResultError;

    fn compute_new_state(&self, current_state: Option<S>, command: &C) -> Result<S, Self::Error> {
        let base_state = current_state.unwrap_or_else(|| (self.initial_state)());
        let events = (self.decide)(command, &base_state)?;
        Ok(events
            .into_iter()
            .fold(base_state, |state, event| (self.evolve)(&state, &event)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeciderTrait, ViewTrait};
    use Event::Incremented;
    use std::convert::Infallible;
    use std::iter::{Once, once};
    #[cfg(not(feature = "single-threaded"))]
    use std::sync::Arc;
    #[cfg(not(feature = "single-threaded"))]
    use std::thread;

    #[derive(Debug)]
    struct Command {
        inc: i32,
    }

    #[derive(Debug, PartialEq)]
    enum Event {
        Incremented(i32),
    }

    /// Helper function to create a standard Decider instance
    fn make_decider() -> AggregateDecider<
        Command,
        i32,
        Event,
        impl Fn(&Command, &i32) -> Result<Once<Event>, Infallible>,
        impl Fn(&i32, &Event) -> i32,
        impl Fn() -> i32,
        Once<Event>,
    > {
        AggregateDecider::new(
            |cmd: &Command, _state: &i32| -> Result<Once<Event>, Infallible> {
                Ok(once(Incremented(cmd.inc)))
            },
            |state: &i32, event: &Event| match event {
                Incremented(n) => state + n,
            },
            || 0,
        )
    }

    #[test]
    fn basic_flow() {
        let decider = make_decider();

        let state0 = (decider.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (decider.decide)(&Command { inc: 5 }, &state0)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![Incremented(5)]);

        let state1 = (decider.evolve)(&state0, &events[0]);
        assert_eq!(state1, 5);
    }

    #[test]
    fn map_state_flow() {
        let decider = make_decider();

        let mapped = decider.map_state(
            |s2: &String| s2.parse::<i32>().unwrap(), // S2 -> S
            |s: &i32| s.to_string(),                  // S -> S2
        );

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, "0");

        let events: Vec<_> = (mapped.decide)(&Command { inc: 3 }, &"10".to_string())
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![Incremented(3)]);

        let state1 = (mapped.evolve)(&"10".to_string(), &events[0]);
        assert_eq!(state1, "13");
    }

    #[test]
    fn map_command_flow() {
        let decider = make_decider();

        // Map from String command to Command
        let mapped = decider.map_command(|s: &String| Command {
            inc: s.parse::<i32>().unwrap(),
        });

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (mapped.decide)(&"5".to_string(), &state0)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![Incremented(5)]);

        let state1 = (mapped.evolve)(&state0, &events[0]);
        assert_eq!(state1, 5);
    }

    #[test]
    fn map_event_flow() {
        let decider = make_decider();

        #[derive(Debug, PartialEq)]
        enum StringEvent {
            IncrementedBy(String),
        }

        // Map between Event and StringEvent
        let mapped = decider.map_event::<StringEvent, _, _, Vec<StringEvent>>(
            |se: &StringEvent| match se {
                StringEvent::IncrementedBy(s) => Incremented(s.parse::<i32>().unwrap()),
            },
            |e: &Event| match e {
                Incremented(n) => StringEvent::IncrementedBy(n.to_string()),
            },
        );

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (mapped.decide)(&Command { inc: 7 }, &state0)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![StringEvent::IncrementedBy("7".to_string())]);

        let state1 = (mapped.evolve)(&state0, &events[0]);
        assert_eq!(state1, 7);
    }

    #[test]
    fn map_error_flow() {
        // Create a decider that can fail
        let failing_decider = AggregateDecider::new(
            |cmd: &Command, _state: &i32| -> Result<Once<Event>, String> {
                if cmd.inc < 0 {
                    Err("Negative increment not allowed".to_string())
                } else {
                    Ok(once(Incremented(cmd.inc)))
                }
            },
            |state: &i32, event: &Event| match event {
                Incremented(n) => state + n,
            },
            || 0,
        );

        // Map String error to i32 error code
        let mapped = failing_decider.map_error(|s: String| s.len() as i32);

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        // Test successful case
        let events: Vec<_> = (mapped.decide)(&Command { inc: 5 }, &state0)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(events, vec![Incremented(5)]);

        // Test error case - error message length becomes error code
        let error = (mapped.decide)(&Command { inc: -1 }, &state0).unwrap_err();
        assert_eq!(error, 30); // Length of "Negative increment not allowed"
    }

    #[test]
    fn composition_flow() {
        let decider = make_decider();

        // Chain multiple mappings
        let composed = decider
            .map_state(
                |s2: &String| s2.parse::<i32>().unwrap(),
                |s: &i32| s.to_string(),
            )
            .map_command(|s: &String| Command {
                inc: s.parse::<i32>().unwrap(),
            });

        let state0 = (composed.initial_state)();
        assert_eq!(state0, "0");

        let events: Vec<_> = (composed.decide)(&"42".to_string(), &"10".to_string())
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![Incremented(42)]);

        let state1 = (composed.evolve)(&"10".to_string(), &events[0]);
        assert_eq!(state1, "52");
    }

    #[test]
    #[cfg(feature = "single-threaded")]
    fn single_threaded_mapping_functions() {
        #[derive(Debug, PartialEq)]
        enum StringEvent {
            IncrementedBy(String),
        }

        let decider = make_decider();

        // Test all mapping functions work in single-threaded mode
        let mapped = decider
            .map_state(
                |s2: &String| s2.parse::<i32>().unwrap(),
                |s: &i32| s.to_string(),
            )
            .map_command(|s: &String| Command {
                inc: s.parse::<i32>().unwrap(),
            })
            .map_event::<StringEvent, _, _, Vec<StringEvent>>(
                |se: &StringEvent| match se {
                    StringEvent::IncrementedBy(s) => Incremented(s.parse::<i32>().unwrap()),
                },
                |e: &Event| match e {
                    Incremented(n) => StringEvent::IncrementedBy(n.to_string()),
                },
            );

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, "0");

        let events: Vec<_> = (mapped.decide)(&"5".to_string(), &"10".to_string())
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![StringEvent::IncrementedBy("5".to_string())]);

        let state1 = (mapped.evolve)(
            &"10".to_string(),
            &StringEvent::IncrementedBy("3".to_string()),
        );
        assert_eq!(state1, "13");
    }

    #[test]
    #[cfg(not(feature = "single-threaded"))]
    fn decider_multithreaded_send_sync() {
        let decider = AggregateDecider::new(
            |cmd: &Command, _state: &i32| -> Result<Once<Event>, Infallible> {
                Ok(once(Incremented(cmd.inc)))
            },
            |state: &i32, event: &Event| match event {
                Incremented(n) => state + n,
            },
            || 0,
        );

        let decider = Arc::new(decider);

        // Spawn multiple threads using Decider
        let handles: Vec<_> = (1..=4)
            .map(|i| {
                let decider_ref = decider.clone();
                thread::spawn(move || {
                    let state = (decider_ref.initial_state)();
                    let events: Vec<_> = (decider_ref.decide)(&Command { inc: i }, &state)
                        .unwrap()
                        .into_iter()
                        .collect();
                    (decider_ref.evolve)(&state, &events[0])
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results, vec![1, 2, 3, 4]);
    }

    #[test]
    fn decider_trait_usage() {
        let decider = make_decider();

        // Test that we can use the decider through the trait
        fn use_decider_trait<D>(d: &D, cmd: &Command, state: &i32) -> Result<i32, D::Error>
        where
            D: DeciderTrait<Command, i32, i32, Event, Event>,
        {
            let events: Vec<_> = d.decide(cmd, state)?.into_iter().collect();
            let mut new_state = *state;
            for event in events {
                new_state = d.evolve(&new_state, &event);
            }
            Ok(new_state)
        }

        let initial_state = decider.initial_state();
        assert_eq!(initial_state, 0);

        let result = use_decider_trait(&decider, &Command { inc: 10 }, &initial_state).unwrap();
        assert_eq!(result, 10);
    }

    #[test]
    fn decider_trait_abstraction() {
        // Test that we can work with deciders abstractly
        let decider1 = make_decider();
        let decider2 = make_decider();

        // Function that works with any DeciderTrait implementation
        fn process_commands<D>(decider: &D, commands: &[Command]) -> Result<i32, D::Error>
        where
            D: DeciderTrait<Command, i32, i32, Event, Event>,
        {
            let mut state = decider.initial_state();

            for command in commands {
                let events: Vec<_> = decider.decide(command, &state)?.into_iter().collect();
                for event in events {
                    state = decider.evolve(&state, &event);
                }
            }

            Ok(state)
        }

        let commands = vec![Command { inc: 5 }, Command { inc: 3 }, Command { inc: 2 }];

        let result1 = process_commands(&decider1, &commands).unwrap();
        let result2 = process_commands(&decider2, &commands).unwrap();

        assert_eq!(result1, 10);
        assert_eq!(result2, 10);
    }

    // ------------------------------
    // Combine tests
    // ------------------------------

    #[derive(Debug, Clone)]
    struct Command2 {
        mul: i32,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Event2 {
        Multiplied(i32),
    }

    fn make_decider2() -> AggregateDecider<
        Command2,
        i32,
        Event2,
        impl Fn(&Command2, &i32) -> Result<Vec<Event2>, Infallible>,
        impl Fn(&i32, &Event2) -> i32,
        impl Fn() -> i32,
        Vec<Event2>,
    > {
        AggregateDecider::new(
            |cmd: &Command2, _state: &i32| -> Result<Vec<Event2>, Infallible> {
                Ok(vec![Event2::Multiplied(cmd.mul)])
            },
            |state: &i32, event: &Event2| match event {
                Event2::Multiplied(n) => state * n,
            },
            || 1,
        )
    }

    #[test]
    fn combine_initial_state() {
        let combined = make_decider().combine(make_decider2());
        let state = (combined.initial_state)();
        assert_eq!(state, (0, 1));
    }

    #[test]
    fn combine_decide_first() {
        let combined = make_decider().combine(make_decider2());
        let state = (combined.initial_state)();

        let events: Vec<_> = (combined.decide)(&crate::Sum::First(Command { inc: 5 }), &state)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![crate::Sum::First(Incremented(5))]);
    }

    #[test]
    fn combine_decide_second() {
        let combined = make_decider().combine(make_decider2());
        let state = (combined.initial_state)();

        let events: Vec<_> = (combined.decide)(&crate::Sum::Second(Command2 { mul: 3 }), &state)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![crate::Sum::Second(Event2::Multiplied(3))]);
    }

    #[test]
    fn combine_evolve() {
        let combined = make_decider().combine(make_decider2());
        let state = (0, 1);

        let s1 = (combined.evolve)(&state, &crate::Sum::First(Incremented(5)));
        assert_eq!(s1, (5, 1));

        let s2 = (combined.evolve)(&s1, &crate::Sum::Second(Event2::Multiplied(3)));
        assert_eq!(s2, (5, 3));
    }

    #[test]
    fn combine_full_flow() {
        let combined = make_decider().combine(make_decider2());
        let mut state = (combined.initial_state)();

        // Send command to first decider
        let events = (combined.decide)(&crate::Sum::First(Command { inc: 10 }), &state).unwrap();
        for e in events {
            state = (combined.evolve)(&state, &e);
        }
        assert_eq!(state, (10, 1));

        // Send command to second decider
        let events = (combined.decide)(&crate::Sum::Second(Command2 { mul: 5 }), &state).unwrap();
        for e in events {
            state = (combined.evolve)(&state, &e);
        }
        assert_eq!(state, (10, 5));
    }
}

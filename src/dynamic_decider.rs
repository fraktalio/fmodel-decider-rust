use crate::EventComputationTrait;
use std::convert::Infallible;
use std::marker::PhantomData;
#[cfg(feature = "single-threaded")]
use std::rc::Rc;
#[cfg(not(feature = "single-threaded"))]
use std::sync::Arc;

// ============================================================================
// Multi-threaded DCBDecider (default)
// ============================================================================

/// # `DCBDecider` — Dynamic Consistency Boundary Pattern (multi-threaded)
///
/// The main entry point for building **cross-boundary event flow** with dynamic consistency
/// boundaries. Enforces `Send + Sync` bounds on all behavioral components and uses `Arc`
/// for cheap cloning when composing deciders.
///
/// ## Key Flexibility: Ei ≠ Eo
///
/// Unlike `AggregateDecider`, this type allows **input events ≠ output events**.
/// This flexibility enables:
/// - **Cross-boundary event flow**: Events from one context, events to another
/// - **Event transformation**: Converting between different event types
/// - **Saga orchestration**: Coordinating workflows across aggregates
/// - **Integration patterns**: Translating internal events to external APIs
///
/// ## Trait Implementations
///
/// - ✅ **`EventComputationTrait<C, Ei, Eo>`**: Event-sourced computation model
/// - ❌ **`StateComputationTrait`**: Not available — output events (`Eo`) cannot be fed
///   back into `evolve` which expects input events (`Ei`) when `Ei ≠ Eo`.
#[cfg(not(feature = "single-threaded"))]
pub struct DCBDecider<
    C,
    S,
    Ei,
    Eo,
    DecideFn,
    EvolveFn,
    InitFn,
    ResultEvents,
    ResultError = Infallible,
> where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &Ei) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = Eo> + Send + Sync,
{
    /// Decision function: `(command, state) -> events`
    pub decide: DecideFn,
    /// Evolution function: `(state, event) -> new_state`
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    _marker: PhantomData<(C, S, Ei, Eo)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &Ei) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = Eo> + Send + Sync,
{
    /// Creates a new thread-safe `DCBDecider`.
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
    ) -> DCBDecider<
        C,
        S2,
        Ei,
        Eo,
        impl Fn(&C, &S2) -> Result<ResultEvents, ResultError> + Send + Sync,
        impl Fn(&S2, &Ei) -> S2 + Send + Sync,
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

        let evolve = move |s2: &S2, e: &Ei| {
            let s = f1_evolve(s2);
            let new_s = (self.evolve)(&s, e);
            f2_evolve(&new_s)
        };

        let initial_state = move || {
            let s = (self.initial_state)();
            f2_init(&s)
        };

        DCBDecider {
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
    ) -> DCBDecider<
        C2,
        S,
        Ei,
        Eo,
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

        DCBDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the input event type `Ei` to a new input event type `Ei2`.
    pub fn map_input_event<Ei2, F>(
        self,
        f: F,
    ) -> DCBDecider<
        C,
        S,
        Ei2,
        Eo,
        DecideFn,
        impl Fn(&S, &Ei2) -> S + Send + Sync,
        InitFn,
        ResultEvents,
        ResultError,
    >
    where
        F: Fn(&Ei2) -> Ei + Send + Sync,
    {
        let f = Arc::new(f);

        let evolve = move |s: &S, ei2: &Ei2| {
            let ei = f(ei2);
            (self.evolve)(s, &ei)
        };

        DCBDecider {
            decide: self.decide,
            evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the output event type `Eo` to a new output event type `Eo2`.
    pub fn map_output_event<Eo2, F, ResultEvents2>(
        self,
        f: F,
    ) -> DCBDecider<
        C,
        S,
        Ei,
        Eo2,
        impl Fn(&C, &S) -> Result<ResultEvents2, ResultError> + Send + Sync,
        EvolveFn,
        InitFn,
        ResultEvents2,
        ResultError,
    >
    where
        F: Fn(&Eo) -> Eo2 + Send + Sync,
        Eo2: Send + Sync,
        ResultEvents: IntoIterator<Item = Eo>,
        ResultEvents2: IntoIterator<Item = Eo2> + Send + Sync + FromIterator<Eo2>,
    {
        let f = Arc::new(f);

        let decide = move |c: &C, s: &S| -> Result<ResultEvents2, ResultError> {
            let events = (self.decide)(c, s)?;
            let mapped_events: ResultEvents2 = events.into_iter().map(|e| f(&e)).collect();
            Ok(mapped_events)
        };

        DCBDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the error type `ResultError` to a new error type `ResultError2`.
    pub fn map_error<ResultError2, F>(
        self,
        f: F,
    ) -> DCBDecider<
        C,
        S,
        Ei,
        Eo,
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

        DCBDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Combines two DCB deciders into one bigger decider.
    ///
    /// The combined decider uses:
    /// - `Sum<C, C2>` for commands
    /// - `(S, S2)` for state (product type)
    /// - `Sum<Ei, Ei2>` for input events
    /// - `Sum<Eo, Eo2>` for output events
    #[allow(clippy::type_complexity)]
    pub fn combine<C2, S2, Ei2, Eo2, DecideFn2, EvolveFn2, InitFn2, ResultEvents2, ResultError2>(
        self,
        other: DCBDecider<
            C2,
            S2,
            Ei2,
            Eo2,
            DecideFn2,
            EvolveFn2,
            InitFn2,
            ResultEvents2,
            ResultError2,
        >,
    ) -> DCBDecider<
        crate::Sum<C, C2>,
        (S, S2),
        crate::Sum<Ei, Ei2>,
        crate::Sum<Eo, Eo2>,
        impl Fn(
            &crate::Sum<C, C2>,
            &(S, S2),
        ) -> Result<Vec<crate::Sum<Eo, Eo2>>, crate::Sum<ResultError, ResultError2>>
        + Send
        + Sync,
        impl Fn(&(S, S2), &crate::Sum<Ei, Ei2>) -> (S, S2) + Send + Sync,
        impl Fn() -> (S, S2) + Send + Sync,
        Vec<crate::Sum<Eo, Eo2>>,
        crate::Sum<ResultError, ResultError2>,
    >
    where
        S: Clone + Send + Sync,
        S2: Clone + Send + Sync,
        C2: Send + Sync,
        Eo: Send + Sync,
        Eo2: Send + Sync,
        DecideFn2: Fn(&C2, &S2) -> Result<ResultEvents2, ResultError2> + Send + Sync,
        EvolveFn2: Fn(&S2, &Ei2) -> S2 + Send + Sync,
        InitFn2: Fn() -> S2 + Send + Sync,
        ResultEvents2: IntoIterator<Item = Eo2> + Send + Sync,
        ResultError2: Send + Sync,
        ResultError: Send + Sync,
    {
        let decide1 = Arc::new(self.decide);
        let decide2 = Arc::new(other.decide);
        let evolve1 = Arc::new(self.evolve);
        let evolve2 = Arc::new(other.evolve);
        let evolve1c = Arc::clone(&evolve1);
        let evolve2c = Arc::clone(&evolve2);

        let decide = move |c: &crate::Sum<C, C2>, s: &(S, S2)| match c {
            crate::Sum::First(c1) => decide1(c1, &s.0)
                .map(|evts| evts.into_iter().map(crate::Sum::First).collect())
                .map_err(crate::Sum::First),
            crate::Sum::Second(c2) => decide2(c2, &s.1)
                .map(|evts| evts.into_iter().map(crate::Sum::Second).collect())
                .map_err(crate::Sum::Second),
        };

        let evolve = move |s: &(S, S2), e: &crate::Sum<Ei, Ei2>| match e {
            crate::Sum::First(e1) => (evolve1c(&s.0, e1), s.1.clone()),
            crate::Sum::Second(e2) => (s.0.clone(), evolve2c(&s.1, e2)),
        };

        let initial_state = move || ((self.initial_state)(), (other.initial_state)());

        DCBDecider::new(decide, evolve, initial_state)
    }
}

// ============================================================================
// Single-threaded DCBDecider
// ============================================================================

/// # `DCBDecider` — Dynamic Consistency Boundary Pattern (single-threaded)
///
/// The main entry point for building **cross-boundary event flow** with dynamic consistency
/// boundaries. Does not impose `Send + Sync` bounds and uses `Rc` instead of `Arc`,
/// avoiding atomic reference counting overhead.
///
/// ## Key Flexibility: Ei ≠ Eo
///
/// Unlike `AggregateDecider`, this type allows **input events ≠ output events**.
///
/// ## Trait Implementations
///
/// - ✅ **`EventComputationTrait<C, Ei, Eo>`**: Event-sourced computation model
/// - ❌ **`StateComputationTrait`**: Not available — see multi-threaded variant docs.
#[cfg(feature = "single-threaded")]
pub struct DCBDecider<
    C,
    S,
    Ei,
    Eo,
    DecideFn,
    EvolveFn,
    InitFn,
    ResultEvents,
    ResultError = Infallible,
> where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &Ei) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = Eo>,
{
    /// Decision function
    pub decide: DecideFn,
    /// Evolution function
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    _marker: PhantomData<(C, S, Ei, Eo)>,
}

#[cfg(feature = "single-threaded")]
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &Ei) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = Eo>,
{
    /// Creates a new single-threaded `DCBDecider`.
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
    ) -> DCBDecider<
        C,
        S2,
        Ei,
        Eo,
        impl Fn(&C, &S2) -> Result<ResultEvents, ResultError>,
        impl Fn(&S2, &Ei) -> S2,
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

        let evolve = move |s2: &S2, e: &Ei| {
            let s = f1_evolve(s2);
            let new_s = (self.evolve)(&s, e);
            f2_evolve(&new_s)
        };

        let initial_state = move || {
            let s = (self.initial_state)();
            f2_init(&s)
        };

        DCBDecider {
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
    ) -> DCBDecider<
        C2,
        S,
        Ei,
        Eo,
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

        DCBDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the input event type `Ei` to a new input event type `Ei2` without thread-safety overhead.
    pub fn map_input_event<Ei2, F>(
        self,
        f: F,
    ) -> DCBDecider<
        C,
        S,
        Ei2,
        Eo,
        DecideFn,
        impl Fn(&S, &Ei2) -> S,
        InitFn,
        ResultEvents,
        ResultError,
    >
    where
        F: Fn(&Ei2) -> Ei,
    {
        let f = Rc::new(f);

        let evolve = move |s: &S, ei2: &Ei2| {
            let ei = f(ei2);
            (self.evolve)(s, &ei)
        };

        DCBDecider {
            decide: self.decide,
            evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the output event type `Eo` to a new output event type `Eo2` without thread-safety overhead.
    pub fn map_output_event<Eo2, F, ResultEvents2>(
        self,
        f: F,
    ) -> DCBDecider<
        C,
        S,
        Ei,
        Eo2,
        impl Fn(&C, &S) -> Result<ResultEvents2, ResultError>,
        EvolveFn,
        InitFn,
        ResultEvents2,
        ResultError,
    >
    where
        F: Fn(&Eo) -> Eo2,
        ResultEvents: IntoIterator<Item = Eo>,
        ResultEvents2: IntoIterator<Item = Eo2> + FromIterator<Eo2>,
    {
        let f = Rc::new(f);

        let decide = move |c: &C, s: &S| -> Result<ResultEvents2, ResultError> {
            let events = (self.decide)(c, s)?;
            let mapped_events: ResultEvents2 = events.into_iter().map(|e| f(&e)).collect();
            Ok(mapped_events)
        };

        DCBDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Maps the error type `ResultError` to a new error type `ResultError2` without thread-safety overhead.
    pub fn map_error<ResultError2, F>(
        self,
        f: F,
    ) -> DCBDecider<
        C,
        S,
        Ei,
        Eo,
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

        DCBDecider {
            decide,
            evolve: self.evolve,
            initial_state: self.initial_state,
            _marker: PhantomData,
        }
    }

    /// Combines two DCB deciders into one bigger decider without thread-safety overhead.
    ///
    /// See the multi-threaded `DCBDecider::combine` for full documentation.
    #[allow(clippy::type_complexity)]
    pub fn combine<C2, S2, Ei2, Eo2, DecideFn2, EvolveFn2, InitFn2, ResultEvents2, ResultError2>(
        self,
        other: DCBDecider<
            C2,
            S2,
            Ei2,
            Eo2,
            DecideFn2,
            EvolveFn2,
            InitFn2,
            ResultEvents2,
            ResultError2,
        >,
    ) -> DCBDecider<
        crate::Sum<C, C2>,
        (S, S2),
        crate::Sum<Ei, Ei2>,
        crate::Sum<Eo, Eo2>,
        impl Fn(
            &crate::Sum<C, C2>,
            &(S, S2),
        ) -> Result<Vec<crate::Sum<Eo, Eo2>>, crate::Sum<ResultError, ResultError2>>,
        impl Fn(&(S, S2), &crate::Sum<Ei, Ei2>) -> (S, S2),
        impl Fn() -> (S, S2),
        Vec<crate::Sum<Eo, Eo2>>,
        crate::Sum<ResultError, ResultError2>,
    >
    where
        S: Clone,
        S2: Clone,
        DecideFn2: Fn(&C2, &S2) -> Result<ResultEvents2, ResultError2>,
        EvolveFn2: Fn(&S2, &Ei2) -> S2,
        InitFn2: Fn() -> S2,
        ResultEvents2: IntoIterator<Item = Eo2>,
    {
        let decide1 = Rc::new(self.decide);
        let decide2 = Rc::new(other.decide);
        let evolve1 = Rc::new(self.evolve);
        let evolve2 = Rc::new(other.evolve);
        let evolve1c = Rc::clone(&evolve1);
        let evolve2c = Rc::clone(&evolve2);

        let decide = move |c: &crate::Sum<C, C2>, s: &(S, S2)| match c {
            crate::Sum::First(c1) => decide1(c1, &s.0)
                .map(|evts| evts.into_iter().map(crate::Sum::First).collect())
                .map_err(crate::Sum::First),
            crate::Sum::Second(c2) => decide2(c2, &s.1)
                .map(|evts| evts.into_iter().map(crate::Sum::Second).collect())
                .map_err(crate::Sum::Second),
        };

        let evolve = move |s: &(S, S2), e: &crate::Sum<Ei, Ei2>| match e {
            crate::Sum::First(e1) => (evolve1c(&s.0, e1), s.1.clone()),
            crate::Sum::Second(e2) => (s.0.clone(), evolve2c(&s.1, e2)),
        };

        let initial_state = move || ((self.initial_state)(), (other.initial_state)());

        DCBDecider::new(decide, evolve, initial_state)
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
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> crate::ViewTrait<S, S, Ei>
    for DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &Ei) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = Eo> + Send + Sync,
{
    fn evolve(&self, state: &S, event: &Ei) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError> crate::ViewTrait<S, S, Ei>
    for DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &Ei) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = Eo>,
{
    fn evolve(&self, state: &S, event: &Ei) -> S {
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
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    crate::DeciderTrait<C, S, S, Ei, Eo>
    for DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &Ei) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = Eo> + Send + Sync,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn decide(&self, command: &C, state: &S) -> Result<Self::Events, Self::Error> {
        (self.decide)(command, state)
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    crate::DeciderTrait<C, S, S, Ei, Eo>
    for DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &Ei) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = Eo>,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn decide(&self, command: &C, state: &S) -> Result<Self::Events, Self::Error> {
        (self.decide)(command, state)
    }
}

// ============================================================================
// EventComputationTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    EventComputationTrait<C, Ei, Eo>
    for DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &Ei) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ResultEvents: IntoIterator<Item = Eo> + Send + Sync,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn compute_new_events(
        &self,
        current_events: &[Ei],
        command: &C,
    ) -> Result<Self::Events, Self::Error> {
        let state = fold_events((self.initial_state)(), current_events, &self.evolve);
        (self.decide)(command, &state)
    }
}

#[cfg(feature = "single-threaded")]
impl<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
    EventComputationTrait<C, Ei, Eo>
    for DCBDecider<C, S, Ei, Eo, DecideFn, EvolveFn, InitFn, ResultEvents, ResultError>
where
    DecideFn: Fn(&C, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &Ei) -> S,
    InitFn: Fn() -> S,
    ResultEvents: IntoIterator<Item = Eo>,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn compute_new_events(
        &self,
        current_events: &[Ei],
        command: &C,
    ) -> Result<Self::Events, Self::Error> {
        let state = fold_events((self.initial_state)(), current_events, &self.evolve);
        (self.decide)(command, &state)
    }
}

// ============================================================================
// StateComputationTrait is NOT implemented for DCBDecider
// ============================================================================
//
// When Ei != Eo, output events cannot be fed back into `evolve` (which expects Ei).
// For state-stored patterns, use AggregateDecider where Ei = Eo = E.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::iter::{Once, once};

    #[derive(Debug)]
    struct Command {
        inc: i32,
    }

    #[derive(Debug, PartialEq)]
    enum InputEvent {
        Incremented(i32),
    }

    #[derive(Debug, PartialEq)]
    enum OutputEvent {
        StateChanged(i32),
    }

    fn make_dynamic_decider() -> DCBDecider<
        Command,
        i32,
        InputEvent,
        OutputEvent,
        impl Fn(&Command, &i32) -> Result<Once<OutputEvent>, Infallible>,
        impl Fn(&i32, &InputEvent) -> i32,
        impl Fn() -> i32,
        Once<OutputEvent>,
    > {
        DCBDecider::new(
            |cmd: &Command, state: &i32| -> Result<Once<OutputEvent>, Infallible> {
                Ok(once(OutputEvent::StateChanged(state + cmd.inc)))
            },
            |state: &i32, event: &InputEvent| match event {
                InputEvent::Incremented(n) => state + n,
            },
            || 0,
        )
    }

    #[test]
    fn basic_dynamic_flow() {
        let decider = make_dynamic_decider();

        let state0 = (decider.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (decider.decide)(&Command { inc: 5 }, &state0)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![OutputEvent::StateChanged(5)]);

        let state1 = (decider.evolve)(&state0, &InputEvent::Incremented(3));
        assert_eq!(state1, 3);
    }

    #[test]
    fn map_state_flow() {
        let decider = make_dynamic_decider();

        let mapped = decider.map_state(
            |s2: &String| s2.parse::<i32>().unwrap(),
            |s: &i32| s.to_string(),
        );

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, "0");

        let events: Vec<_> = (mapped.decide)(&Command { inc: 3 }, &"10".to_string())
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![OutputEvent::StateChanged(13)]);

        let state1 = (mapped.evolve)(&"10".to_string(), &InputEvent::Incremented(5));
        assert_eq!(state1, "15");
    }

    #[test]
    fn map_command_flow() {
        let decider = make_dynamic_decider();

        let mapped = decider.map_command(|s: &String| Command {
            inc: s.parse::<i32>().unwrap(),
        });

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (mapped.decide)(&"5".to_string(), &state0)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(events, vec![OutputEvent::StateChanged(5)]);
    }

    #[test]
    fn map_input_event_flow() {
        let decider = make_dynamic_decider();

        #[derive(Debug, PartialEq)]
        enum StringInputEvent {
            IncrementedBy(String),
        }

        let mapped = decider.map_input_event(|se: &StringInputEvent| match se {
            StringInputEvent::IncrementedBy(s) => {
                InputEvent::Incremented(s.parse::<i32>().unwrap())
            }
        });

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        let state1 = (mapped.evolve)(&state0, &StringInputEvent::IncrementedBy("7".to_string()));
        assert_eq!(state1, 7);
    }

    #[test]
    fn map_output_event_flow() {
        let decider = make_dynamic_decider();

        #[derive(Debug, PartialEq)]
        enum StringOutputEvent {
            StateChangedTo(String),
        }

        let mapped = decider.map_output_event::<StringOutputEvent, _, Vec<StringOutputEvent>>(
            |oe: &OutputEvent| match oe {
                OutputEvent::StateChanged(n) => StringOutputEvent::StateChangedTo(n.to_string()),
            },
        );

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (mapped.decide)(&Command { inc: 7 }, &state0)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            events,
            vec![StringOutputEvent::StateChangedTo("7".to_string())]
        );
    }

    #[test]
    fn map_error_flow() {
        let failing_decider = DCBDecider::new(
            |cmd: &Command, _state: &i32| -> Result<Once<OutputEvent>, String> {
                if cmd.inc < 0 {
                    Err("Negative increment not allowed".to_string())
                } else {
                    Ok(once(OutputEvent::StateChanged(cmd.inc)))
                }
            },
            |state: &i32, event: &InputEvent| match event {
                InputEvent::Incremented(n) => state + n,
            },
            || 0,
        );

        let mapped = failing_decider.map_error(|s: String| s.len() as i32);

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, 0);

        let events: Vec<_> = (mapped.decide)(&Command { inc: 5 }, &state0)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(events, vec![OutputEvent::StateChanged(5)]);

        let error = (mapped.decide)(&Command { inc: -1 }, &state0).unwrap_err();
        assert_eq!(error, 30);
    }

    #[test]
    fn composition_flow() {
        let decider = make_dynamic_decider();

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

        assert_eq!(events, vec![OutputEvent::StateChanged(52)]);
    }

    #[test]
    #[cfg(feature = "single-threaded")]
    fn single_threaded_mapping_functions() {
        #[derive(Debug, PartialEq)]
        enum StringInputEvent {
            IncrementedBy(String),
        }

        #[derive(Debug, PartialEq)]
        enum StringOutputEvent {
            StateChangedTo(String),
        }

        let decider = make_dynamic_decider();

        let mapped = decider
            .map_state(
                |s2: &String| s2.parse::<i32>().unwrap(),
                |s: &i32| s.to_string(),
            )
            .map_command(|s: &String| Command {
                inc: s.parse::<i32>().unwrap(),
            })
            .map_input_event(|se: &StringInputEvent| match se {
                StringInputEvent::IncrementedBy(s) => {
                    InputEvent::Incremented(s.parse::<i32>().unwrap())
                }
            })
            .map_output_event::<StringOutputEvent, _, Vec<StringOutputEvent>>(
                |oe: &OutputEvent| match oe {
                    OutputEvent::StateChanged(n) => {
                        StringOutputEvent::StateChangedTo(n.to_string())
                    }
                },
            );

        let state0 = (mapped.initial_state)();
        assert_eq!(state0, "0");

        let events: Vec<_> = (mapped.decide)(&"5".to_string(), &"10".to_string())
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            events,
            vec![StringOutputEvent::StateChangedTo("15".to_string())]
        );

        let state1 = (mapped.evolve)(
            &"10".to_string(),
            &StringInputEvent::IncrementedBy("3".to_string()),
        );
        assert_eq!(state1, "13");
    }

    // ------------------------------
    // Combine tests
    // ------------------------------

    #[derive(Debug)]
    struct Command2 {
        val: i32,
    }

    #[derive(Debug, PartialEq)]
    enum InputEvent2 {
        Set(i32),
    }

    #[derive(Debug, PartialEq)]
    enum OutputEvent2 {
        WasSet(i32),
    }

    fn make_dynamic_decider2() -> DCBDecider<
        Command2,
        i32,
        InputEvent2,
        OutputEvent2,
        impl Fn(&Command2, &i32) -> Result<Vec<OutputEvent2>, Infallible>,
        impl Fn(&i32, &InputEvent2) -> i32,
        impl Fn() -> i32,
        Vec<OutputEvent2>,
    > {
        DCBDecider::new(
            |cmd: &Command2, _state: &i32| -> Result<Vec<OutputEvent2>, Infallible> {
                Ok(vec![OutputEvent2::WasSet(cmd.val)])
            },
            |_state: &i32, event: &InputEvent2| match event {
                InputEvent2::Set(n) => *n,
            },
            || 0,
        )
    }

    #[test]
    fn combine_initial_state() {
        let combined = make_dynamic_decider().combine(make_dynamic_decider2());
        assert_eq!((combined.initial_state)(), (0, 0));
    }

    #[test]
    fn combine_decide_first() {
        let combined = make_dynamic_decider().combine(make_dynamic_decider2());
        let state = (combined.initial_state)();

        let events: Vec<_> =
            (combined.decide)(&crate::Sum::First(Command { inc: 5 }), &state).unwrap();

        assert_eq!(
            events,
            vec![crate::Sum::First(OutputEvent::StateChanged(5))]
        );
    }

    #[test]
    fn combine_decide_second() {
        let combined = make_dynamic_decider().combine(make_dynamic_decider2());
        let state = (combined.initial_state)();

        let events: Vec<_> =
            (combined.decide)(&crate::Sum::Second(Command2 { val: 42 }), &state).unwrap();

        assert_eq!(events, vec![crate::Sum::Second(OutputEvent2::WasSet(42))]);
    }

    #[test]
    fn combine_evolve() {
        let combined = make_dynamic_decider().combine(make_dynamic_decider2());
        let state = (0, 0);

        let s1 = (combined.evolve)(&state, &crate::Sum::First(InputEvent::Incremented(5)));
        assert_eq!(s1, (5, 0));

        let s2 = (combined.evolve)(&s1, &crate::Sum::Second(InputEvent2::Set(99)));
        assert_eq!(s2, (5, 99));
    }
}

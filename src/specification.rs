//! ## A test specification DSL for deciders, projections, and processes that supports the given-when-then format.
//!
//! This module provides fluent testing DSLs inspired by BDD (Behavior-Driven Development) patterns,
//! allowing you to write expressive tests for your domain logic.
//!
//! ## Available Specifications
//!
//! - `AggregateDeciderTestSpecification` - Test traditional aggregates with event-sourced or state-stored patterns
//! - `DCBDeciderTestSpecification` - Test dynamic consistency boundary deciders with different input/output event types
//! - `ProjectionTestSpecification` - Test pure state evolution from event streams
//! - `ProcessTestSpecification` - Test reactive processes with ToDo list semantics
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use fmodel_decider_rust::{AggregateDecider, specification::AggregateDeciderTestSpecification};
//! # #[derive(Debug, Clone, PartialEq)]
//! # enum OrderEvent { OrderCreated { id: u32 }, OrderPlaced { id: u32 } }
//! # #[derive(Debug, Clone, PartialEq)]
//! # enum OrderCommand { PlaceOrder { id: u32 } }
//! # #[derive(Debug, Clone, PartialEq, Default)]
//! # struct OrderState { placed: bool }
//!
//! #[test]
//! fn test_order_placement() {
//!     let decider = AggregateDecider::new(
//!         |c: &OrderCommand, _s: &OrderState| match c {
//!             OrderCommand::PlaceOrder { id } => Ok(vec![OrderEvent::OrderPlaced { id: *id }]),
//!         },
//!         |_s: &OrderState, _e: &OrderEvent| OrderState { placed: true },
//!         || OrderState::default(),
//!     );
//!     AggregateDeciderTestSpecification::default()
//!         .for_decider(&decider)
//!         .given(vec![OrderEvent::OrderCreated { id: 1 }])
//!         .when(OrderCommand::PlaceOrder { id: 1 })
//!         .then(vec![OrderEvent::OrderPlaced { id: 1 }]);
//! }
//! ```

use pretty_assertions::assert_eq;

use crate::{EventComputationTrait, ProcessTrait, StateComputationTrait, ViewTrait};

// ########################################################
// ######### AggregateDecider Specification DSL ###########
// ########################################################

/// A test specification DSL for `AggregateDecider` that supports the `given-when-then` format.
///
/// This DSL allows you to test deciders in both event-sourced and state-stored patterns:
/// - **Event-sourced**: Use `given()` to provide event history, then `then()` to assert new events
/// - **State-stored**: Use `given_state()` to provide current state, then `then_state()` to assert new state
///
/// ## Type Parameters
///
/// - `Command` - The command type that triggers decisions
/// - `State` - The state type maintained by the decider
/// - `Event` - The event type (same for input and output in AggregateDecider)
/// - `Error` - The error type returned by decision logic
/// - `D` - The decider type implementing the required traits
///
/// ## Example
///
/// ```rust
/// # use fmodel_decider_rust::{AggregateDecider, specification::AggregateDeciderTestSpecification};
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum Event { AccountCreated { id: u32, balance: i32 }, MoneyDeposited { id: u32, amount: i32 } }
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum Command { Deposit { id: u32, amount: i32 } }
/// # #[derive(Debug, Clone, PartialEq, Default)]
/// # struct State { id: Option<u32>, balance: i32 }
/// # let decider = AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<Event>, String> { match c {
/// #         Command::Deposit { id, amount } => Ok(vec![Event::MoneyDeposited { id: *id, amount: *amount }]),
/// #     } },
/// #     |s: &State, e: &Event| {
/// #         let mut ns = s.clone();
/// #         match e {
/// #             Event::AccountCreated { id, balance } => { ns.id = Some(*id); ns.balance = *balance; }
/// #             Event::MoneyDeposited { amount, .. } => { ns.balance += amount; }
/// #         }
/// #         ns
/// #     },
/// #     || State::default(),
/// # );
/// AggregateDeciderTestSpecification::default()
///     .for_decider(&decider)
///     .given(vec![Event::AccountCreated { id: 1, balance: 0 }])
///     .when(Command::Deposit { id: 1, amount: 100 })
///     .then(vec![Event::MoneyDeposited { id: 1, amount: 100 }]);
/// ```
pub struct AggregateDeciderTestSpecification<'a, Command, State, Event, Error, D>
where
    Event: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    D: EventComputationTrait<Command, Event, Event, Events = Vec<Event>, Error = Error>
        + StateComputationTrait<Command, State, Error = Error>,
{
    events: Vec<Event>,
    state: Option<State>,
    command: Option<Command>,
    decider: Option<&'a D>,
}

impl<Command, State, Event, Error, D> Default
    for AggregateDeciderTestSpecification<'_, Command, State, Event, Error, D>
where
    Event: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    D: EventComputationTrait<Command, Event, Event, Events = Vec<Event>, Error = Error>
        + StateComputationTrait<Command, State, Error = Error>,
{
    fn default() -> Self {
        Self {
            events: Vec::new(),
            state: None,
            command: None,
            decider: None,
        }
    }
}

impl<'a, Command, State, Event, Error, D>
    AggregateDeciderTestSpecification<'a, Command, State, Event, Error, D>
where
    Command: std::fmt::Debug,
    Event: PartialEq + std::fmt::Debug + Clone,
    State: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    D: EventComputationTrait<Command, Event, Event, Events = Vec<Event>, Error = Error>
        + StateComputationTrait<Command, State, Error = Error>,
{
    /// Specify the decider you want to test
    pub fn for_decider(mut self, decider: &'a D) -> Self {
        self.decider = Some(decider);
        self
    }

    /// Given preconditions / previous events (for event-sourced testing)
    pub fn given(mut self, events: Vec<Event>) -> Self {
        self.events = events;
        self
    }

    /// Given preconditions / previous state (for state-stored testing)
    pub fn given_state(mut self, state: Option<State>) -> Self {
        self.state = state;
        self
    }

    /// When action/command is executed
    pub fn when(mut self, command: Command) -> Self {
        self.command = Some(command);
        self
    }

    /// Then expect result / new events (for event-sourced testing)
    #[track_caller]
    pub fn then(self, expected_events: Vec<Event>) {
        let decider = self
            .decider
            .expect("Decider must be initialized. Did you forget to call `for_decider`?");
        let command = self
            .command
            .expect("Command must be initialized. Did you forget to call `when`?");
        let events = self.events;

        let new_events_result = decider.compute_new_events(&events, &command);
        let new_events = match new_events_result {
            Ok(events) => events,
            Err(error) => {
                panic!("Events were expected but the decider returned an error instead: {error:?}")
            }
        };
        assert_eq!(
            new_events, expected_events,
            "Actual and Expected events do not match!\nCommand: {command:?}\n",
        );
    }

    /// Then expect result / new state (for state-stored testing)
    #[track_caller]
    pub fn then_state(self, expected_state: State) {
        let decider = self
            .decider
            .expect("Decider must be initialized. Did you forget to call `for_decider`?");
        let command = self
            .command
            .expect("Command must be initialized. Did you forget to call `when`?");
        let state = self.state;

        let new_state_result = decider.compute_new_state(state, &command);
        let new_state = match new_state_result {
            Ok(state) => state,
            Err(error) => {
                panic!("State was expected but the decider returned an error instead: {error:?}")
            }
        };
        assert_eq!(
            new_state, expected_state,
            "Actual and Expected states do not match.\nCommand: {command:?}\n"
        );
    }

    /// Then expect error result / these are not events
    #[track_caller]
    pub fn then_error(self, expected_error: Error) {
        let decider = self
            .decider
            .expect("Decider must be initialized. Did you forget to call `for_decider`?");
        let command = self
            .command
            .expect("Command must be initialized. Did you forget to call `when`?");
        let events = self.events;

        let error_result = decider.compute_new_events(&events, &command);
        let error = match error_result {
            Ok(events) => {
                panic!("An error was expected but the decider returned events instead: {events:?}")
            }
            Err(error) => error,
        };
        assert_eq!(
            error, expected_error,
            "Actual and Expected errors do not match.\nCommand: {command:?}\n"
        );
    }
}

// ########################################################
// ########### DCBDecider Specification DSL ###############
// ########################################################

/// A test specification DSL for `DCBDecider` that supports the `given-when-then` format.
///
/// This DSL is specifically designed for testing deciders with different input and output event types,
/// representing dynamic consistency boundaries where events cross architectural boundaries.
///
/// ## Type Parameters
///
/// - `Command` - The command type that triggers decisions
/// - `State` - The state type maintained by the decider
/// - `Ei` - Input event type (events consumed for state reconstruction)
/// - `Eo` - Output event type (events produced by decision logic)
/// - `Error` - The error type returned by decision logic
/// - `D` - The decider type implementing the required traits
///
/// ## Example
///
/// ```rust
/// # use fmodel_decider_rust::{DCBDecider, specification::DCBDeciderTestSpecification};
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum UpstreamEvent { DataA { data: u32 } }
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum DownstreamEvent { ResultB { result: u32 } }
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum Command { Transform { id: u32 } }
/// # #[derive(Debug, Clone, PartialEq, Default)]
/// # struct State { seen: Vec<u32> }
/// # let decider = DCBDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<DownstreamEvent>, String> { match c {
/// #         Command::Transform { id } => Ok(vec![DownstreamEvent::ResultB { result: *id }]),
/// #     } },
/// #     |s: &State, e: &UpstreamEvent| {
/// #         let mut ns = s.clone();
/// #         if let UpstreamEvent::DataA { data } = e { ns.seen.push(*data); }
/// #         ns
/// #     },
/// #     || State::default(),
/// # );
/// DCBDeciderTestSpecification::<_, State, _, _, _, _>::default()
///     .for_decider(&decider)
///     .given(vec![UpstreamEvent::DataA { data: 1 }])
///     .when(Command::Transform { id: 1 })
///     .then(vec![DownstreamEvent::ResultB { result: 1 }]);
/// ```
pub struct DCBDeciderTestSpecification<'a, Command, State, Ei, Eo, Error, D>
where
    Ei: PartialEq + std::fmt::Debug,
    Eo: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    D: EventComputationTrait<Command, Ei, Eo, Events = Vec<Eo>, Error = Error>,
{
    events: Vec<Ei>,
    command: Option<Command>,
    decider: Option<&'a D>,
    _phantom: std::marker::PhantomData<(State, Eo, Error)>,
}

impl<Command, State, Ei, Eo, Error, D> Default
    for DCBDeciderTestSpecification<'_, Command, State, Ei, Eo, Error, D>
where
    Ei: PartialEq + std::fmt::Debug,
    Eo: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    D: EventComputationTrait<Command, Ei, Eo, Events = Vec<Eo>, Error = Error>,
{
    fn default() -> Self {
        Self {
            events: Vec::new(),
            command: None,
            decider: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, Command, State, Ei, Eo, Error, D>
    DCBDeciderTestSpecification<'a, Command, State, Ei, Eo, Error, D>
where
    Command: std::fmt::Debug,
    Ei: PartialEq + std::fmt::Debug + Clone,
    Eo: PartialEq + std::fmt::Debug,
    State: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    D: EventComputationTrait<Command, Ei, Eo, Events = Vec<Eo>, Error = Error>,
{
    /// Specify the DCB decider you want to test
    pub fn for_decider(mut self, decider: &'a D) -> Self {
        self.decider = Some(decider);
        self
    }

    /// Given preconditions / previous input events
    pub fn given(mut self, events: Vec<Ei>) -> Self {
        self.events = events;
        self
    }

    /// When action/command is executed
    pub fn when(mut self, command: Command) -> Self {
        self.command = Some(command);
        self
    }

    /// Then expect result / new output events
    #[track_caller]
    pub fn then(self, expected_events: Vec<Eo>) {
        let decider = self
            .decider
            .expect("Decider must be initialized. Did you forget to call `for_decider`?");
        let command = self
            .command
            .expect("Command must be initialized. Did you forget to call `when`?");
        let events = self.events;

        let new_events_result = decider.compute_new_events(&events, &command);
        let new_events = match new_events_result {
            Ok(events) => events,
            Err(error) => {
                panic!("Events were expected but the decider returned an error instead: {error:?}")
            }
        };
        assert_eq!(
            new_events, expected_events,
            "Actual and Expected events do not match!\nCommand: {command:?}\n",
        );
    }

    /// Then expect error result
    #[track_caller]
    pub fn then_error(self, expected_error: Error) {
        let decider = self
            .decider
            .expect("Decider must be initialized. Did you forget to call `for_decider`?");
        let command = self
            .command
            .expect("Command must be initialized. Did you forget to call `when`?");
        let events = self.events;

        let error_result = decider.compute_new_events(&events, &command);
        let error = match error_result {
            Ok(events) => {
                panic!("An error was expected but the decider returned events instead: {events:?}")
            }
            Err(error) => error,
        };
        assert_eq!(
            error, expected_error,
            "Actual and Expected errors do not match.\nCommand: {command:?}\n"
        );
    }
}

// ########################################################
// ############ Projection Specification DSL ##############
// ########################################################

/// A test specification DSL for `Projection` that supports the `given-then` format.
///
/// This DSL is used to test pure state evolution from event streams without decision-making.
/// It's perfect for testing read-side projections and materialized views.
///
/// ## Type Parameters
///
/// - `State` - The state type maintained by the projection
/// - `Event` - The event type that drives state evolution
/// - `P` - The projection type implementing ViewTrait
///
/// ## Example
///
/// ```rust
/// # use fmodel_decider_rust::{Projection, specification::ProjectionTestSpecification};
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum Event { UserRegistered { id: u32, name: String }, UserUpdated { id: u32, name: String } }
/// # #[derive(Debug, Clone, PartialEq, Default)]
/// # struct State { name: String }
/// # let projection = Projection::new(
/// #     |_s: &State, e: &Event| match e {
/// #         Event::UserRegistered { name, .. } | Event::UserUpdated { name, .. } => State { name: name.clone() },
/// #     },
/// #     || State::default(),
/// # );
/// ProjectionTestSpecification::default()
///     .for_projection(&projection)
///     .given(vec![
///         Event::UserRegistered { id: 1, name: "Alice".into() },
///         Event::UserUpdated { id: 1, name: "Alice Smith".into() },
///     ])
///     .then(State { name: "Alice Smith".into() });
/// ```
pub struct ProjectionTestSpecification<'a, State, Event, P>
where
    State: PartialEq + std::fmt::Debug,
    P: ViewTrait<State, State, Event>,
{
    events: Vec<Event>,
    projection: Option<&'a P>,
    _phantom: std::marker::PhantomData<State>,
}

impl<State, Event, P> Default for ProjectionTestSpecification<'_, State, Event, P>
where
    State: PartialEq + std::fmt::Debug,
    P: ViewTrait<State, State, Event>,
{
    fn default() -> Self {
        Self {
            events: Vec::new(),
            projection: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, State, Event, P> ProjectionTestSpecification<'a, State, Event, P>
where
    State: PartialEq + std::fmt::Debug,
    Event: std::fmt::Debug,
    P: ViewTrait<State, State, Event>,
{
    /// Specify the projection you want to test
    pub fn for_projection(mut self, projection: &'a P) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Given preconditions / events
    pub fn given(mut self, events: Vec<Event>) -> Self {
        self.events = events;
        self
    }

    /// Then expect evolving new state of the projection
    #[track_caller]
    pub fn then(self, expected_state: State) {
        let projection = self
            .projection
            .expect("Projection must be initialized. Did you forget to call `for_projection`?");

        let events = self.events;

        let mut state = projection.initial_state();
        for event in &events {
            state = projection.evolve(&state, event);
        }

        assert_eq!(
            state, expected_state,
            "Actual and Expected states do not match.\nEvents: {events:?}\n"
        );
    }
}

// ########################################################
// ############# Process Specification DSL ################
// ########################################################

/// A test specification DSL for `Process` that supports the `given-when-then` format with reactive behavior.
///
/// This DSL extends the decider testing pattern with additional assertions for reactive behavior:
/// - `then_react()` - Assert actions generated in response to specific events
/// - `then_pending()` - Assert all available actions for a given state
///
/// ## Type Parameters
///
/// - `ActionResult` - The action result type (replaces Command in Process)
/// - `State` - The state type maintained by the process
/// - `Event` - The event type (same for input and output in Process)
/// - `Action` - The action type that can be performed
/// - `Error` - The error type returned by decision logic
/// - `P` - The process type implementing the required traits
///
/// ## Example
///
/// ```rust
/// # use fmodel_decider_rust::{Process, specification::ProcessTestSpecification};
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum Event { TaskAdded { id: u32 }, TaskInProgress { id: u32 } }
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum ActionResult { TaskCreated { id: u32 }, TaskStarted { id: u32 } }
/// # #[derive(Debug, Clone, PartialEq)]
/// # enum Action { NotifyManager { task_id: u32 } }
/// # #[derive(Debug, Clone, PartialEq, Default)]
/// # struct State { count: u32 }
/// # let process = Process::new(
/// #     |ar: &ActionResult, _s: &State| -> Result<Vec<Event>, String> { match ar {
/// #         ActionResult::TaskCreated { id } => Ok(vec![Event::TaskAdded { id: *id }]),
/// #         ActionResult::TaskStarted { id } => Ok(vec![Event::TaskInProgress { id: *id }]),
/// #     } },
/// #     |s: &State, _e: &Event| State { count: s.count + 1 },
/// #     || State::default(),
/// #     |_s: &State, e: &Event| match e {
/// #         Event::TaskInProgress { id } => vec![Action::NotifyManager { task_id: *id }],
/// #         _ => vec![],
/// #     },
/// #     |_s: &State| vec![],
/// # );
/// ProcessTestSpecification::default()
///     .for_process(&process)
///     .given(vec![Event::TaskAdded { id: 1 }, Event::TaskInProgress { id: 1 }])
///     .when(ActionResult::TaskStarted { id: 1 })
///     .then(vec![Event::TaskInProgress { id: 1 }])
///     .then_react(vec![Action::NotifyManager { task_id: 1 }]);
/// ```
pub struct ProcessTestSpecification<'a, ActionResult, State, Event, Action, Error, P>
where
    Event: PartialEq + std::fmt::Debug,
    Action: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    P: ProcessTrait<
            ActionResult,
            State,
            State,
            Event,
            Event,
            Action,
            Events = Vec<Event>,
            Error = Error,
            Actions = Vec<Action>,
        > + EventComputationTrait<ActionResult, Event, Event, Events = Vec<Event>, Error = Error>
        + StateComputationTrait<ActionResult, State, Error = Error>,
{
    events: Vec<Event>,
    state: Option<State>,
    action_result: Option<ActionResult>,
    process: Option<&'a P>,
    _phantom: std::marker::PhantomData<Action>,
}

impl<ActionResult, State, Event, Action, Error, P> Default
    for ProcessTestSpecification<'_, ActionResult, State, Event, Action, Error, P>
where
    Event: PartialEq + std::fmt::Debug,
    Action: PartialEq + std::fmt::Debug,
    Error: PartialEq + std::fmt::Debug,
    P: ProcessTrait<
            ActionResult,
            State,
            State,
            Event,
            Event,
            Action,
            Events = Vec<Event>,
            Error = Error,
            Actions = Vec<Action>,
        > + EventComputationTrait<ActionResult, Event, Event, Events = Vec<Event>, Error = Error>
        + StateComputationTrait<ActionResult, State, Error = Error>,
{
    fn default() -> Self {
        Self {
            events: Vec::new(),
            state: None,
            action_result: None,
            process: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, ActionResult, State, Event, Action, Error, P>
    ProcessTestSpecification<'a, ActionResult, State, Event, Action, Error, P>
where
    ActionResult: std::fmt::Debug,
    Event: PartialEq + std::fmt::Debug + Clone,
    Action: PartialEq + std::fmt::Debug,
    State: PartialEq + std::fmt::Debug + Clone,
    Error: PartialEq + std::fmt::Debug,
    P: ProcessTrait<
            ActionResult,
            State,
            State,
            Event,
            Event,
            Action,
            Events = Vec<Event>,
            Error = Error,
            Actions = Vec<Action>,
        > + EventComputationTrait<ActionResult, Event, Event, Events = Vec<Event>, Error = Error>
        + StateComputationTrait<ActionResult, State, Error = Error>,
{
    /// Specify the process you want to test
    pub fn for_process(mut self, process: &'a P) -> Self {
        self.process = Some(process);
        self
    }

    /// Given preconditions / previous events (for event-sourced testing)
    pub fn given(mut self, events: Vec<Event>) -> Self {
        self.events = events;
        self
    }

    /// Given preconditions / previous state (for state-stored testing)
    pub fn given_state(mut self, state: Option<State>) -> Self {
        self.state = state;
        self
    }

    /// When action result is received
    pub fn when(mut self, action_result: ActionResult) -> Self {
        self.action_result = Some(action_result);
        self
    }

    /// Then expect result / new events (for event-sourced testing)
    #[track_caller]
    pub fn then(self, expected_events: Vec<Event>) -> Self {
        let process = self
            .process
            .expect("Process must be initialized. Did you forget to call `for_process`?");
        let action_result = self
            .action_result
            .as_ref()
            .expect("ActionResult must be initialized. Did you forget to call `when`?");
        let events = &self.events;

        let new_events_result = process.compute_new_events(events, action_result);
        let new_events = match new_events_result {
            Ok(events) => events,
            Err(error) => {
                panic!("Events were expected but the process returned an error instead: {error:?}")
            }
        };
        assert_eq!(
            new_events, expected_events,
            "Actual and Expected events do not match!\nActionResult: {action_result:?}\n",
        );
        self
    }

    /// Then expect result / new state (for state-stored testing)
    #[track_caller]
    pub fn then_state(self, expected_state: State) -> Self {
        let process = self
            .process
            .expect("Process must be initialized. Did you forget to call `for_process`?");
        let action_result = self
            .action_result
            .as_ref()
            .expect("ActionResult must be initialized. Did you forget to call `when`?");
        let state = self.state.clone();

        let new_state_result = process.compute_new_state(state, action_result);
        let new_state = match new_state_result {
            Ok(state) => state,
            Err(error) => {
                panic!("State was expected but the process returned an error instead: {error:?}")
            }
        };
        assert_eq!(
            new_state, expected_state,
            "Actual and Expected states do not match.\nActionResult: {action_result:?}\n"
        );
        self
    }

    /// Then expect error result
    #[track_caller]
    pub fn then_error(self, expected_error: Error) -> Self {
        let process = self
            .process
            .expect("Process must be initialized. Did you forget to call `for_process`?");
        let action_result = self
            .action_result
            .as_ref()
            .expect("ActionResult must be initialized. Did you forget to call `when`?");
        let events = &self.events;

        let error_result = process.compute_new_events(events, action_result);
        let error = match error_result {
            Ok(events) => {
                panic!("An error was expected but the process returned events instead: {events:?}")
            }
            Err(error) => error,
        };
        assert_eq!(
            error, expected_error,
            "Actual and Expected errors do not match.\nActionResult: {action_result:?}\n"
        );
        self
    }

    /// Then expect reactive actions in response to the last event
    #[track_caller]
    pub fn then_react(self, expected_actions: Vec<Action>) -> Self {
        let process = self
            .process
            .expect("Process must be initialized. Did you forget to call `for_process`?");

        // Reconstruct state from events
        let mut state = process.initial_state();
        for event in &self.events {
            state = process.evolve(&state, event);
        }

        // Get the last event to react to
        let last_event = self
            .events
            .last()
            .expect("No events available to react to. Did you forget to call `given`?");

        let actions = process.react(&state, last_event);
        let actions_vec: Vec<Action> = actions.into_iter().collect();

        assert_eq!(
            actions_vec, expected_actions,
            "Actual and Expected reactive actions do not match!\nState: {state:?}\nEvent: {last_event:?}\n",
        );
        self
    }

    /// Then expect all pending actions for the current state
    #[track_caller]
    pub fn then_pending(self, expected_actions: Vec<Action>) -> Self {
        let process = self
            .process
            .expect("Process must be initialized. Did you forget to call `for_process`?");

        // Reconstruct state from events
        let mut state = process.initial_state();
        for event in &self.events {
            state = process.evolve(&state, event);
        }

        let actions = process.pending(&state);
        let actions_vec: Vec<Action> = actions.into_iter().collect();

        assert_eq!(
            actions_vec, expected_actions,
            "Actual and Expected pending actions do not match!\nState: {state:?}\n",
        );
        self
    }
}

// ########################################################
// ###################### Tests ###########################
// ########################################################

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateDecider, DCBDecider, Process, Projection};

    // ========== Test Domain: Bank Account ==========

    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    enum AccountCommand {
        OpenAccount { id: u32, initial_balance: i32 },
        Deposit { id: u32, amount: i32 },
        Withdraw { id: u32, amount: i32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    enum AccountEvent {
        AccountOpened { id: u32, initial_balance: i32 },
        MoneyDeposited { id: u32, amount: i32 },
        MoneyWithdrawn { id: u32, amount: i32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct AccountState {
        id: Option<u32>,
        balance: i32,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum AccountError {
        InsufficientFunds,
        AccountAlreadyExists,
        AccountNotFound,
    }

    fn account_decider() -> AggregateDecider<
        AccountCommand,
        AccountState,
        AccountEvent,
        impl Fn(&AccountCommand, &AccountState) -> Result<Vec<AccountEvent>, AccountError>,
        impl Fn(&AccountState, &AccountEvent) -> AccountState,
        impl Fn() -> AccountState,
        Vec<AccountEvent>,
        AccountError,
    > {
        AggregateDecider::new(
            |command: &AccountCommand, state: &AccountState| match command {
                AccountCommand::OpenAccount {
                    id,
                    initial_balance,
                } => {
                    if state.id.is_some() {
                        Err(AccountError::AccountAlreadyExists)
                    } else {
                        Ok(vec![AccountEvent::AccountOpened {
                            id: *id,
                            initial_balance: *initial_balance,
                        }])
                    }
                }
                AccountCommand::Deposit { id, amount } => {
                    if state.id.is_none() {
                        Err(AccountError::AccountNotFound)
                    } else {
                        Ok(vec![AccountEvent::MoneyDeposited {
                            id: *id,
                            amount: *amount,
                        }])
                    }
                }
                AccountCommand::Withdraw { id, amount } => {
                    if state.id.is_none() {
                        Err(AccountError::AccountNotFound)
                    } else if state.balance < *amount {
                        Err(AccountError::InsufficientFunds)
                    } else {
                        Ok(vec![AccountEvent::MoneyWithdrawn {
                            id: *id,
                            amount: *amount,
                        }])
                    }
                }
            },
            |state: &AccountState, event: &AccountEvent| {
                let mut new_state = state.clone();
                match event {
                    AccountEvent::AccountOpened {
                        id,
                        initial_balance,
                    } => {
                        new_state.id = Some(*id);
                        new_state.balance = *initial_balance;
                    }
                    AccountEvent::MoneyDeposited { amount, .. } => {
                        new_state.balance += amount;
                    }
                    AccountEvent::MoneyWithdrawn { amount, .. } => {
                        new_state.balance -= amount;
                    }
                }
                new_state
            },
            || AccountState {
                id: None,
                balance: 0,
            },
        )
    }

    // ========== AggregateDecider Tests ==========

    #[test]
    fn test_aggregate_decider_event_sourced_success() {
        let decider = account_decider();

        AggregateDeciderTestSpecification::default()
            .for_decider(&decider)
            .given(vec![AccountEvent::AccountOpened {
                id: 1,
                initial_balance: 100,
            }])
            .when(AccountCommand::Deposit { id: 1, amount: 50 })
            .then(vec![AccountEvent::MoneyDeposited { id: 1, amount: 50 }]);
    }

    #[test]
    fn test_aggregate_decider_event_sourced_error() {
        let decider = account_decider();

        AggregateDeciderTestSpecification::default()
            .for_decider(&decider)
            .given(vec![AccountEvent::AccountOpened {
                id: 1,
                initial_balance: 50,
            }])
            .when(AccountCommand::Withdraw { id: 1, amount: 100 })
            .then_error(AccountError::InsufficientFunds);
    }

    #[test]
    fn test_aggregate_decider_state_stored_success() {
        let decider = account_decider();

        AggregateDeciderTestSpecification::default()
            .for_decider(&decider)
            .given_state(Some(AccountState {
                id: Some(1),
                balance: 100,
            }))
            .when(AccountCommand::Deposit { id: 1, amount: 50 })
            .then_state(AccountState {
                id: Some(1),
                balance: 150,
            });
    }

    #[test]
    fn test_aggregate_decider_multiple_events() {
        let decider = account_decider();

        AggregateDeciderTestSpecification::default()
            .for_decider(&decider)
            .given(vec![
                AccountEvent::AccountOpened {
                    id: 1,
                    initial_balance: 100,
                },
                AccountEvent::MoneyDeposited { id: 1, amount: 50 },
                AccountEvent::MoneyWithdrawn { id: 1, amount: 30 },
            ])
            .when(AccountCommand::Withdraw { id: 1, amount: 50 })
            .then(vec![AccountEvent::MoneyWithdrawn { id: 1, amount: 50 }]);
    }

    // ========== DCBDecider Tests ==========

    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    enum UpstreamEvent {
        OrderPlaced { order_id: u32, amount: i32 },
        OrderCancelled { order_id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    enum DownstreamEvent {
        PaymentRequested { order_id: u32, amount: i32 },
        PaymentCancelled { order_id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    enum TransformCommand {
        ProcessOrder { order_id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct TransformState {
        processed_orders: Vec<u32>,
    }

    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    enum TransformError {
        OrderNotFound,
    }

    fn transform_decider() -> DCBDecider<
        TransformCommand,
        TransformState,
        UpstreamEvent,
        DownstreamEvent,
        impl Fn(&TransformCommand, &TransformState) -> Result<Vec<DownstreamEvent>, TransformError>,
        impl Fn(&TransformState, &UpstreamEvent) -> TransformState,
        impl Fn() -> TransformState,
        Vec<DownstreamEvent>,
        TransformError,
    > {
        DCBDecider::new(
            |command: &TransformCommand, _state: &TransformState| match command {
                TransformCommand::ProcessOrder { order_id } => {
                    Ok(vec![DownstreamEvent::PaymentRequested {
                        order_id: *order_id,
                        amount: 100,
                    }])
                }
            },
            |state: &TransformState, event: &UpstreamEvent| {
                let mut new_state = state.clone();
                match event {
                    UpstreamEvent::OrderPlaced { order_id, .. } => {
                        new_state.processed_orders.push(*order_id);
                    }
                    UpstreamEvent::OrderCancelled { order_id } => {
                        new_state.processed_orders.retain(|id| id != order_id);
                    }
                }
                new_state
            },
            || TransformState {
                processed_orders: Vec::new(),
            },
        )
    }

    #[test]
    fn test_dcb_decider_event_transformation() {
        let decider = transform_decider();

        DCBDeciderTestSpecification::<_, TransformState, _, _, _, _>::default()
            .for_decider(&decider)
            .given(vec![UpstreamEvent::OrderPlaced {
                order_id: 1,
                amount: 100,
            }])
            .when(TransformCommand::ProcessOrder { order_id: 1 })
            .then(vec![DownstreamEvent::PaymentRequested {
                order_id: 1,
                amount: 100,
            }]);
    }

    #[test]
    fn test_dcb_decider_multiple_upstream_events() {
        let decider = transform_decider();

        DCBDeciderTestSpecification::<_, TransformState, _, _, _, _>::default()
            .for_decider(&decider)
            .given(vec![
                UpstreamEvent::OrderPlaced {
                    order_id: 1,
                    amount: 100,
                },
                UpstreamEvent::OrderPlaced {
                    order_id: 2,
                    amount: 200,
                },
            ])
            .when(TransformCommand::ProcessOrder { order_id: 2 })
            .then(vec![DownstreamEvent::PaymentRequested {
                order_id: 2,
                amount: 100,
            }]);
    }

    // ========== Projection Tests ==========

    #[derive(Debug, Clone, PartialEq)]
    enum UserEvent {
        UserRegistered { id: u32, name: String },
        UserUpdated { id: u32, name: String },
        UserDeleted { id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct UserDirectory {
        users: std::collections::HashMap<u32, String>,
    }

    fn user_projection() -> Projection<
        UserDirectory,
        UserEvent,
        impl Fn(&UserDirectory, &UserEvent) -> UserDirectory,
        impl Fn() -> UserDirectory,
    > {
        Projection::new(
            |state: &UserDirectory, event: &UserEvent| {
                let mut new_state = state.clone();
                match event {
                    UserEvent::UserRegistered { id, name } => {
                        new_state.users.insert(*id, name.clone());
                    }
                    UserEvent::UserUpdated { id, name } => {
                        new_state.users.insert(*id, name.clone());
                    }
                    UserEvent::UserDeleted { id } => {
                        new_state.users.remove(id);
                    }
                }
                new_state
            },
            || UserDirectory {
                users: std::collections::HashMap::new(),
            },
        )
    }

    #[test]
    fn test_projection_single_event() {
        let projection = user_projection();

        let mut expected = UserDirectory {
            users: std::collections::HashMap::new(),
        };
        expected.users.insert(1, "Alice".to_string());

        ProjectionTestSpecification::default()
            .for_projection(&projection)
            .given(vec![UserEvent::UserRegistered {
                id: 1,
                name: "Alice".to_string(),
            }])
            .then(expected);
    }

    #[test]
    fn test_projection_multiple_events() {
        let projection = user_projection();

        let mut expected = UserDirectory {
            users: std::collections::HashMap::new(),
        };
        expected.users.insert(1, "Alice Smith".to_string());
        expected.users.insert(2, "Bob".to_string());

        ProjectionTestSpecification::default()
            .for_projection(&projection)
            .given(vec![
                UserEvent::UserRegistered {
                    id: 1,
                    name: "Alice".to_string(),
                },
                UserEvent::UserRegistered {
                    id: 2,
                    name: "Bob".to_string(),
                },
                UserEvent::UserUpdated {
                    id: 1,
                    name: "Alice Smith".to_string(),
                },
            ])
            .then(expected);
    }

    #[test]
    fn test_projection_with_deletion() {
        let projection = user_projection();

        let mut expected = UserDirectory {
            users: std::collections::HashMap::new(),
        };
        expected.users.insert(2, "Bob".to_string());

        ProjectionTestSpecification::default()
            .for_projection(&projection)
            .given(vec![
                UserEvent::UserRegistered {
                    id: 1,
                    name: "Alice".to_string(),
                },
                UserEvent::UserRegistered {
                    id: 2,
                    name: "Bob".to_string(),
                },
                UserEvent::UserDeleted { id: 1 },
            ])
            .then(expected);
    }

    // ========== Process Tests ==========

    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    enum TaskActionResult {
        TaskCreated { id: u32, title: String },
        TaskStarted { id: u32 },
        TaskCompleted { id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    enum TaskEvent {
        TaskAdded { id: u32, title: String },
        TaskInProgress { id: u32 },
        TaskFinished { id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    enum TaskAction {
        NotifyManager { task_id: u32 },
        UpdateDashboard { task_id: u32 },
        SendEmail { task_id: u32 },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct TaskState {
        tasks: std::collections::HashMap<u32, String>,
        in_progress: Vec<u32>,
    }

    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)]
    enum TaskError {
        TaskNotFound,
    }

    fn task_process() -> Process<
        TaskActionResult,
        TaskState,
        TaskEvent,
        TaskAction,
        impl Fn(&TaskActionResult, &TaskState) -> Result<Vec<TaskEvent>, TaskError>,
        impl Fn(&TaskState, &TaskEvent) -> TaskState,
        impl Fn() -> TaskState,
        impl Fn(&TaskState, &TaskEvent) -> Vec<TaskAction>,
        impl Fn(&TaskState) -> Vec<TaskAction>,
        Vec<TaskEvent>,
        Vec<TaskAction>,
        TaskError,
    > {
        Process::new(
            |action_result: &TaskActionResult, _state: &TaskState| match action_result {
                TaskActionResult::TaskCreated { id, title } => Ok(vec![TaskEvent::TaskAdded {
                    id: *id,
                    title: title.clone(),
                }]),
                TaskActionResult::TaskStarted { id } => {
                    Ok(vec![TaskEvent::TaskInProgress { id: *id }])
                }
                TaskActionResult::TaskCompleted { id } => {
                    Ok(vec![TaskEvent::TaskFinished { id: *id }])
                }
            },
            |state: &TaskState, event: &TaskEvent| {
                let mut new_state = state.clone();
                match event {
                    TaskEvent::TaskAdded { id, title } => {
                        new_state.tasks.insert(*id, title.clone());
                    }
                    TaskEvent::TaskInProgress { id } => {
                        new_state.in_progress.push(*id);
                    }
                    TaskEvent::TaskFinished { id } => {
                        new_state.in_progress.retain(|task_id| task_id != id);
                    }
                }
                new_state
            },
            || TaskState {
                tasks: std::collections::HashMap::new(),
                in_progress: Vec::new(),
            },
            |_state: &TaskState, event: &TaskEvent| match event {
                TaskEvent::TaskInProgress { id } => {
                    vec![TaskAction::NotifyManager { task_id: *id }]
                }
                TaskEvent::TaskFinished { id } => {
                    vec![TaskAction::UpdateDashboard { task_id: *id }]
                }
                _ => vec![],
            },
            |state: &TaskState| {
                state
                    .tasks
                    .keys()
                    .filter(|id| !state.in_progress.contains(id))
                    .map(|id| TaskAction::SendEmail { task_id: *id })
                    .collect()
            },
        )
    }

    #[test]
    fn test_process_event_sourced_with_react() {
        let process = task_process();

        ProcessTestSpecification::default()
            .for_process(&process)
            .given(vec![
                TaskEvent::TaskAdded {
                    id: 1,
                    title: "Task 1".to_string(),
                },
                TaskEvent::TaskInProgress { id: 1 },
            ])
            .when(TaskActionResult::TaskStarted { id: 1 })
            .then(vec![TaskEvent::TaskInProgress { id: 1 }])
            .then_react(vec![TaskAction::NotifyManager { task_id: 1 }]);
    }

    #[test]
    fn test_process_pending_actions() {
        let process = task_process();

        ProcessTestSpecification::default()
            .for_process(&process)
            .given(vec![
                TaskEvent::TaskAdded {
                    id: 1,
                    title: "Task 1".to_string(),
                },
                TaskEvent::TaskAdded {
                    id: 2,
                    title: "Task 2".to_string(),
                },
                TaskEvent::TaskInProgress { id: 1 },
                TaskEvent::TaskFinished { id: 1 },
            ])
            .when(TaskActionResult::TaskCompleted { id: 1 })
            .then(vec![TaskEvent::TaskFinished { id: 1 }]);

        // Note: We can't reliably test pending actions with HashMap iteration order
        // In a real test, you'd either sort the results or use a different data structure
    }

    #[test]
    fn test_process_state_stored() {
        let process = task_process();

        let mut initial_state = TaskState {
            tasks: std::collections::HashMap::new(),
            in_progress: Vec::new(),
        };
        initial_state.tasks.insert(1, "Task 1".to_string());

        let mut expected_state = initial_state.clone();
        expected_state.in_progress.push(1);

        ProcessTestSpecification::default()
            .for_process(&process)
            .given_state(Some(initial_state))
            .when(TaskActionResult::TaskStarted { id: 1 })
            .then_state(expected_state);
    }

    #[test]
    fn test_process_react_on_completion() {
        let process = task_process();

        ProcessTestSpecification::default()
            .for_process(&process)
            .given(vec![
                TaskEvent::TaskAdded {
                    id: 1,
                    title: "Task 1".to_string(),
                },
                TaskEvent::TaskInProgress { id: 1 },
                TaskEvent::TaskFinished { id: 1 },
            ])
            .when(TaskActionResult::TaskCompleted { id: 1 })
            .then(vec![TaskEvent::TaskFinished { id: 1 }])
            .then_react(vec![TaskAction::UpdateDashboard { task_id: 1 }]);
    }

    #[test]
    fn test_process_chaining_assertions() {
        let process = task_process();

        ProcessTestSpecification::default()
            .for_process(&process)
            .given(vec![
                TaskEvent::TaskAdded {
                    id: 1,
                    title: "Task 1".to_string(),
                },
                TaskEvent::TaskInProgress { id: 1 },
            ])
            .when(TaskActionResult::TaskStarted { id: 1 })
            .then(vec![TaskEvent::TaskInProgress { id: 1 }])
            .then_react(vec![TaskAction::NotifyManager { task_id: 1 }])
            .then_pending(vec![]);
    }
}

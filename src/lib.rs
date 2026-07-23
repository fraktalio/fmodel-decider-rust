#![allow(clippy::type_complexity)]
#![deny(missing_docs)]

//! # Decider
//!
//! A **pure, type-safe, event-driven decision engine** inspired by [fmodel-decider](https://github.com/fraktalio/fmodel-decider).
//!
//! This library brings the **progressive type refinement** philosophy,
//! providing abstractions for building event-sourced and state-stored systems with deterministic,
//! replayable business logic.
//!
//! ## Progressive Type Refinement Philosophy
//!
//! This library demonstrates how to evolve from general, flexible types to specific, constrained
//! types that better represent real-world information systems. Starting with the most generic
//! interfaces that support all possible type combinations, we progressively add constraints that
//! capture business rules and invariants.
//!
//! ## Core Abstractions
//!
//! The library provides four main abstractions that implement different architectural patterns
//! through **progressive type refinement**:
//!
//! ### 1. `Projection<S, E>` - Pure State Evolution Pattern
//!
//! - **Type Constraint**: Input state = Output state = `S` (same type)
//! - **Capability**: Only state evolution (`evolve`, `initial_state`)
//! - **Use Case**: Read-side projections, materialized views, event stream processing
//! - **Trait Implementations**: Only `ViewTrait<S, S, E>` (foundation level)
//!
//! ### 2. `AggregateDecider<C, S, E>` - Traditional Aggregate Pattern
//!
//! - **Type Constraint**: Input events = Output events = `E` (same type)
//! - **Capability**: State evolution + decision making (`decide`, `evolve`, `initial_state`)
//! - **Use Case**: Traditional DDD aggregates with strong consistency boundaries
//! - **Trait Implementations**: Both `ViewTrait<S, S, E>` and `DeciderTrait<C, S, S, E, E>`
//!
//! ### 3. `DCBDecider<C, S, Ei, Eo>` - Dynamic Consistency Boundary Pattern
//!
//! - **Type Flexibility**: Input events (`Ei`) ≠ Output events (`Eo`) (different types)
//! - **Capability**: State evolution + dynamic decision making
//! - **Use Case**: Cross-boundary decision making
//! - **Trait Implementations**: Both `ViewTrait<S, S, Ei>` and `DeciderTrait<C, S, S, Ei, Eo>`
//!
//! ### 4. `Process<AR, S, E, A>` - ToDo List Process Pattern
//!
//! - **Type Constraint**: Command = Action Result (`AR`), Input events = Output events = `E` (same type)
//! - **Capability**: State evolution + decision making + ToDo list management (`decide`, `evolve`, `initial_state`, `react`, `pending`)
//! - **Use Case**: State-driven ToDo lists, workflow management, saga orchestration, task scheduling
//! - **Trait Implementations**: `ViewTrait<S, S, E>`, `DeciderTrait<AR, S, S, E, E>`, and `ProcessTrait<AR, S, S, E, E, A>`
//! - **ToDo Semantics**: State serves as a ToDo list where `pending` returns all available actions, `react` filters to event-relevant actions
//!
//! ## Progressive Type Refinement Hierarchy
//!
//! ```text
//! ViewTrait<Si, So, Ei>           ← Foundation: State evolution only
//!     ↑
//! DeciderTrait<C, Si, So, Ei, Eo> ← Extension: Adds decision-making
//!     ↑
//! ProcessTrait<AR, Si, So, Ei, Eo, A> ← Further Extension: Adds automation
//!     ↑
//! Implementations with constraints:
//!     ├── Projection<S, E>              : ViewTrait<S, S, E>
//!     ├── AggregateDecider<C, S, E>      : DeciderTrait<C, S, S, E, E>
//!     ├── DCBDecider<C, S, Ei, Eo>       : DeciderTrait<C, S, S, Ei, Eo>
//!     └── Process<AR, S, E, A>           : ProcessTrait<AR, S, S, E, E, A>
//! ```
//!
//! ---
//!
//! ## Two Computation Models
//!
//! This crate formalizes two complementary computation patterns:
//!
//! ### 1. Event-Sourced Computation (`EventComputationTrait`)
//!
//! **State is reconstructed from event history**
//!
//! - Decision logic derives new events from **previously recorded events** and a command
//! - State is computed by folding over historical events using the `evolve` function
//! - Provides complete audit trail and time-travel capabilities
//! - Useful for: Event-sourced systems, append-only logs, auditable domain flows
//!
//! #### Available For:
//! - ✅ `AggregateDecider<C, S, E>` - Events flow back into the same aggregate
//! - ✅ `DCBDecider<C, S, Ei, Eo>` - Events flow across boundaries
//!
//! #### Trait Signature:
//! ```text
//! compute_new_events(&self, current_events: &[Ei], command: &C) -> Result<Events, Error>
//! ```
//!
//! ### 2. State-Stored Computation (`StateComputationTrait`)
//!
//! **Current state is maintained directly (e.g., in a database)**
//!
//! - Decision logic updates the **current state** directly, given a command
//! - Event history is not required; snapshots are sufficient
//! - Faster for read-heavy workloads (no event replay required)
//! - Useful for: CRUD-style models, snapshot-based systems, traditional applications
//!
//! #### Available For:
//! - ✅ `AggregateDecider<C, S, E>` - Same event type allows state evolution
//! - ❌ `DCBDecider<C, S, Ei, Eo>` - **Not possible** due to event type mismatch
//!
//! #### Why Not Available for DCBDecider:
//! Output events (`Eo`) cannot be fed back into `evolve` function which expects input events (`Ei`).
//! When `Ei ≠ Eo`, there's no way to apply output events back to the state for evolution.
//!
//! #### Trait Signature:
//! ```text
//! compute_new_state(&self, current_state: Option<Si>, command: &C) -> Result<So, Error>
//! ```
//!
//! ---
//!
//! ## Architectural Patterns
//!
//! ### Pure State Evolution (Projection)
//! ```text
//! Events (E) ──→ [Projection] ──→ State (S)
//!                    ↑              ↓
//!                evolve(S, E) ──→ S'
//! ```
//! - Events drive state evolution without decision-making
//! - Perfect for read-side projections and materialized views
//! - Only implements `ViewTrait` (foundation level)
//!
//! ### Traditional Aggregate (AggregateDecider)
//! ```text
//! Command ──→ [AggregateDecider] ──→ Events (E)
//!                ↑              ↓
//!            State (S) ←─── evolve(S, E)
//! ```
//! - Events produced can be consumed by the same aggregate
//! - Supports both event-sourced and state-stored patterns
//! - Strong consistency within the aggregate boundary
//!
//! ### Dynamic Consistency Boundary (DCBDecider)
//! ```text
//! Upstream Context    │    DCBDecider         │    Downstream Context
//!                     │                       │
//! Events (Ei) ────────┼──→ State Reconstruction │
//!                     │         ↓             │
//! Command ────────────┼──→ Business Logic     │
//!                     │         ↓             │
//!                     │    Events (Eo) ───────┼──→ Next Process
//! ```
//! - Events cross consistency boundaries
//! - Input events from one context, output events to another
//! - Only event-sourced pattern (state derived from input events)
//!
//! ---
//!
//! ## Core Components
//!
//! Both `AggregateDecider` and `DCBDecider` provide three core functions:
//!
//! - **`decide`**: `(Command, State) → Events` - Pure business logic
//! - **`evolve`**: `(State, Event) → State` - State evolution
//! - **`initial_state`**: `() → State` - State initialization
//!
//! ## Thread Safety
//!
//! Both abstractions support feature-gated thread safety:
//!
//! - **Multi-threaded mode** (default): Uses `Arc`, enforces `Send + Sync`
//! - **Single-threaded mode** (`--features single-threaded`): Uses `Rc`, better performance
//!
//! ## Zero-Cost Abstractions
//!
//! - **Fully statically dispatched** (no dynamic dispatch)
//! - **Allocation-free by default** (events can be iterators)
//! - **Generic type parameters** for maximum flexibility
//! - **Function types as generics** for compile-time optimization
mod application;
mod decider;
mod dynamic_decider;
mod process;
mod projection;
pub mod specification;
mod workflow;

pub use application::{
    EventLoader, EventRepository, EventSourcedCommandHandler, EventSourcedQueryHandler,
    MaterializedViewHandler, QueryTuple, StateRepository, StateStoredCommandHandler,
    ViewRepository,
};
pub use decider::AggregateDecider;
pub use dynamic_decider::DCBDecider;
pub use process::Process;
pub use projection::Projection;
pub use workflow::{Workflow, WorkflowEvent, WorkflowState};

/// Generic sum type representing a value that is either `First(A)` or `Second(B)`.
///
/// This is the Rust equivalent of TypeScript's union type `A | B`, used by the
/// `combine` method to merge command and event types from two deciders into a
/// single combined type.
///
/// ## Usage with `combine`
///
/// When combining two deciders, their types are merged as:
/// - Commands: `Sum<C1, C2>` — a command is either for the first or second decider
/// - Events: `Sum<E1, E2>` — an event belongs to either the first or second decider
/// - State: `(S1, S2)` — both states are maintained as a tuple (product type)
#[derive(Debug, Clone, PartialEq)]
pub enum Sum<A, B> {
    /// First variant
    First(A),
    /// Second variant
    Second(B),
}

/// Core trait defining the essential view behavior for state evolution.
///
/// This trait represents the **most fundamental abstraction** in the progressive type
/// refinement hierarchy. It captures only the essential operations needed to:
/// - Evolve state in response to events
/// - Provide initial state
///
/// This is the **foundation trait** that more complex abstractions build upon.
///
/// ## Progressive Type Refinement Foundation
///
/// `ViewTrait` demonstrates the starting point of progressive refinement:
/// 1. **Start with minimal, essential operations** (evolve, initial_state)
/// 2. **More complex traits extend this foundation** (`DeciderTrait` adds `decide`)
/// 3. **Implementations constrain type parameters** to model specific patterns
///
/// ## Type Parameters
///
/// - `Si` — Input State type: State used as input for evolution
/// - `So` — Output State type: State produced after applying events
/// - `Ei` — Input Event type: Events that drive state evolution
///
/// ## Core Operations
///
/// - **`evolve`**: Transform input state by applying an input event
/// - **`initial_state`**: Provide the starting input state
pub trait ViewTrait<Si, So, Ei> {
    /// Evolve input state by applying a single input event.
    ///
    /// This function defines how the input state changes in response to input events.
    /// It should be pure and deterministic - the same input state and input event
    /// should always produce the same output state.
    ///
    /// ## Type Transformation
    /// This method enables state transformation:
    /// - `Si` → `So`: State may change type during evolution
    /// - Typically `Si = So` for most implementations
    fn evolve(&self, state: &Si, event: &Ei) -> So;

    /// Get the initial input state.
    ///
    /// This provides the starting input state before any events have been applied.
    /// Should be pure and deterministic.
    fn initial_state(&self) -> Si;
}

/// Core trait defining the essential behavior of a Decider.
///
/// This trait **extends `ViewTrait`** by adding decision-making capabilities to the
/// foundation of state evolution. This demonstrates **progressive type refinement**
/// through trait composition.
///
/// ## Progressive Type Refinement Through Extension
///
/// This trait captures the fundamental operations that any decider implementation
/// must provide, using the most generic type parameters possible. Through progressive
/// type refinement, concrete implementations constrain these parameters to model
/// specific architectural patterns.
///
/// ## Progressive Type Refinement
///
/// This trait demonstrates the library's core philosophy by starting with maximum
/// generality and allowing implementations to add constraints:
///
/// - **Most Generic**: `DeciderTrait<C, Si, So, Ei, Eo>` - All types independent
/// - **Traditional Aggregate**: `DeciderTrait<C, S, S, E, E>` - State and events unified
/// - **Dynamic Boundary**: `DeciderTrait<C, Si, So, Ei, Eo>` - Full flexibility for cross-boundary flow
///
/// ## Type Parameters
///
/// - `C` — Command type: Represents the intent to change the system
/// - `Si` — Input State type: State used for decision making (inherited from ViewTrait)
/// - `So` — Output State type: State produced after applying events (inherited from ViewTrait)
/// - `Ei` — Input Event type: Events consumed for state reconstruction (inherited from ViewTrait)
/// - `Eo` — Output Event type: Events produced by decision logic
///
/// ## Associated Types
///
/// - `Events` — Collection type that yields output events (`Eo`)
/// - `Error` — Error type returned by the `decide` operation
///
/// ## Architectural Patterns Through Constraints
///
/// ### Traditional Aggregate Pattern (`Si = So = S`, `Ei = Eo = E`)
/// - Events flow within the same consistency boundary
/// - State type remains consistent throughout
/// - Supports both event-sourced and state-stored patterns
///
/// ### Dynamic Consistency Boundary Pattern (`Si = So = S`, `Ei ≠ Eo`)
/// - Events cross consistency boundaries
/// - State remains consistent internally
/// - Primarily event-sourced pattern
///
/// ## Usage
///
/// This trait enables working with deciders abstractly while preserving
/// the ability to constrain types for specific use cases.
///
/// ## Implementations
///
/// This trait is implemented by:
/// - `AggregateDecider` with constraints `Si = So = S`, `Ei = Eo = E` (traditional aggregates)
/// - `DCBDecider` with constraints `Si = So = S`, `Ei ≠ Eo` (dynamic boundaries)
pub trait DeciderTrait<C, Si, So, Ei, Eo>: ViewTrait<Si, So, Ei> {
    /// The collection of newly produced events of type `Eo`.
    ///
    /// This type must be convertible into an iterator over output events.
    /// Typically this is an iterator itself to avoid allocations.
    type Events: IntoIterator<Item = Eo>;

    /// The error type returned by the `decide` operation.
    type Error;

    /// Apply business logic to generate output events from command and input state.
    ///
    /// This is the core decision-making function that encapsulates business rules.
    /// Given a command (intent) and current input state, it determines what output
    /// events should occur.
    ///
    /// ## Type Flexibility
    /// - Input state (`Si`) may differ from output state (`So`) for cross-boundary patterns
    /// - Output events (`Eo`) may differ from input events (`Ei`) for event transformation
    ///
    /// ## Purity
    /// This function should be pure (no side effects) and deterministic.
    /// The same command and input state should always produce the same output events.
    fn decide(&self, command: &C, state: &Si) -> Result<Self::Events, Self::Error>;
}

/// Formalizes the **event-sourced computation model**.
///
/// `EventComputationTrait` represents the decision logic of an event-sourced system where state is
/// reconstructed from event history. Given a command and previously recorded events of type `Ei`,
/// it produces new domain events of type `Eo`.
///
/// ## Key Characteristics
///
/// - **State Reconstruction**: Current state is computed by folding over historical events
/// - **Pure Functions**: Implementations should be deterministic and side-effect free
/// - **Event Flow**: Can model both internal aggregate events and cross-boundary event flow
/// - **Audit Trail**: Complete event history provides full auditability and time-travel capabilities
///
/// ## Type Flexibility
///
/// - **Same Event Types** (`Ei = Eo = E`): Traditional aggregate pattern (implemented by `Decider`)
/// - **Different Event Types** (`Ei ≠ Eo`): Dynamic consistency boundary pattern (implemented by `DynamicDecider`)
///
/// ## When to Use
///
/// Use this trait when:
/// - Your system follows **event sourcing** patterns
/// - State is derived by replaying events using an `evolve` function
/// - You need complete audit trails and replayability
/// - You're modeling event flow across consistency boundaries
///
/// ## Implementation Notes
///
/// - `current_events` is a borrowed slice (`&[Ei]`) to avoid allocation and ownership transfer
/// - `Events` is an associated type to allow efficient, allocation-free event production
/// - The trait supports both single-aggregate and cross-boundary event flow patterns
pub trait EventComputationTrait<C, Ei, Eo> {
    /// The collection of newly produced events of type `Eo`.
    type Events: IntoIterator<Item = Eo>;
    /// The error type returned by the computation.
    type Error;

    /// Computes new domain events from command and event history.
    ///
    /// This method implements the core event-sourcing pattern:
    /// 1. **Reconstruct State**: Fold over `current_events` to rebuild current state
    /// 2. **Apply Business Logic**: Use command and reconstructed state to make decisions  
    /// 3. **Generate Events**: Produce events that represent the outcome of the decision
    ///
    /// The relationship between input events (`Ei`) and output events (`Eo`) determines
    /// the architectural pattern:
    /// - **Same types**: Events flow within the same aggregate boundary
    /// - **Different types**: Events flow across consistency boundaries
    fn compute_new_events(
        &self,
        current_events: &[Ei],
        command: &C,
    ) -> Result<Self::Events, Self::Error>;
}

/// Formalizes the **state-stored computation model**.
///
/// `StateComputationTrait` represents the decision logic of a state-stored system where current state
/// is maintained directly (e.g., in a database) rather than reconstructed from events. Given a command
/// and current state of type `S`, it produces a new state of the same type `S`.
///
/// ## Key Characteristics
///
/// - **Direct State Storage**: Current state is persisted and retrieved directly
/// - **Snapshot-Based**: No need to replay event history
/// - **Performance**: Faster for read-heavy workloads (no event replay required)
/// - **Simplicity**: Familiar pattern for traditional applications
/// - **State Continuity**: Input and output state types are identical, ensuring consistency
///
/// ## Type Parameters
///
/// - `C`: Command type that triggers state changes
/// - `S`: State type (same for both input and output)
///
/// ## When to Use
///
/// Use this trait when:
/// - Your system follows **CRUD/snapshot-based** patterns
/// - Current state is stored directly in databases
/// - You need fast read operations without event replay
/// - You're building traditional applications with state persistence
///
/// ## Implementation Notes
///
/// - `current_state` is `Option<S>` to handle initialization cases
/// - Implementations should handle `None` by using initial state
/// - The trait is designed for systems where state evolution is direct
/// - State type must remain consistent between input and output
pub trait StateComputationTrait<C, S> {
    /// The error type returned by the computation.
    type Error;

    /// Computes new state from current state and command.
    ///
    /// This method implements the state-stored pattern:
    /// 1. **Use Current State**: Take the provided state as-is (no reconstruction needed)
    /// 2. **Generate Events**: Apply business logic to determine what should happen
    /// 3. **Apply Events**: Fold the generated events into the current state
    /// 4. **Return New State**: Provide the updated state for persistence
    ///
    /// The state evolution maintains **referential transparency**: the same
    /// current state and command will always produce the same new state.
    fn compute_new_state(&self, current_state: Option<S>, command: &C) -> Result<S, Self::Error>;
}

/// Core trait defining the essential behavior of a Process with ToDo list semantics.
///
/// This trait **extends `DeciderTrait`** by adding reactive and proactive behavior capabilities
/// to model state-driven ToDo list management. This demonstrates **progressive type refinement**
/// through trait composition, building upon the decision-making foundation.
///
/// ## Progressive Type Refinement Through Extension
///
/// This trait represents the most specialized level in the trait hierarchy:
/// - **Foundation**: `ViewTrait<Si, So, Ei>` - State evolution only
/// - **Extension 1**: `DeciderTrait<C, Si, So, Ei, Eo>` - Adds decision-making
/// - **Extension 2**: `ProcessTrait<AR, Si, So, Ei, Eo, A>` - Adds reactive behavior
///
/// ## ToDo List Semantics
///
/// The Process state serves as a **ToDo list context** where:
/// - **`pending(state)`**: Returns the **complete ToDo list** - all actions available in the current state
/// - **`react(state, event)`**: Returns a **filtered ToDo list** - only actions relevant to the specific event
/// - **Subset Relationship**: `react` results are always a subset of `pending` results
///
/// ## Type Parameters
///
/// - `AR` — Action Result type: Represents the outcome of external actions (replaces Command in DeciderTrait)
/// - `Si` — Input State type: State used for decision making (inherited from DeciderTrait)
/// - `So` — Output State type: State produced after applying events (inherited from DeciderTrait)
/// - `Ei` — Input Event type: Events consumed for state reconstruction (inherited from DeciderTrait)
/// - `Eo` — Output Event type: Events produced by decision logic (inherited from DeciderTrait)
/// - `A` — Action type: Actions that can be performed in response to state/events
///
/// ## Associated Types
///
/// - `Actions` — Collection type that yields actions (`A`)
///
/// ## Architectural Pattern
///
/// ### Process Pattern Constraints (`AR = ActionResult`, `Si = So = S`, `Ei = Eo = E`)
/// - Action results drive the process instead of commands
/// - State and events remain consistent within the process boundary
/// - Models reactive processes that respond to external action outcomes
/// - Perfect for workflow orchestration and saga management
///
/// ## Usage
///
/// This trait enables building reactive processes that maintain ToDo lists and respond
/// to both state changes and external events, providing both proactive and reactive
/// behavior patterns.
///
/// ## Implementations
///
/// This trait is implemented by:
/// - `Process` with constraints `AR = ActionResult`, `Si = So = S`, `Ei = Eo = E`
pub trait ProcessTrait<AR, Si, So, Ei, Eo, A>: DeciderTrait<AR, Si, So, Ei, Eo> {
    /// The collection of actions of type `A`.
    ///
    /// This type must be convertible into an iterator over actions.
    /// Typically this is an iterator itself to avoid allocations.
    type Actions: IntoIterator<Item = A>;

    /// Generate actions in response to state and event (reactive behavior).
    ///
    /// This method implements **reactive ToDo list filtering**:
    /// - Takes the current state and a specific event
    /// - Returns only the actions that are **relevant to this specific event**
    /// - Results are always a **subset** of what `pending` would return for the same state
    ///
    /// ## ToDo List Semantics
    /// This represents the **filtered ToDo list** - when an event occurs, only certain
    /// actions from the complete ToDo list become immediately relevant.
    ///
    /// ## Purity
    /// This function should be pure (no side effects) and deterministic.
    /// The same state and event should always produce the same actions.
    fn react(&self, state: &Si, event: &Ei) -> Self::Actions;

    /// Generate all available actions for the current state (proactive behavior).
    ///
    /// This method implements **complete ToDo list generation**:
    /// - Takes only the current state
    /// - Returns **all actions** that are available/possible in this state
    /// - Represents the **complete ToDo list** for the current context
    ///
    /// ## ToDo List Semantics
    /// This represents the **complete ToDo list** - all tasks/actions that could
    /// potentially be performed given the current state, regardless of recent events.
    ///
    /// ## Relationship to React
    /// The results from `react(state, event)` should always be a subset of
    /// `pending(state)` for the same state.
    ///
    /// ## Purity
    /// This function should be pure (no side effects) and deterministic.
    /// The same state should always produce the same actions.
    fn pending(&self, state: &Si) -> Self::Actions;
}

/// A single `key:value` tag attached to an event, used for secondary indexing.
///
/// Encapsulates the `"key:value"` string convention structurally instead of leaving
/// callers to hand-format strings (e.g. `format!("id:{}", id)`), which removes the risk
/// of malformed tags: a missing separator, or a value that itself contains a colon and
/// breaks naive parsing downstream.
///
/// `Tag` implements [`std::fmt::Display`], producing the canonical `"key:value"` wire
/// format that repository implementations index on.
///
/// # Example
///
/// ```rust
/// use fmodel_decider_rust::Tag;
///
/// let tag = Tag::new("restaurantId", "123");
/// assert_eq!(tag.to_string(), "restaurantId:123");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag {
    /// The tag's key (e.g. `"restaurantId"`).
    pub key: String,
    /// The tag's value (e.g. `"123"`).
    pub value: String,
}

impl Tag {
    /// Creates a new tag from a key and value.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.key, self.value)
    }
}

/// Trait for events that carry metadata for indexing and querying.
///
/// Used by repository implementations (e.g., FoundationDB) to build secondary
/// indexes based on event type and tags. This trait lives in the core crate with
/// zero external dependencies, preserving the library's dependency-free philosophy.
///
/// ## Dynamic Consistency Boundary (DCB) Indexing
///
/// In DCB patterns there is no fixed stream or aggregate ID. Instead, events declare
/// their own type and tags, and queries are expressed as event type + optional tag
/// combinations. This enables flexible querying where different deciders can define
/// overlapping consistency boundaries by querying different tag combinations.
///
/// ## Tags
///
/// Tags are [`Tag`] values extracted from the event's own fields. They serve
/// as the basis for secondary indexing and stream identification. Repository
/// implementations use the power set of an event's tags to build indexes that
/// support queries by any tag combination.
///
/// ## Example
///
/// ```rust
/// use fmodel_decider_rust::{EventMeta, Tag};
///
/// struct RestaurantCreatedEvent {
///     restaurant_id: String,
/// }
///
/// impl EventMeta for RestaurantCreatedEvent {
///     fn event_type(&self) -> &str {
///         "RestaurantCreatedEvent"
///     }
///
///     fn tags(&self) -> Vec<Tag> {
///         vec![Tag::new("restaurantId", self.restaurant_id.clone())]
///     }
/// }
/// ```
pub trait EventMeta {
    /// Returns the event type identifier (e.g., `"RestaurantCreatedEvent"`).
    ///
    /// This string is used as the primary discriminator in secondary indexes,
    /// forming the first element of tag index and last event pointer keys.
    fn event_type(&self) -> &str;

    /// Returns tags extracted from the event's fields.
    ///
    /// Tags are used for secondary indexing and stream identification. Repository
    /// implementations build power set indexes over these tags to support queries
    /// by any combination.
    fn tags(&self) -> Vec<Tag>;
}

/// Implemented by commands that carry an idempotency key.
///
/// Repository implementations use this key to make command execution idempotent:
/// when a command with the same key is executed more than once the original output
/// events are returned without re-running the decider.
pub trait IdempotencyKey {
    /// Returns the idempotency key for this command.
    fn idempotency_key(&self) -> &str;
}

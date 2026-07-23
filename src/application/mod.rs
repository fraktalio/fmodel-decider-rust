//! # Application Layer
//!
//! The application layer bridges **pure domain logic** with **infrastructure concerns** through
//! repository abstractions. This module provides traits that define the **execute pattern**:
//! a transactional flow that encapsulates fetching, computing, and saving in a single operation.
//!
//! ## Core Philosophy
//!
//! This layer maintains the library's core principles:
//! - **Separation of Concerns**: Domain components remain pure; repositories handle I/O
//! - **Dependency Injection**: Domain components are passed as parameters, not stored
//! - **Zero Dependencies**: Uses only Rust standard library with async trait methods
//! - **Feature-Gated Threading**: Supports both multi-threaded and single-threaded modes
//! - **Runtime Agnostic**: Works with any async runtime (tokio, async-std, etc.)
//!
//! ## The Execute Pattern
//!
//! All repository traits follow the same three-stage transactional pattern:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    execute(input, component)             │
//! │                                                          │
//! │  1. FETCH  ──→  Load current state from storage         │
//! │                 (events or state snapshot)              │
//! │                                                          │
//! │  2. COMPUTE ──→ Apply domain logic via component        │
//! │                 (pure business rules)                   │
//! │                                                          │
//! │  3. SAVE   ──→  Persist results to storage              │
//! │                 (events or state snapshot)              │
//! │                                                          │
//! │  Returns: Newly persisted data (events or state)        │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! This pattern ensures:
//! - **Atomicity**: All three stages succeed or fail together
//! - **Consistency**: Domain logic is always applied to current state
//! - **Isolation**: Each execution is independent
//! - **Durability**: Results are persisted before returning
//!
//! ## Four Application Patterns
//!
//! The application layer provides repository traits and loaders, each aligned with a specific
//! computation model from the domain layer:
//!
//! ### 1. EventRepository - Event-Sourced Aggregates
//!
//! **Pattern**: Fetch events → Compute new events → Save events
//!
//! ```text
//! Command ──→ [EventRepository] ──→ Vec<OutputEvent>
//!                    ↓
//!              1. Fetch events from stream
//!              2. Call compute_new_events on decider
//!              3. Save new events to stream
//! ```
//!
//! - **Works with**: `EventComputationTrait<C, Ei, Eo>`
//! - **Use case**: Event-sourced aggregates with full audit trail
//! - **Returns**: Newly persisted output events
//!
//! ### 2. StateRepository - State-Stored Systems
//!
//! **Pattern**: Fetch state → Compute new state → Save state
//!
//! ```text
//! Command ──→ [StateRepository] ──→ State
//!                    ↓
//!              1. Fetch current state snapshot
//!              2. Call compute_new_state on component
//!              3. Save new state snapshot
//! ```
//!
//! - **Works with**: `StateComputationTrait<C, S>`
//! - **Use case**: CRUD-style systems with state snapshots
//! - **Returns**: Newly persisted state
//!
//! ### 3. ViewRepository - Materialized Views
//!
//! **Pattern**: Fetch state → Evolve state → Save state
//!
//! ```text
//! Event ──→ [ViewRepository] ──→ State
//!                  ↓
//!            1. Fetch current view state
//!            2. Call evolve on view component
//!            3. Save evolved state
//! ```
//!
//! - **Works with**: `ViewTrait<S, S, E>`
//! - **Use case**: Read-side projections and materialized views
//! - **Returns**: Newly persisted state
//!
//! ### 4. EventLoader - Ad-hoc Event Queries
//!
//! **Pattern**: Load events → Fold through view → Return computed state (not persisted)
//!
//! ```text
//! QueryTuples ──→ [EventLoader] ──→ Vec<Event>
//!                       ↓
//!                 [ViewTrait.evolve] ──→ State (on-the-fly, not persisted)
//! ```
//!
//! - **Works with**: `ViewTrait<S, S, E>` via `EventSourcedQueryHandler`
//! - **Use case**: Ad-hoc queries directly on the event store without materialized views
//! - **Returns**: Computed state (not persisted)
//!
//! ## Usage Example: EventRepository
//!
//! ```rust,no_run
//! use std::collections::HashMap;
//! use std::sync::{Arc, Mutex};
//!
//! # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
//! # #[derive(Clone, Debug)]
//! # enum Command { Deposit(u32) }
//! # #[derive(Clone, Debug)]
//! # enum Event { Deposited(u32) }
//! # #[derive(Clone, Debug, Default)]
//! # struct State { balance: u32 }
//! #
//! // Define your repository implementation
//! struct InMemoryEventRepository {
//!     events: Arc<Mutex<HashMap<String, Vec<Event>>>>,
//! }
//!
//! impl InMemoryEventRepository {
//!     fn new() -> Self {
//!         Self {
//!             events: Arc::new(Mutex::new(HashMap::new())),
//!         }
//!     }
//! }
//!
//! // Implement the EventRepository trait
//! # #[cfg(not(feature = "single-threaded"))]
//! # trait EventRepository<C, Ei, Eo>: Send + Sync {
//! #     type Error;
//! #     async fn execute<D>(&self, command: C, decider: &D) -> Result<Vec<Eo>, Self::Error>
//! #     where D: EventComputationTrait<C, Ei, Eo> + Send + Sync, D::Error: std::fmt::Debug;
//! # }
//! #
//! # #[cfg(not(feature = "single-threaded"))]
//! impl EventRepository<Command, Event, Event> for InMemoryEventRepository {
//!     type Error = String;
//!
//!     async fn execute<D>(
//!         &self,
//!         command: Command,
//!         decider: &D,
//!     ) -> Result<Vec<Event>, Self::Error>
//!     where
//!         D: EventComputationTrait<Command, Event, Event> + Send + Sync,
//!         D::Error: std::fmt::Debug,
//!     {
//!         // 1. FETCH: Load events from storage
//!         let stream_id = "account-123".to_string();
//!         let mut events = self.events.lock().unwrap();
//!         let current_events = events.get(&stream_id).cloned().unwrap_or_default();
//!
//!         // 2. COMPUTE: Apply domain logic
//!         let new_events = decider
//!             .compute_new_events(&current_events, &command)
//!             .map_err(|e| format!("Compute failed: {:?}", e))?;
//!         let new_events_vec: Vec<Event> = new_events.into_iter().collect();
//!
//!         // 3. SAVE: Persist new events
//!         events
//!             .entry(stream_id)
//!             .or_default()
//!             .extend(new_events_vec.clone());
//!
//!         Ok(new_events_vec)
//!     }
//! }
//!
//! // Usage with a domain decider
//! # #[cfg(not(feature = "single-threaded"))]
//! # async fn example() -> Result<(), String> {
//! # let decider = AggregateDecider::new(
//! #     |_c: &Command, _s: &State| -> Result<Vec<Event>, String> { Ok(vec![Event::Deposited(100)]) },
//! #     |s: &State, e: &Event| {
//! #         let mut new_state = s.clone();
//! #         if let Event::Deposited(amount) = e {
//! #             new_state.balance += amount;
//! #         }
//! #         new_state
//! #     },
//! #     || State::default(),
//! # );
//! let repository = InMemoryEventRepository::new();
//! let command = Command::Deposit(100);
//!
//! // Execute command through repository
//! let events = repository.execute(command, &decider).await?;
//! println!("Persisted events: {:?}", events);
//! # Ok(())
//! # }
//! ```
//!
//! ## Usage Example: StateRepository
//!
//! ```rust,no_run
//! use std::collections::HashMap;
//! use std::sync::{Arc, Mutex};
//!
//! # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider};
//! # #[derive(Clone, Debug)]
//! # enum Command { Increment }
//! # #[derive(Clone, Debug, Default)]
//! # struct State { count: u32 }
//! #
//! // Define your repository implementation
//! struct InMemoryStateRepository {
//!     states: Arc<Mutex<HashMap<String, State>>>,
//! }
//!
//! impl InMemoryStateRepository {
//!     fn new() -> Self {
//!         Self {
//!             states: Arc::new(Mutex::new(HashMap::new())),
//!         }
//!     }
//! }
//!
//! // Implement the StateRepository trait
//! # #[cfg(not(feature = "single-threaded"))]
//! # trait StateRepository<C, S>: Send + Sync {
//! #     type Error;
//! #     async fn execute<D>(&self, command: C, component: &D) -> Result<S, Self::Error>
//! #     where D: StateComputationTrait<C, S> + Send + Sync, D::Error: std::fmt::Debug;
//! # }
//! #
//! # #[cfg(not(feature = "single-threaded"))]
//! impl StateRepository<Command, State> for InMemoryStateRepository {
//!     type Error = String;
//!
//!     async fn execute<D>(
//!         &self,
//!         command: Command,
//!         component: &D,
//!     ) -> Result<State, Self::Error>
//!     where
//!         D: StateComputationTrait<Command, State> + Send + Sync,
//!         D::Error: std::fmt::Debug,
//!     {
//!         // 1. FETCH: Load current state from storage
//!         let stream_id = "counter-1".to_string();
//!         let mut states = self.states.lock().unwrap();
//!         let current_state = states.get(&stream_id).cloned();
//!
//!         // 2. COMPUTE: Apply domain logic
//!         let new_state = component
//!             .compute_new_state(current_state, &command)
//!             .map_err(|e| format!("Compute failed: {:?}", e))?;
//!
//!         // 3. SAVE: Persist new state
//!         states.insert(stream_id, new_state.clone());
//!
//!         Ok(new_state)
//!     }
//! }
//!
//! // Usage with a domain component
//! # #[cfg(not(feature = "single-threaded"))]
//! # async fn example() -> Result<(), String> {
//! # let component = AggregateDecider::new(
//! #     |_c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![]) },
//! #     |s: &State, _e: &()| s.clone(),
//! #     || State::default(),
//! # );
//! let repository = InMemoryStateRepository::new();
//! let command = Command::Increment;
//!
//! // Execute command through repository
//! let state = repository.execute(command, &component).await?;
//! println!("Persisted state: {:?}", state);
//! # Ok(())
//! # }
//! ```
//!
//! ## Usage Example: ViewRepository
//!
//! ```rust,no_run
//! use std::collections::HashMap;
//! use std::sync::{Arc, Mutex};
//!
//! # use fmodel_decider_rust::{ViewTrait, Projection};
//! # #[derive(Clone, Debug)]
//! # enum Event { Deposited(u32) }
//! # #[derive(Clone, Debug, Default)]
//! # struct State { balance: u32 }
//! #
//! // Define your repository implementation
//! struct InMemoryViewRepository {
//!     views: Arc<Mutex<HashMap<String, State>>>,
//! }
//!
//! impl InMemoryViewRepository {
//!     fn new() -> Self {
//!         Self {
//!             views: Arc::new(Mutex::new(HashMap::new())),
//!         }
//!     }
//! }
//!
//! // Implement the ViewRepository trait
//! # #[cfg(not(feature = "single-threaded"))]
//! # trait ViewRepository<E, S>: Send + Sync {
//! #     type Error;
//! #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
//! #     where V: ViewTrait<S, S, E> + Send + Sync;
//! # }
//! #
//! # #[cfg(not(feature = "single-threaded"))]
//! impl ViewRepository<Event, State> for InMemoryViewRepository {
//!     type Error = String;
//!
//!     async fn execute<V>(
//!         &self,
//!         event: Event,
//!         view: &V,
//!     ) -> Result<State, Self::Error>
//!     where
//!         V: ViewTrait<State, State, Event> + Send + Sync,
//!     {
//!         // 1. FETCH: Load current view state from storage
//!         let stream_id = "balance-view".to_string();
//!         let mut views = self.views.lock().unwrap();
//!         let current_state = views
//!             .get(&stream_id)
//!             .cloned()
//!             .unwrap_or_else(|| view.initial_state());
//!
//!         // 2. COMPUTE: Evolve state via view component
//!         let new_state = view.evolve(&current_state, &event);
//!
//!         // 3. SAVE: Persist evolved state
//!         views.insert(stream_id, new_state.clone());
//!
//!         Ok(new_state)
//!     }
//! }
//!
//! // Usage with a view component
//! # #[cfg(not(feature = "single-threaded"))]
//! # async fn example() -> Result<(), String> {
//! # let view = Projection::new(
//! #     |s: &State, e: &Event| {
//! #         let mut new_state = s.clone();
//! #         if let Event::Deposited(amount) = e {
//! #             new_state.balance += amount;
//! #         }
//! #         new_state
//! #     },
//! #     || State::default(),
//! # );
//! let repository = InMemoryViewRepository::new();
//! let event = Event::Deposited(100);
//!
//! // Execute event through repository
//! let state = repository.execute(event, &view).await?;
//! println!("Updated view state: {:?}", state);
//! # Ok(())
//! # }
//! ```
//!
//! ## Error Handling
//!
//! Each repository trait uses an associated `Error` type, allowing implementers to define
//! errors that capture both domain and infrastructure concerns:
//!
//! ```rust
//! #[derive(Debug)]
//! enum RepositoryError {
//!     // Infrastructure errors
//!     FetchFailed(String),
//!     SaveFailed(String),
//!     ConnectionError(String),
//!     
//!     // Domain errors (from compute operations)
//!     ComputationFailed(String),
//!     ValidationError(String),
//!     
//!     // Combined errors with context
//!     TransactionFailed { stage: String, cause: String },
//! }
//! ```
//!
//! The `execute` method returns `Result<T, Self::Error>`, allowing errors at any stage
//! to be propagated with appropriate context:
//!
//! 1. **Fetch stage**: Storage retrieval errors
//! 2. **Compute stage**: Domain logic errors (from `decide`, `compute_new_events`, etc.)
//! 3. **Save stage**: Storage persistence errors
//!
//! ## Thread Safety
//!
//! Repository traits support feature-gated thread safety, matching the domain layer:
//!
//! ### Multi-threaded Mode (default)
//! ```text
//! - Requires: Send + Sync bounds on traits
//! - Compatible with: Arc-based domain components
//! - Use case: Concurrent request handling in web servers
//! ```
//!
//! ### Single-threaded Mode (`--features single-threaded`)
//! ```text
//! - No Send/Sync requirements
//! - Compatible with: Rc-based domain components
//! - Use case: Single-threaded performance optimization
//! ```
//!
//! ## Design Principles
//!
//! 1. **Dependency Injection**: Domain components are passed as parameters to `execute`,
//!    not stored in repositories. This keeps repositories focused on I/O concerns.
//!
//! 2. **Transactional Semantics**: The `execute` method encapsulates the complete
//!    transactional flow, ensuring atomicity and consistency.
//!
//! 3. **Pure Domain Logic**: Domain components remain pure functions with no side effects.
//!    All I/O is handled by repository implementations.
//!
//!    customize behavior based on their storage mechanisms.
//!
//! 5. **Runtime Agnostic**: Async trait methods work with any async runtime
//!    (tokio, async-std, smol, etc.) without external dependencies.

mod event_repository;
mod event_sourced_command_handler;
mod event_sourced_query_handler;
mod materialized_view_handler;
mod state_repository;
mod state_stored_command_handler;
mod view_repository;

pub use event_repository::*;
pub use event_sourced_command_handler::*;
pub use event_sourced_query_handler::*;
pub use materialized_view_handler::*;
pub use state_repository::*;
pub use state_stored_command_handler::*;
pub use view_repository::*;

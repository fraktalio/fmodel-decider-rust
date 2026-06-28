use crate::EventComputationTrait;

// ================================================================================================
// EventRepository Trait
// ================================================================================================

/// Repository trait for event-sourced aggregates.
///
/// This trait defines the contract for persisting and retrieving events in an event-sourced
/// system. It encapsulates the complete transactional flow: fetch events → compute new events
/// → save events.
///
/// # Type Parameters
///
/// - `C`: Command type that triggers state changes
/// - `Ei`: Input event type (events loaded from storage)
/// - `Eo`: Output event type (events to be persisted)
///
/// # Associated Types
///
/// - `Error`: Repository-specific error type for fetch, compute, or save failures
///
/// # The Execute Pattern
///
/// The `execute` method implements a three-stage transactional pattern:
///
/// ```text
/// 1. FETCH  → Load events from the stream identified by the command
/// 2. COMPUTE → Call compute_new_events on the provided decider
/// 3. SAVE   → Persist the newly computed events to the stream
/// ```
///
/// This pattern ensures atomicity: all three stages succeed or fail together.
///
/// # Thread Safety
///
/// In multi-threaded mode (default), this trait requires `Send + Sync` bounds, making
/// implementations safe to share across threads. In single-threaded mode
/// (`--features single-threaded`), these bounds are removed for better performance.
///
/// # Usage Example
///
/// ```rust,no_run
/// use std::collections::HashMap;
/// use std::sync::{Arc, Mutex};
/// # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
///
/// # #[derive(Clone, Debug)]
/// # enum Command { OpenAccount { id: String } }
/// # #[derive(Clone, Debug)]
/// # enum Event { AccountOpened { id: String } }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { opened: bool }
///
/// // Define your repository implementation
/// struct InMemoryEventRepository {
///     events: Arc<Mutex<HashMap<String, Vec<Event>>>>,
/// }
///
/// # #[cfg(not(feature = "single-threaded"))]
/// # trait EventRepository<C, Ei, Eo>: Send + Sync {
/// #     type Error;
/// #     async fn execute<D>(&self, command: C, decider: &D) -> Result<Vec<Eo>, Self::Error>
/// #     where D: EventComputationTrait<C, Ei, Eo> + Send + Sync, D::Error: std::fmt::Debug;
/// # }
///
/// # #[cfg(not(feature = "single-threaded"))]
/// impl EventRepository<Command, Event, Event> for InMemoryEventRepository {
///     type Error = String;
///
///     async fn execute<D>(
///         &self,
///         command: Command,
///         decider: &D,
///     ) -> Result<Vec<Event>, Self::Error>
///     where
///         D: EventComputationTrait<Command, Event, Event> + Send + Sync,
///         D::Error: std::fmt::Debug,
///     {
///         // 1. FETCH: Extract stream ID and load events
///         let stream_id = match &command {
///             Command::OpenAccount { id } => id.clone(),
///         };
///         let mut events = self.events.lock().unwrap();
///         let current_events = events.get(&stream_id).cloned().unwrap_or_default();
///
///         // 2. COMPUTE: Apply domain logic via decider
///         let new_events = decider
///             .compute_new_events(&current_events, &command)
///             .map_err(|e| format!("Compute failed: {:?}", e))?;
///         let new_events_vec: Vec<Event> = new_events.into_iter().collect();
///
///         // 3. SAVE: Persist new events to stream
///         events
///             .entry(stream_id)
///             .or_default()
///             .extend(new_events_vec.clone());
///
///         Ok(new_events_vec)
///     }
/// }
///
/// // Usage with a domain decider
/// # async fn example() -> Result<(), String> {
/// # let decider = AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<Event>, String> {
/// #         match c {
/// #             Command::OpenAccount { id } => Ok(vec![Event::AccountOpened { id: id.clone() }]),
/// #         }
/// #     },
/// #     |s: &State, _e: &Event| {
/// #         let mut new_state = s.clone();
/// #         new_state.opened = true;
/// #         new_state
/// #     },
/// #     || State::default(),
/// # );
/// # let repository = InMemoryEventRepository {
/// #     events: Arc::new(Mutex::new(HashMap::new())),
/// # };
/// let command = Command::OpenAccount { id: "acc-123".to_string() };
///
/// // Execute command through repository
/// let events = repository.execute(command, &decider).await?;
/// println!("Persisted {} events", events.len());
/// # Ok(())
/// # }
/// ```
///
/// # Error Handling
///
/// The `execute` method returns `Result<Vec<Eo>, Self::Error>`, allowing errors at any
/// stage to be propagated:
///
/// - **Fetch failures**: Storage retrieval errors (connection issues, missing streams, etc.)
/// - **Compute failures**: Domain logic errors from the decider's `compute_new_events` method
/// - **Save failures**: Storage persistence errors (write conflicts, transaction failures, etc.)
///
/// Implementers should provide rich error types that capture context about which stage failed:
///
/// ```rust
/// #[derive(Debug)]
/// enum RepositoryError {
///     FetchFailed(String),
///     ComputationFailed(String),
///     SaveFailed(String),
///     TransactionFailed { stage: String, cause: String },
/// }
/// ```
///
/// # Relationship to fmodel-ts
///
/// This trait adapts the TypeScript `IEventRepository` interface:
///
/// ```typescript
/// export interface IEventRepository<C, Ei, Eo, CM, EM> {
///   readonly execute: (
///     command: C & CM,
///     decider: IEventComputation<C, Ei, Eo>,
///   ) => Promise<readonly (Eo & EM)[]>;
/// }
/// ```
#[cfg(not(feature = "single-threaded"))]
pub trait EventRepository<C, Ei, Eo>: Send + Sync {
    /// Error type for repository operations.
    ///
    /// This should capture both infrastructure errors (fetch/save failures) and domain
    /// errors (computation failures). Implementers can define rich error types that
    /// provide context about which stage of the execute pattern failed.
    type Error;

    /// Execute a command against an event-sourced aggregate.
    ///
    /// This method encapsulates the complete transactional flow:
    /// 1. **Fetch**: Load events for the stream identified by the command
    /// 2. **Compute**: Call `compute_new_events` on the provided decider
    /// 3. **Save**: Persist the newly computed events to the stream
    /// 4. **Return**: The persisted output events
    ///
    /// # Type Parameters
    ///
    /// - `D`: The decider implementing `EventComputationTrait<C, Ei, Eo>`
    ///
    /// # Parameters
    ///
    /// - `command`: The command to execute (contains stream ID and command data)
    /// - `decider`: Reference to the domain component that computes new events
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<Eo>)`: The newly persisted output events on success
    /// - `Err(Self::Error)`: Any error during fetch, compute, or save stages
    ///
    /// # Atomicity
    ///
    /// Implementations should ensure atomicity: if any stage fails, no events are persisted.
    /// The exact mechanism depends on the underlying storage (transactions, optimistic
    /// locking, etc.).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
    /// # #[derive(Clone, Debug)]
    /// # enum Command { Deposit(u32) }
    /// # #[derive(Clone, Debug)]
    /// # enum Event { Deposited(u32) }
    /// # #[derive(Clone, Debug, Default)]
    /// # struct State { balance: u32 }
    /// # struct MyRepository;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # trait EventRepository<C, Ei, Eo>: Send + Sync {
    /// #     type Error;
    /// #     async fn execute<D>(&self, command: C, decider: &D) -> Result<Vec<Eo>, Self::Error>
    /// #     where D: EventComputationTrait<C, Ei, Eo> + Send + Sync;
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl EventRepository<Command, Event, Event> for MyRepository {
    /// #     type Error = String;
    /// #     async fn execute<D>(&self, command: Command, decider: &D) -> Result<Vec<Event>, Self::Error>
    /// #     where D: EventComputationTrait<Command, Event, Event> + Send + Sync
    /// #     { Ok(vec![]) }
    /// # }
    /// # async fn example() -> Result<(), String> {
    /// # let repository = MyRepository;
    /// # let decider = AggregateDecider::new(
    /// #     |c: &Command, _s: &State| -> Result<Vec<Event>, String> {
    /// #         match c {
    /// #             Command::Deposit(amount) => Ok(vec![Event::Deposited(*amount)]),
    /// #         }
    /// #     },
    /// #     |s: &State, e: &Event| {
    /// #         let mut new_state = s.clone();
    /// #         if let Event::Deposited(amount) = e {
    /// #             new_state.balance += amount;
    /// #         }
    /// #         new_state
    /// #     },
    /// #     || State::default(),
    /// # );
    /// let command = Command::Deposit(100);
    /// let events = repository.execute(command, &decider).await?;
    /// # Ok(())
    /// # }
    /// ```
    fn execute<D>(
        &self,
        command: C,
        decider: &D,
    ) -> impl std::future::Future<Output = Result<Vec<Eo>, Self::Error>> + Send
    where
        D: EventComputationTrait<C, Ei, Eo> + Send + Sync;
}

/// Repository trait for event-sourced aggregates (single-threaded variant).
///
/// This is the single-threaded variant of `EventRepository`, enabled with the
/// `single-threaded` feature flag. It has the same API as the multi-threaded variant
/// but without `Send + Sync` bounds, allowing use with `Rc`-based domain components
/// for better single-threaded performance.
///
/// See the multi-threaded `EventRepository` documentation for detailed usage information.
///
/// # Feature Flag
///
/// This variant is only available when compiling with `--features single-threaded`.
///
/// # Thread Safety
///
/// This variant does NOT require `Send + Sync` bounds, making it suitable for:
/// - Single-threaded applications
/// - `Rc`-based domain components
/// - Performance-critical single-threaded scenarios
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "single-threaded")]
/// # {
/// use std::rc::Rc;
/// use std::cell::RefCell;
/// use std::collections::HashMap;
/// # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
///
/// # #[derive(Clone, Debug)]
/// # enum Command { Deposit(u32) }
/// # #[derive(Clone, Debug)]
/// # enum Event { Deposited(u32) }
///
/// // Single-threaded repository using Rc instead of Arc
/// struct SingleThreadedEventRepository {
///     events: Rc<RefCell<HashMap<String, Vec<Event>>>>,
/// }
///
/// # trait EventRepository<C, Ei, Eo> {
/// #     type Error;
/// #     async fn execute<D>(&self, command: C, decider: &D) -> Result<Vec<Eo>, Self::Error>
/// #     where D: EventComputationTrait<C, Ei, Eo>;
/// # }
///
/// impl EventRepository<Command, Event, Event> for SingleThreadedEventRepository {
///     type Error = String;
///
///     fn execute<D>(
///         &self,
///         command: Command,
///         decider: &D,
///     ) -> impl std::future::Future<Output = Result<Vec<Event>, Self::Error>>
///     where
///         D: EventComputationTrait<Command, Event, Event>,
///     {
///         async move {
///             // Implementation using Rc/RefCell instead of Arc/Mutex
///             # Ok(vec![])
///         }
///     }
/// }
/// # }
/// ```
#[cfg(feature = "single-threaded")]
pub trait EventRepository<C, Ei, Eo> {
    /// Error type for repository operations.
    type Error;

    /// Execute a command against an event-sourced aggregate.
    ///
    /// See the multi-threaded variant documentation for detailed information.
    fn execute<D>(
        &self,
        command: C,
        decider: &D,
    ) -> impl std::future::Future<Output = Result<Vec<Eo>, Self::Error>>
    where
        D: EventComputationTrait<C, Ei, Eo>;
}

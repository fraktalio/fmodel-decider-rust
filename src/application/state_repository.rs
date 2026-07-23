use crate::{IdempotencyKey, StateComputationTrait};

// ================================================================================================
// StateRepository Trait
// ================================================================================================

/// Repository trait for state-stored systems.
///
/// This trait defines the contract for persisting and retrieving state snapshots in a
/// state-stored system. It encapsulates the complete transactional flow: fetch state →
/// compute new state → save state.
///
/// # Type Parameters
///
/// - `C`: Command type that triggers state changes. Must implement [`IdempotencyKey`] so
///   repository implementations can deduplicate retried commands.
/// - `S`: State type (both current and new state)
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
/// 1. FETCH  → Load current state snapshot from storage
/// 2. COMPUTE → Call compute_new_state on the provided component
/// 3. SAVE   → Persist the newly computed state snapshot
/// ```
///
/// This pattern ensures atomicity: all three stages succeed or fail together.
///
/// # Difference from EventRepository
///
/// While `EventRepository` works with event streams and event-sourced aggregates,
/// `StateRepository` works with state snapshots and CRUD-style systems. The key differences:
///
/// - **EventRepository**: Fetches events, computes new events, saves events
/// - **StateRepository**: Fetches state, computes new state, saves state
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
/// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
///
/// # #[derive(Clone, Debug)]
/// # enum Command {
/// #     Increment { idempotency_key: String },
/// #     Decrement { idempotency_key: String },
/// # }
/// # impl IdempotencyKey for Command {
/// #     fn idempotency_key(&self) -> &str {
/// #         match self {
/// #             Command::Increment { idempotency_key } => idempotency_key,
/// #             Command::Decrement { idempotency_key } => idempotency_key,
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { count: i32 }
///
/// // Define your repository implementation
/// struct InMemoryStateRepository {
///     states: Arc<Mutex<HashMap<String, State>>>,
/// }
///
/// # #[cfg(not(feature = "single-threaded"))]
/// # trait StateRepository<C, S>: Send + Sync
/// # where
/// #     C: IdempotencyKey,
/// # {
/// #     type Error;
/// #     async fn execute<D>(&self, command: C, component: &D) -> Result<S, Self::Error>
/// #     where D: StateComputationTrait<C, S> + Send + Sync, D::Error: std::fmt::Debug;
/// # }
///
/// # #[cfg(not(feature = "single-threaded"))]
/// impl StateRepository<Command, State> for InMemoryStateRepository {
///     type Error = String;
///
///     async fn execute<D>(
///         &self,
///         command: Command,
///         component: &D,
///     ) -> Result<State, Self::Error>
///     where
///         D: StateComputationTrait<Command, State> + Send + Sync,
///         D::Error: std::fmt::Debug,
///     {
///         // 1. FETCH: Load current state from storage
///         let stream_id = "counter-1".to_string();
///         let mut states = self.states.lock().unwrap();
///         let current_state = states.get(&stream_id).cloned();
///
///         // 2. COMPUTE: Apply domain logic via component
///         let new_state = component
///             .compute_new_state(current_state, &command)
///             .map_err(|e| format!("Compute failed: {:?}", e))?;
///
///         // 3. SAVE: Persist new state snapshot
///         states.insert(stream_id, new_state.clone());
///
///         Ok(new_state)
///     }
/// }
///
/// // Usage with a domain component
/// # async fn example() -> Result<(), String> {
/// # let component = AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<()>, String> {
/// #         match c {
/// #             Command::Increment { .. } => Ok(vec![()]),
/// #             Command::Decrement { .. } => Ok(vec![()]),
/// #         }
/// #     },
/// #     |s: &State, _e: &()| s.clone(),
/// #     || State::default(),
/// # );
/// # let repository = InMemoryStateRepository {
/// #     states: Arc::new(Mutex::new(HashMap::new())),
/// # };
/// let command = Command::Increment { idempotency_key: "req-1".to_string() };
///
/// // Execute command through repository
/// let state = repository.execute(command, &component).await?;
/// println!("Persisted state: {:?}", state);
/// # Ok(())
/// # }
/// ```
///
/// # Error Handling
///
/// The `execute` method returns `Result<S, Self::Error>`, allowing errors at any
/// stage to be propagated:
///
/// - **Fetch failures**: Storage retrieval errors (connection issues, missing state, etc.)
/// - **Compute failures**: Domain logic errors from the component's `compute_new_state` method
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
#[cfg(not(feature = "single-threaded"))]
pub trait StateRepository<C, S>: Send + Sync
where
    C: IdempotencyKey,
{
    /// Error type for repository operations.
    ///
    /// This should capture both infrastructure errors (fetch/save failures) and domain
    /// errors (computation failures). Implementers can define rich error types that
    /// provide context about which stage of the execute pattern failed.
    type Error;

    /// Execute a command against a state-stored aggregate.
    ///
    /// This method encapsulates the complete transactional flow:
    /// 1. **Fetch**: Load current state snapshot for the stream identified by the command
    /// 2. **Compute**: Call `compute_new_state` on the provided component
    /// 3. **Save**: Persist the newly computed state snapshot
    /// 4. **Return**: The persisted state
    ///
    /// # Type Parameters
    ///
    /// - `D`: The component implementing `StateComputationTrait<C, S>`
    ///
    /// # Parameters
    ///
    /// - `command`: The command to execute (contains stream ID and command data)
    /// - `component`: Reference to the domain component that computes new state
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The newly persisted state on success
    /// - `Err(Self::Error)`: Any error during fetch, compute, or save stages
    ///
    /// # Atomicity
    ///
    /// Implementations should ensure atomicity: if any stage fails, no state changes are
    /// persisted. The exact mechanism depends on the underlying storage (transactions,
    /// optimistic locking, etc.).
    ///
    /// # Idempotency
    ///
    /// Implementations should use `command.idempotency_key()` to detect retried commands:
    /// when a command with a previously seen key is executed again, the original persisted
    /// state should be returned without re-running the component.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
    /// # #[derive(Clone, Debug)]
    /// # enum Command { SetValue { value: i32, idempotency_key: String } }
    /// # impl IdempotencyKey for Command {
    /// #     fn idempotency_key(&self) -> &str {
    /// #         match self {
    /// #             Command::SetValue { idempotency_key, .. } => idempotency_key,
    /// #         }
    /// #     }
    /// # }
    /// # #[derive(Clone, Debug, Default)]
    /// # struct State { value: i32 }
    /// # struct MyRepository;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # trait StateRepository<C, S>: Send + Sync
    /// # where
    /// #     C: IdempotencyKey,
    /// # {
    /// #     type Error;
    /// #     async fn execute<D>(&self, command: C, component: &D) -> Result<S, Self::Error>
    /// #     where D: StateComputationTrait<C, S> + Send + Sync;
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl StateRepository<Command, State> for MyRepository {
    /// #     type Error = String;
    /// #     async fn execute<D>(&self, command: Command, component: &D) -> Result<State, Self::Error>
    /// #     where D: StateComputationTrait<Command, State> + Send + Sync
    /// #     { Ok(State::default()) }
    /// # }
    /// # async fn example() -> Result<(), String> {
    /// # let repository = MyRepository;
    /// # let component = AggregateDecider::new(
    /// #     |c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![()]) },
    /// #     |s: &State, _e: &()| s.clone(),
    /// #     || State::default(),
    /// # );
    /// let command = Command::SetValue { value: 42, idempotency_key: "req-1".to_string() };
    /// let state = repository.execute(command, &component).await?;
    /// # Ok(())
    /// # }
    /// ```
    fn execute<D>(
        &self,
        command: C,
        component: &D,
    ) -> impl std::future::Future<Output = Result<S, Self::Error>> + Send
    where
        D: StateComputationTrait<C, S> + Send + Sync;

    /// Execute a batch of commands against a state-stored aggregate.
    ///
    /// This is the batch counterpart of [`execute`](Self::execute). It processes an ordered
    /// list of commands and returns the single, final state after applying all of them.
    ///
    /// # Semantics
    ///
    /// Commands are applied **in order**, each against the state produced by the preceding
    /// command in the batch. Only the final state is returned (mirroring the TypeScript
    /// `IStateRepository.executeBatch`, which returns a single `S`, not a list).
    ///
    /// # Atomicity
    ///
    /// Implementations should treat the batch as a single unit of work: either all commands
    /// succeed and the resulting state is persisted, or none are. The exact mechanism depends
    /// on the underlying storage (transactions, optimistic locking, etc.).
    ///
    /// # Idempotency
    ///
    /// As with [`execute`](Self::execute), implementations should use each command's
    /// `idempotency_key()` to detect and skip re-running previously handled commands within
    /// the batch.
    ///
    /// # Empty batch
    ///
    /// An empty `commands` vec has no computed result. Implementations should decide on a
    /// well-defined behavior for this case — typically fetching and returning the current
    /// persisted state, or returning an error — and document it.
    ///
    /// # Type Parameters
    ///
    /// - `D`: The component implementing `StateComputationTrait<C, S>`
    ///
    /// # Parameters
    ///
    /// - `commands`: The ordered list of commands to execute
    /// - `component`: Reference to the domain component that computes new state
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The final persisted state on success
    /// - `Err(Self::Error)`: Any error during fetch, compute, or save stages
    fn execute_batch<D>(
        &self,
        commands: Vec<C>,
        component: &D,
    ) -> impl std::future::Future<Output = Result<S, Self::Error>> + Send
    where
        D: StateComputationTrait<C, S> + Send + Sync;
}

/// Repository trait for state-stored systems (single-threaded variant).
///
/// This is the single-threaded variant of `StateRepository`, enabled with the
/// `single-threaded` feature flag. It has the same API as the multi-threaded variant
/// but without `Send + Sync` bounds, allowing use with `Rc`-based domain components
/// for better single-threaded performance.
///
/// See the multi-threaded `StateRepository` documentation for detailed usage information.
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
/// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
///
/// # #[derive(Clone, Debug)]
/// # enum Command { Increment { idempotency_key: String } }
/// # impl IdempotencyKey for Command {
/// #     fn idempotency_key(&self) -> &str {
/// #         match self {
/// #             Command::Increment { idempotency_key } => idempotency_key,
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { count: i32 }
///
/// // Single-threaded repository using Rc instead of Arc
/// struct SingleThreadedStateRepository {
///     states: Rc<RefCell<HashMap<String, State>>>,
/// }
///
/// # trait StateRepository<C, S>
/// # where
/// #     C: IdempotencyKey,
/// # {
/// #     type Error;
/// #     async fn execute<D>(&self, command: C, component: &D) -> Result<S, Self::Error>
/// #     where D: StateComputationTrait<C, S>;
/// # }
///
/// impl StateRepository<Command, State> for SingleThreadedStateRepository {
///     type Error = String;
///
///     async fn execute<D>(
///         &self,
///         command: Command,
///         component: &D,
///     ) -> Result<State, Self::Error>
///     where
///         D: StateComputationTrait<Command, State>,
///     {
///         // Implementation using Rc/RefCell instead of Arc/Mutex
///         # Ok(State::default())
///     }
/// }
/// # }
/// ```
#[cfg(feature = "single-threaded")]
pub trait StateRepository<C, S>
where
    C: IdempotencyKey,
{
    /// Error type for repository operations.
    type Error;

    /// Execute a command against a state-stored aggregate.
    ///
    /// See the multi-threaded variant documentation for detailed information.
    fn execute<D>(
        &self,
        command: C,
        component: &D,
    ) -> impl std::future::Future<Output = Result<S, Self::Error>>
    where
        D: StateComputationTrait<C, S>;

    /// Execute a batch of commands against a state-stored aggregate.
    ///
    /// See the multi-threaded variant documentation for detailed information.
    fn execute_batch<D>(
        &self,
        commands: Vec<C>,
        component: &D,
    ) -> impl std::future::Future<Output = Result<S, Self::Error>>
    where
        D: StateComputationTrait<C, S>;
}

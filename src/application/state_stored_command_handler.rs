use super::state_repository::StateRepository;
use crate::{IdempotencyKey, StateComputationTrait};

// ================================================================================================
// StateStoredCommandHandler - Convenience Layer
// ================================================================================================

/// Command handler for state-stored systems (multi-threaded variant).
///
/// This struct provides a convenience API that encapsulates a domain component (decider)
/// and a repository together. It eliminates the need to pass the decider on every
/// `execute` call, providing a cleaner API for repeated command execution.
///
/// # Type Parameters
///
/// - `C`: Command type that triggers state changes. Must implement [`IdempotencyKey`].
/// - `S`: State type (both current and new state)
/// - `D`: The decider implementing `StateComputationTrait<C, S>`
/// - `R`: The repository implementing `StateRepository<C, S>`
///
/// # Thread Safety
///
/// In multi-threaded mode (default), this struct uses `Arc` for shared ownership and
/// requires `Send + Sync` bounds on both the decider and repository. This makes it
/// safe to share across threads in concurrent applications.
///
/// # Motivation
///
/// **Without handler (direct repository usage):**
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
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
/// # let component = AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![()]) },
/// #     |s: &State, _e: &()| s.clone(),
/// #     || State::default(),
/// # );
/// # let repository = Arc::new(MyRepository);
/// // Must pass component on every call
/// let state1 = repository.execute(Command::Increment { idempotency_key: "req-1".to_string() }, &component).await?;
/// let state2 = repository.execute(Command::Increment { idempotency_key: "req-2".to_string() }, &component).await?;
/// let state3 = repository.execute(Command::Decrement { idempotency_key: "req-3".to_string() }, &component).await?;
/// # Ok(())
/// # }
/// ```
///
/// **With handler:**
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
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
/// # use std::marker::PhantomData;
/// # #[cfg(not(feature = "single-threaded"))]
/// # struct StateStoredCommandHandler<C, S, D, R>
/// # where
/// #     C: IdempotencyKey,
/// #     D: StateComputationTrait<C, S> + Send + Sync,
/// #     R: StateRepository<C, S> + Send + Sync,
/// # {
/// #     decider: Arc<D>,
/// #     repository: Arc<R>,
/// #     _phantom: PhantomData<(C, S)>,
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
/// # where
/// #     C: IdempotencyKey,
/// #     D: StateComputationTrait<C, S> + Send + Sync,
/// #     R: StateRepository<C, S> + Send + Sync,
/// # {
/// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
/// #         Self { decider, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, command: C) -> Result<S, R::Error> {
/// #         self.repository.execute(command, &*self.decider).await
/// #     }
/// # }
/// # async fn example() -> Result<(), String> {
/// # let component = Arc::new(AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![()]) },
/// #     |s: &State, _e: &()| s.clone(),
/// #     || State::default(),
/// # ));
/// # let repository = Arc::new(MyRepository);
/// // Component encapsulated in handler
/// let handler = StateStoredCommandHandler::new(component, repository);
/// let state1 = handler.handle(Command::Increment { idempotency_key: "req-1".to_string() }).await?;
/// let state2 = handler.handle(Command::Increment { idempotency_key: "req-2".to_string() }).await?;
/// let state3 = handler.handle(Command::Decrement { idempotency_key: "req-3".to_string() }).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Trade-offs
///
/// **Direct Repository:**
/// - ✅ More flexible (can use different components per call)
/// - ✅ No additional struct allocation
/// - ❌ More verbose (must pass component each time)
/// - ❌ Easy to accidentally use wrong component
///
/// **Command Handler:**
/// - ✅ Less verbose (component encapsulated)
/// - ✅ Type-safe (impossible to use wrong component)
/// - ✅ Better for repeated operations on same aggregate
/// - ❌ Less flexible (single component per handler)
/// - ❌ Additional Arc allocation
///
/// # Usage Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
/// # #[derive(Clone, Debug)]
/// # enum Command {
/// #     Increment { idempotency_key: String },
/// #     Decrement { idempotency_key: String },
/// #     Reset { idempotency_key: String },
/// # }
/// # impl IdempotencyKey for Command {
/// #     fn idempotency_key(&self) -> &str {
/// #         match self {
/// #             Command::Increment { idempotency_key } => idempotency_key,
/// #             Command::Decrement { idempotency_key } => idempotency_key,
/// #             Command::Reset { idempotency_key } => idempotency_key,
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { count: i32 }
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
/// # use std::marker::PhantomData;
/// # #[cfg(not(feature = "single-threaded"))]
/// # struct StateStoredCommandHandler<C, S, D, R>
/// # where
/// #     C: IdempotencyKey,
/// #     D: StateComputationTrait<C, S> + Send + Sync,
/// #     R: StateRepository<C, S> + Send + Sync,
/// # {
/// #     decider: Arc<D>,
/// #     repository: Arc<R>,
/// #     _phantom: PhantomData<(C, S)>,
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
/// # where
/// #     C: IdempotencyKey,
/// #     D: StateComputationTrait<C, S> + Send + Sync,
/// #     R: StateRepository<C, S> + Send + Sync,
/// # {
/// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
/// #         Self { decider, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, command: C) -> Result<S, R::Error> {
/// #         self.repository.execute(command, &*self.decider).await
/// #     }
/// # }
///
/// # async fn example() -> Result<(), String> {
/// // Create domain component and repository
/// let component = Arc::new(AggregateDecider::new(
///     |c: &Command, _s: &State| -> Result<Vec<()>, String> {
///         match c {
///             Command::Increment { .. } => Ok(vec![()]),
///             Command::Decrement { .. } => Ok(vec![()]),
///             Command::Reset { .. } => Ok(vec![()]),
///         }
///     },
///     |s: &State, _e: &()| {
///         let mut new_state = s.clone();
///         new_state.count += 1;
///         new_state
///     },
///     || State::default(),
/// ));
/// let repository = Arc::new(MyRepository);
///
/// // Create handler
/// let handler = StateStoredCommandHandler::new(component, repository);
///
/// // Handle commands without passing component each time
/// let state = handler.handle(Command::Increment { idempotency_key: "req-1".to_string() }).await?;
/// let state = handler.handle(Command::Increment { idempotency_key: "req-2".to_string() }).await?;
/// let state = handler.handle(Command::Decrement { idempotency_key: "req-3".to_string() }).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(not(feature = "single-threaded"))]
pub struct StateStoredCommandHandler<C, S, D, R>
where
    C: IdempotencyKey + Send + Sync,
    S: Send + Sync,
    D: StateComputationTrait<C, S> + Send + Sync,
    R: StateRepository<C, S> + Send + Sync,
{
    decider: std::sync::Arc<D>,
    repository: std::sync::Arc<R>,
    _phantom: std::marker::PhantomData<(C, S)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
where
    C: IdempotencyKey + Send + Sync,
    S: Send + Sync,
    D: StateComputationTrait<C, S> + Send + Sync,
    R: StateRepository<C, S> + Send + Sync,
{
    /// Create a new state-stored command handler.
    ///
    /// This constructor encapsulates a domain component (decider) and a repository,
    /// providing a convenient API for repeated command execution without needing to
    /// pass the decider on every call.
    ///
    /// # Parameters
    ///
    /// - `decider`: The domain component implementing state computation logic
    /// - `repository`: The state repository for persistence
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
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
    /// # use std::marker::PhantomData;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # struct StateStoredCommandHandler<C, S, D, R>
    /// # where
    /// #     C: IdempotencyKey,
    /// #     D: StateComputationTrait<C, S> + Send + Sync,
    /// #     R: StateRepository<C, S> + Send + Sync,
    /// # {
    /// #     decider: Arc<D>,
    /// #     repository: Arc<R>,
    /// #     _phantom: PhantomData<(C, S)>,
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
    /// # where
    /// #     C: IdempotencyKey,
    /// #     D: StateComputationTrait<C, S> + Send + Sync,
    /// #     R: StateRepository<C, S> + Send + Sync,
    /// # {
    /// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
    /// #         Self { decider, repository, _phantom: PhantomData }
    /// #     }
    /// # }
    ///
    /// let component = Arc::new(AggregateDecider::new(
    ///     |c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![()]) },
    ///     |s: &State, _e: &()| s.clone(),
    ///     || State::default(),
    /// ));
    /// let repository = Arc::new(MyRepository);
    ///
    /// let handler = StateStoredCommandHandler::new(component, repository);
    /// ```
    pub fn new(decider: std::sync::Arc<D>, repository: std::sync::Arc<R>) -> Self {
        Self {
            decider,
            repository,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a command by executing it through the state repository.
    ///
    /// This method delegates to `repository.execute(command, &decider)`, providing
    /// a cleaner API that doesn't require passing the decider on every call.
    ///
    /// # Parameters
    ///
    /// - `command`: The command to execute
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The newly persisted state on success
    /// - `Err(R::Error)`: Any error during fetch, compute, or save stages
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
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
    /// # use std::marker::PhantomData;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # struct StateStoredCommandHandler<C, S, D, R>
    /// # where
    /// #     C: IdempotencyKey,
    /// #     D: StateComputationTrait<C, S> + Send + Sync,
    /// #     R: StateRepository<C, S> + Send + Sync,
    /// # {
    /// #     decider: Arc<D>,
    /// #     repository: Arc<R>,
    /// #     _phantom: PhantomData<(C, S)>,
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
    /// # where
    /// #     C: IdempotencyKey,
    /// #     D: StateComputationTrait<C, S> + Send + Sync,
    /// #     R: StateRepository<C, S> + Send + Sync,
    /// # {
    /// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
    /// #         Self { decider, repository, _phantom: PhantomData }
    /// #     }
    /// #     async fn handle(&self, command: C) -> Result<S, R::Error> {
    /// #         self.repository.execute(command, &*self.decider).await
    /// #     }
    /// # }
    /// # async fn example() -> Result<(), String> {
    /// # let component = Arc::new(AggregateDecider::new(
    /// #     |c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![()]) },
    /// #     |s: &State, _e: &()| s.clone(),
    /// #     || State::default(),
    /// # ));
    /// # let repository = Arc::new(MyRepository);
    /// # let handler = StateStoredCommandHandler::new(component, repository);
    /// // Handle multiple commands
    /// let state1 = handler.handle(Command::Increment { idempotency_key: "req-1".to_string() }).await?;
    /// let state2 = handler.handle(Command::Increment { idempotency_key: "req-2".to_string() }).await?;
    /// let state3 = handler.handle(Command::Decrement { idempotency_key: "req-3".to_string() }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn handle(&self, command: C) -> Result<S, R::Error> {
        self.repository.execute(command, &*self.decider).await
    }

    /// Handle a batch of commands by executing them through the state repository.
    ///
    /// This method delegates to `repository.execute_batch(commands, &decider)`. Commands are
    /// applied in order and the single, final state is returned.
    ///
    /// # Parameters
    ///
    /// - `commands`: The ordered list of commands to execute
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The final persisted state on success
    /// - `Err(R::Error)`: Any error during fetch, compute, or save stages
    pub async fn handle_batch(&self, commands: Vec<C>) -> Result<S, R::Error> {
        self.repository
            .execute_batch(commands, &*self.decider)
            .await
    }
}

/// Command handler for state-stored systems (single-threaded variant).
///
/// This is the single-threaded variant of `StateStoredCommandHandler`, enabled with the
/// `single-threaded` feature flag. It has the same API as the multi-threaded variant
/// but uses `Rc` instead of `Arc` and doesn't require `Send + Sync` bounds.
///
/// See the multi-threaded `StateStoredCommandHandler` documentation for detailed usage
/// information.
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
/// # use fmodel_decider_rust::{StateComputationTrait, AggregateDecider, IdempotencyKey};
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
/// # struct MyRepository;
/// # trait StateRepository<C, S>
/// # where
/// #     C: IdempotencyKey,
/// # {
/// #     type Error;
/// #     async fn execute<D>(&self, command: C, component: &D) -> Result<S, Self::Error>
/// #     where D: StateComputationTrait<C, S>;
/// # }
/// # impl StateRepository<Command, State> for MyRepository {
/// #     type Error = String;
/// #     async fn execute<D>(&self, command: Command, component: &D) -> Result<State, Self::Error>
/// #     where D: StateComputationTrait<Command, State>
/// #     { Ok(State::default()) }
/// # }
/// # use std::marker::PhantomData;
/// # struct StateStoredCommandHandler<C, S, D, R>
/// # where
/// #     C: IdempotencyKey,
/// #     D: StateComputationTrait<C, S>,
/// #     R: StateRepository<C, S>,
/// # {
/// #     decider: Rc<D>,
/// #     repository: Rc<R>,
/// #     _phantom: PhantomData<(C, S)>,
/// # }
/// # impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
/// # where
/// #     C: IdempotencyKey,
/// #     D: StateComputationTrait<C, S>,
/// #     R: StateRepository<C, S>,
/// # {
/// #     fn new(decider: Rc<D>, repository: Rc<R>) -> Self {
/// #         Self { decider, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, command: C) -> Result<S, R::Error> {
/// #         self.repository.execute(command, &*self.decider).await
/// #     }
/// # }
///
/// // Single-threaded handler using Rc instead of Arc
/// let component = Rc::new(AggregateDecider::new(
///     |c: &Command, _s: &State| -> Result<Vec<()>, String> { Ok(vec![()]) },
///     |s: &State, _e: &()| s.clone(),
///     || State::default(),
/// ));
/// let repository = Rc::new(MyRepository);
///
/// let handler = StateStoredCommandHandler::new(component, repository);
/// # }
/// ```
#[cfg(feature = "single-threaded")]
pub struct StateStoredCommandHandler<C, S, D, R>
where
    C: IdempotencyKey,
    D: StateComputationTrait<C, S>,
    R: StateRepository<C, S>,
{
    decider: std::rc::Rc<D>,
    repository: std::rc::Rc<R>,
    _phantom: std::marker::PhantomData<(C, S)>,
}

#[cfg(feature = "single-threaded")]
impl<C, S, D, R> StateStoredCommandHandler<C, S, D, R>
where
    C: IdempotencyKey,
    D: StateComputationTrait<C, S>,
    R: StateRepository<C, S>,
{
    /// Create a new state-stored command handler (single-threaded variant).
    ///
    /// This constructor encapsulates a domain component (decider) and a repository,
    /// providing a convenient API for repeated command execution without needing to
    /// pass the decider on every call.
    ///
    /// # Parameters
    ///
    /// - `decider`: The domain component implementing state computation logic
    /// - `repository`: The state repository for persistence
    ///
    /// See the multi-threaded variant documentation for detailed usage information.
    pub fn new(decider: std::rc::Rc<D>, repository: std::rc::Rc<R>) -> Self {
        Self {
            decider,
            repository,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a command by executing it through the state repository (single-threaded variant).
    ///
    /// This method delegates to `repository.execute(command, &decider)`, providing
    /// a cleaner API that doesn't require passing the decider on every call.
    ///
    /// # Parameters
    ///
    /// - `command`: The command to execute
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The newly persisted state on success
    /// - `Err(R::Error)`: Any error during fetch, compute, or save stages
    ///
    /// See the multi-threaded variant documentation for detailed usage information.
    pub async fn handle(&self, command: C) -> Result<S, R::Error> {
        self.repository.execute(command, &*self.decider).await
    }

    /// Handle a batch of commands by executing them through the state repository.
    ///
    /// This method delegates to `repository.execute_batch(commands, &decider)`. Commands are
    /// applied in order and the single, final state is returned.
    ///
    /// # Parameters
    ///
    /// - `commands`: The ordered list of commands to execute
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The final persisted state on success
    /// - `Err(R::Error)`: Any error during fetch, compute, or save stages
    pub async fn handle_batch(&self, commands: Vec<C>) -> Result<S, R::Error> {
        self.repository
            .execute_batch(commands, &*self.decider)
            .await
    }
}

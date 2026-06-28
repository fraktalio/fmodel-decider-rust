use super::event_repository::EventRepository;
use crate::EventComputationTrait;

// ================================================================================================
// EventSourcedCommandHandler - Convenience Layer
// ================================================================================================

/// Command handler for event-sourced aggregates (multi-threaded variant).
///
/// This struct provides a convenience API that encapsulates a domain component (decider)
/// and a repository together. It eliminates the need to pass the decider on every
/// `execute` call, providing a cleaner API for repeated command execution.
///
/// # Type Parameters
///
/// - `C`: Command type that triggers state changes
/// - `Ei`: Input event type (events loaded from storage)
/// - `Eo`: Output event type (events to be persisted)
/// - `D`: The decider implementing `EventComputationTrait<C, Ei, Eo>`
/// - `R`: The repository implementing `EventRepository<C, Ei, Eo>`
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
/// # let decider = AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<Event>, String> { Ok(vec![Event::Deposited(100)]) },
/// #     |s: &State, e: &Event| s.clone(),
/// #     || State::default(),
/// # );
/// # let repository = Arc::new(MyRepository);
/// // Must pass decider on every call
/// let events1 = repository.execute(Command::Deposit(100), &decider).await?;
/// let events2 = repository.execute(Command::Deposit(200), &decider).await?;
/// let events3 = repository.execute(Command::Deposit(300), &decider).await?;
/// # Ok(())
/// # }
/// ```
///
/// **With handler:**
/// ```rust,no_run
/// # use std::sync::Arc;
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
/// # use std::marker::PhantomData;
/// # #[cfg(not(feature = "single-threaded"))]
/// # struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
/// # where
/// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
/// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
/// # {
/// #     decider: Arc<D>,
/// #     repository: Arc<R>,
/// #     _phantom: PhantomData<(C, Ei, Eo)>,
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
/// # where
/// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
/// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
/// # {
/// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
/// #         Self { decider, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, command: C) -> Result<Vec<Eo>, R::Error> {
/// #         self.repository.execute(command, &*self.decider).await
/// #     }
/// # }
/// # async fn example() -> Result<(), String> {
/// # let decider = Arc::new(AggregateDecider::new(
/// #     |c: &Command, _s: &State| -> Result<Vec<Event>, String> { Ok(vec![Event::Deposited(100)]) },
/// #     |s: &State, e: &Event| s.clone(),
/// #     || State::default(),
/// # ));
/// # let repository = Arc::new(MyRepository);
/// // Decider encapsulated in handler
/// let handler = EventSourcedCommandHandler::new(decider, repository);
/// let events1 = handler.handle(Command::Deposit(100)).await?;
/// let events2 = handler.handle(Command::Deposit(200)).await?;
/// let events3 = handler.handle(Command::Deposit(300)).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Trade-offs
///
/// **Direct Repository:**
/// - ✅ More flexible (can use different deciders per call)
/// - ✅ No additional struct allocation
/// - ❌ More verbose (must pass decider each time)
/// - ❌ Easy to accidentally use wrong decider
///
/// **Command Handler:**
/// - ✅ Less verbose (decider encapsulated)
/// - ✅ Type-safe (impossible to use wrong decider)
/// - ✅ Better for repeated operations on same aggregate
/// - ❌ Less flexible (single decider per handler)
/// - ❌ Additional Arc allocation
///
/// # Usage Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
/// # #[derive(Clone, Debug)]
/// # enum Command { OpenAccount { id: String }, Deposit { id: String, amount: u32 } }
/// # #[derive(Clone, Debug)]
/// # enum Event { AccountOpened { id: String }, MoneyDeposited { id: String, amount: u32 } }
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
/// # use std::marker::PhantomData;
/// # #[cfg(not(feature = "single-threaded"))]
/// # struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
/// # where
/// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
/// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
/// # {
/// #     decider: Arc<D>,
/// #     repository: Arc<R>,
/// #     _phantom: PhantomData<(C, Ei, Eo)>,
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
/// # where
/// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
/// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
/// # {
/// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
/// #         Self { decider, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, command: C) -> Result<Vec<Eo>, R::Error> {
/// #         self.repository.execute(command, &*self.decider).await
/// #     }
/// # }
///
/// # async fn example() -> Result<(), String> {
/// // Create domain component and repository
/// let decider = Arc::new(AggregateDecider::new(
///     |c: &Command, _s: &State| -> Result<Vec<Event>, String> {
///         match c {
///             Command::OpenAccount { id } => Ok(vec![Event::AccountOpened { id: id.clone() }]),
///             Command::Deposit { id, amount } => Ok(vec![Event::MoneyDeposited { id: id.clone(), amount: *amount }]),
///         }
///     },
///     |s: &State, e: &Event| {
///         let mut new_state = s.clone();
///         if let Event::MoneyDeposited { amount, .. } = e {
///             new_state.balance += amount;
///         }
///         new_state
///     },
///     || State::default(),
/// ));
/// let repository = Arc::new(MyRepository);
///
/// // Create handler
/// let handler = EventSourcedCommandHandler::new(decider, repository);
///
/// // Handle commands without passing decider each time
/// let events = handler.handle(Command::OpenAccount { id: "123".to_string() }).await?;
/// let events = handler.handle(Command::Deposit { id: "123".to_string(), amount: 100 }).await?;
/// let events = handler.handle(Command::Deposit { id: "123".to_string(), amount: 50 }).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Relationship to fmodel-ts
///
/// This struct adapts the TypeScript `EventSourcedCommandHandler` class:
///
/// ```typescript
/// export class EventSourcedCommandHandler<C, Ei, Eo, CM, EM> {
///   constructor(
///     private readonly decider: IEventComputation<C, Ei, Eo>,
///     private readonly eventRepository: IEventRepository<C, Ei, Eo, CM, EM>,
///   ) {}
///
///   handle(command: C & CM): Promise<readonly (Eo & EM)[]> {
///     return this.eventRepository.execute(command, this.decider);
///   }
/// }
/// ```
#[cfg(not(feature = "single-threaded"))]
pub struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
where
    D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
    R: EventRepository<C, Ei, Eo> + Send + Sync,
{
    decider: std::sync::Arc<D>,
    repository: std::sync::Arc<R>,
    _phantom: std::marker::PhantomData<(C, Ei, Eo)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
where
    D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
    R: EventRepository<C, Ei, Eo> + Send + Sync,
{
    /// Create a new event-sourced command handler.
    ///
    /// This constructor encapsulates a domain component (decider) and a repository,
    /// providing a convenient API for repeated command execution without needing to
    /// pass the decider on every call.
    ///
    /// # Parameters
    ///
    /// - `decider`: The domain component implementing event computation logic
    /// - `repository`: The event repository for persistence
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
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
    /// # use std::marker::PhantomData;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
    /// # where
    /// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
    /// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
    /// # {
    /// #     decider: Arc<D>,
    /// #     repository: Arc<R>,
    /// #     _phantom: PhantomData<(C, Ei, Eo)>,
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
    /// # where
    /// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
    /// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
    /// # {
    /// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
    /// #         Self { decider, repository, _phantom: PhantomData }
    /// #     }
    /// # }
    ///
    /// let decider = Arc::new(AggregateDecider::new(
    ///     |c: &Command, _s: &State| -> Result<Vec<Event>, String> { Ok(vec![Event::Deposited(100)]) },
    ///     |s: &State, e: &Event| s.clone(),
    ///     || State::default(),
    /// ));
    /// let repository = Arc::new(MyRepository);
    ///
    /// let handler = EventSourcedCommandHandler::new(decider, repository);
    /// ```
    pub fn new(decider: std::sync::Arc<D>, repository: std::sync::Arc<R>) -> Self {
        Self {
            decider,
            repository,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a command by executing it through the event repository.
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
    /// - `Ok(Vec<Eo>)`: The newly persisted output events on success
    /// - `Err(R::Error)`: Any error during fetch, compute, or save stages
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
    /// # #[derive(Clone, Debug)]
    /// # enum Command { Deposit(u32), Withdraw(u32) }
    /// # #[derive(Clone, Debug)]
    /// # enum Event { Deposited(u32), Withdrawn(u32) }
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
    /// # use std::marker::PhantomData;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
    /// # where
    /// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
    /// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
    /// # {
    /// #     decider: Arc<D>,
    /// #     repository: Arc<R>,
    /// #     _phantom: PhantomData<(C, Ei, Eo)>,
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
    /// # where
    /// #     D: EventComputationTrait<C, Ei, Eo> + Send + Sync,
    /// #     R: EventRepository<C, Ei, Eo> + Send + Sync,
    /// # {
    /// #     fn new(decider: Arc<D>, repository: Arc<R>) -> Self {
    /// #         Self { decider, repository, _phantom: PhantomData }
    /// #     }
    /// #     async fn handle(&self, command: C) -> Result<Vec<Eo>, R::Error> {
    /// #         self.repository.execute(command, &*self.decider).await
    /// #     }
    /// # }
    /// # async fn example() -> Result<(), String> {
    /// # let decider = Arc::new(AggregateDecider::new(
    /// #     |c: &Command, _s: &State| -> Result<Vec<Event>, String> { Ok(vec![Event::Deposited(100)]) },
    /// #     |s: &State, e: &Event| s.clone(),
    /// #     || State::default(),
    /// # ));
    /// # let repository = Arc::new(MyRepository);
    /// # let handler = EventSourcedCommandHandler::new(decider, repository);
    /// // Handle multiple commands
    /// let events1 = handler.handle(Command::Deposit(100)).await?;
    /// let events2 = handler.handle(Command::Deposit(200)).await?;
    /// let events3 = handler.handle(Command::Withdraw(50)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn handle(&self, command: C) -> Result<Vec<Eo>, R::Error> {
        self.repository.execute(command, &*self.decider).await
    }
}

/// Command handler for event-sourced aggregates (single-threaded variant).
///
/// This is the single-threaded variant of `EventSourcedCommandHandler`, enabled with the
/// `single-threaded` feature flag. It has the same API as the multi-threaded variant
/// but uses `Rc` instead of `Arc` and doesn't require `Send + Sync` bounds.
///
/// See the multi-threaded `EventSourcedCommandHandler` documentation for detailed usage
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
/// # use fmodel_decider_rust::{EventComputationTrait, AggregateDecider};
/// # #[derive(Clone, Debug)]
/// # enum Command { Deposit(u32) }
/// # #[derive(Clone, Debug)]
/// # enum Event { Deposited(u32) }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { balance: u32 }
/// # struct MyRepository;
/// # trait EventRepository<C, Ei, Eo> {
/// #     type Error;
/// #     async fn execute<D>(&self, command: C, decider: &D) -> Result<Vec<Eo>, Self::Error>
/// #     where D: EventComputationTrait<C, Ei, Eo>;
/// # }
/// # impl EventRepository<Command, Event, Event> for MyRepository {
/// #     type Error = String;
/// #     async fn execute<D>(&self, command: Command, decider: &D) -> Result<Vec<Event>, Self::Error>
/// #     where D: EventComputationTrait<Command, Event, Event>
/// #     { Ok(vec![]) }
/// # }
/// # use std::marker::PhantomData;
/// # struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
/// # where
/// #     D: EventComputationTrait<C, Ei, Eo>,
/// #     R: EventRepository<C, Ei, Eo>,
/// # {
/// #     decider: Rc<D>,
/// #     repository: Rc<R>,
/// #     _phantom: PhantomData<(C, Ei, Eo)>,
/// # }
/// # impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
/// # where
/// #     D: EventComputationTrait<C, Ei, Eo>,
/// #     R: EventRepository<C, Ei, Eo>,
/// # {
/// #     fn new(decider: Rc<D>, repository: Rc<R>) -> Self {
/// #         Self { decider, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, command: C) -> Result<Vec<Eo>, R::Error> {
/// #         self.repository.execute(command, &*self.decider).await
/// #     }
/// # }
///
/// // Single-threaded handler using Rc instead of Arc
/// let decider = Rc::new(AggregateDecider::new(
///     |c: &Command, _s: &State| -> Result<Vec<Event>, String> { Ok(vec![Event::Deposited(100)]) },
///     |s: &State, e: &Event| s.clone(),
///     || State::default(),
/// ));
/// let repository = Rc::new(MyRepository);
///
/// let handler = EventSourcedCommandHandler::new(decider, repository);
/// # }
/// ```
#[cfg(feature = "single-threaded")]
pub struct EventSourcedCommandHandler<C, Ei, Eo, D, R>
where
    D: EventComputationTrait<C, Ei, Eo>,
    R: EventRepository<C, Ei, Eo>,
{
    decider: std::rc::Rc<D>,
    repository: std::rc::Rc<R>,
    _phantom: std::marker::PhantomData<(C, Ei, Eo)>,
}

#[cfg(feature = "single-threaded")]
impl<C, Ei, Eo, D, R> EventSourcedCommandHandler<C, Ei, Eo, D, R>
where
    D: EventComputationTrait<C, Ei, Eo>,
    R: EventRepository<C, Ei, Eo>,
{
    /// Create a new event-sourced command handler (single-threaded variant).
    ///
    /// This constructor encapsulates a domain component (decider) and a repository,
    /// providing a convenient API for repeated command execution without needing to
    /// pass the decider on every call.
    ///
    /// # Parameters
    ///
    /// - `decider`: The domain component implementing event computation logic
    /// - `repository`: The event repository for persistence
    ///
    /// See the multi-threaded variant documentation for detailed usage information.
    pub fn new(decider: std::rc::Rc<D>, repository: std::rc::Rc<R>) -> Self {
        Self {
            decider,
            repository,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a command by executing it through the event repository (single-threaded variant).
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
    /// - `Ok(Vec<Eo>)`: The newly persisted output events on success
    /// - `Err(R::Error)`: Any error during fetch, compute, or save stages
    ///
    /// See the multi-threaded variant documentation for detailed usage information.
    pub async fn handle(&self, command: C) -> Result<Vec<Eo>, R::Error> {
        self.repository.execute(command, &*self.decider).await
    }
}

use super::view_repository::ViewRepository;
use crate::{EventMeta, ViewTrait};

// ================================================================================================
// MaterializedViewHandler - Convenience Layer
// ================================================================================================

/// Handler for materialized views (multi-threaded variant).
///
/// This struct provides a convenience API that encapsulates a view component and a repository
/// together. It eliminates the need to pass the view on every `execute` call, providing a
/// cleaner API for repeated event processing.
///
/// # Type Parameters
///
/// - `E`: Event type that triggers view updates. Must implement [`EventMeta`].
/// - `S`: State type (both current and evolved state)
/// - `V`: The view implementing `ViewTrait<S, S, E>`
/// - `R`: The repository implementing `ViewRepository<E, S>`
///
/// # Thread Safety
///
/// In multi-threaded mode (default), this struct uses `Arc` for shared ownership and
/// requires `Send + Sync` bounds on both the view and repository. This makes it
/// safe to share across threads in concurrent applications.
///
/// # Motivation
///
/// **Without handler (direct repository usage):**
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
/// # #[derive(Clone, Debug)]
/// # enum Event { ItemAdded(String), ItemRemoved(String) }
/// # impl EventMeta for Event {
/// #     fn event_type(&self) -> &str {
/// #         match self {
/// #             Event::ItemAdded(_) => "ItemAdded",
/// #             Event::ItemRemoved(_) => "ItemRemoved",
/// #         }
/// #     }
/// #     fn tags(&self) -> Vec<Tag> {
/// #         match self {
/// #             Event::ItemAdded(item) => vec![Tag::new("item", item.clone())],
/// #             Event::ItemRemoved(item) => vec![Tag::new("item", item.clone())],
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { items: Vec<String> }
/// # struct MyRepository;
/// # #[cfg(not(feature = "single-threaded"))]
/// # trait ViewRepository<E, S>: Send + Sync
/// # where
/// #     E: EventMeta,
/// # {
/// #     type Error;
/// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
/// #     where V: ViewTrait<S, S, E> + Send + Sync;
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl ViewRepository<Event, State> for MyRepository {
/// #     type Error = String;
/// #     async fn execute<V>(&self, event: Event, view: &V) -> Result<State, Self::Error>
/// #     where V: ViewTrait<State, State, Event> + Send + Sync
/// #     { Ok(State::default()) }
/// # }
/// # async fn example() -> Result<(), String> {
/// # let view = Projection::new(
/// #     |s: &State, e: &Event| s.clone(),
/// #     || State::default(),
/// # );
/// # let repository = Arc::new(MyRepository);
/// // Must pass view on every call
/// let state1 = repository.execute(Event::ItemAdded("item1".to_string()), &view).await?;
/// let state2 = repository.execute(Event::ItemAdded("item2".to_string()), &view).await?;
/// let state3 = repository.execute(Event::ItemRemoved("item1".to_string()), &view).await?;
/// # Ok(())
/// # }
/// ```
///
/// **With handler:**
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
/// # #[derive(Clone, Debug)]
/// # enum Event { ItemAdded(String), ItemRemoved(String) }
/// # impl EventMeta for Event {
/// #     fn event_type(&self) -> &str {
/// #         match self {
/// #             Event::ItemAdded(_) => "ItemAdded",
/// #             Event::ItemRemoved(_) => "ItemRemoved",
/// #         }
/// #     }
/// #     fn tags(&self) -> Vec<Tag> {
/// #         match self {
/// #             Event::ItemAdded(item) => vec![Tag::new("item", item.clone())],
/// #             Event::ItemRemoved(item) => vec![Tag::new("item", item.clone())],
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { items: Vec<String> }
/// # struct MyRepository;
/// # #[cfg(not(feature = "single-threaded"))]
/// # trait ViewRepository<E, S>: Send + Sync
/// # where
/// #     E: EventMeta,
/// # {
/// #     type Error;
/// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
/// #     where V: ViewTrait<S, S, E> + Send + Sync;
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl ViewRepository<Event, State> for MyRepository {
/// #     type Error = String;
/// #     async fn execute<V>(&self, event: Event, view: &V) -> Result<State, Self::Error>
/// #     where V: ViewTrait<State, State, Event> + Send + Sync
/// #     { Ok(State::default()) }
/// # }
/// # use std::marker::PhantomData;
/// # #[cfg(not(feature = "single-threaded"))]
/// # struct MaterializedViewHandler<E, S, V, R>
/// # where
/// #     E: EventMeta,
/// #     V: ViewTrait<S, S, E> + Send + Sync,
/// #     R: ViewRepository<E, S> + Send + Sync,
/// # {
/// #     view: Arc<V>,
/// #     repository: Arc<R>,
/// #     _phantom: PhantomData<(E, S)>,
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
/// # where
/// #     E: EventMeta,
/// #     V: ViewTrait<S, S, E> + Send + Sync,
/// #     R: ViewRepository<E, S> + Send + Sync,
/// # {
/// #     fn new(view: Arc<V>, repository: Arc<R>) -> Self {
/// #         Self { view, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, event: E) -> Result<S, R::Error> {
/// #         self.repository.execute(event, &*self.view).await
/// #     }
/// # }
/// # async fn example() -> Result<(), String> {
/// # let view = Arc::new(Projection::new(
/// #     |s: &State, e: &Event| s.clone(),
/// #     || State::default(),
/// # ));
/// # let repository = Arc::new(MyRepository);
/// // View encapsulated in handler
/// let handler = MaterializedViewHandler::new(view, repository);
/// let state1 = handler.handle(Event::ItemAdded("item1".to_string())).await?;
/// let state2 = handler.handle(Event::ItemAdded("item2".to_string())).await?;
/// let state3 = handler.handle(Event::ItemRemoved("item1".to_string())).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Trade-offs
///
/// **Direct Repository:**
/// - ✅ More flexible (can use different views per call)
/// - ✅ No additional struct allocation
/// - ❌ More verbose (must pass view each time)
/// - ❌ Easy to accidentally use wrong view
///
/// **View Handler:**
/// - ✅ Less verbose (view encapsulated)
/// - ✅ Type-safe (impossible to use wrong view)
/// - ✅ Better for repeated event processing on same view
/// - ❌ Less flexible (single view per handler)
/// - ❌ Additional Arc allocation
///
/// # Usage Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
/// # #[derive(Clone, Debug)]
/// # enum Event { AccountOpened { id: String }, MoneyDeposited { id: String, amount: u32 }, MoneyWithdrawn { id: String, amount: u32 } }
/// # impl EventMeta for Event {
/// #     fn event_type(&self) -> &str {
/// #         match self {
/// #             Event::AccountOpened { .. } => "AccountOpened",
/// #             Event::MoneyDeposited { .. } => "MoneyDeposited",
/// #             Event::MoneyWithdrawn { .. } => "MoneyWithdrawn",
/// #         }
/// #     }
/// #     fn tags(&self) -> Vec<Tag> {
/// #         match self {
/// #             Event::AccountOpened { id } => vec![Tag::new("id", id.clone())],
/// #             Event::MoneyDeposited { id, .. } => vec![Tag::new("id", id.clone())],
/// #             Event::MoneyWithdrawn { id, .. } => vec![Tag::new("id", id.clone())],
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { balance: u32 }
/// # struct MyRepository;
/// # #[cfg(not(feature = "single-threaded"))]
/// # trait ViewRepository<E, S>: Send + Sync
/// # where
/// #     E: EventMeta,
/// # {
/// #     type Error;
/// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
/// #     where V: ViewTrait<S, S, E> + Send + Sync;
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl ViewRepository<Event, State> for MyRepository {
/// #     type Error = String;
/// #     async fn execute<V>(&self, event: Event, view: &V) -> Result<State, Self::Error>
/// #     where V: ViewTrait<State, State, Event> + Send + Sync
/// #     { Ok(State::default()) }
/// # }
/// # use std::marker::PhantomData;
/// # #[cfg(not(feature = "single-threaded"))]
/// # struct MaterializedViewHandler<E, S, V, R>
/// # where
/// #     E: EventMeta,
/// #     V: ViewTrait<S, S, E> + Send + Sync,
/// #     R: ViewRepository<E, S> + Send + Sync,
/// # {
/// #     view: Arc<V>,
/// #     repository: Arc<R>,
/// #     _phantom: PhantomData<(E, S)>,
/// # }
/// # #[cfg(not(feature = "single-threaded"))]
/// # impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
/// # where
/// #     E: EventMeta,
/// #     V: ViewTrait<S, S, E> + Send + Sync,
/// #     R: ViewRepository<E, S> + Send + Sync,
/// # {
/// #     fn new(view: Arc<V>, repository: Arc<R>) -> Self {
/// #         Self { view, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, event: E) -> Result<S, R::Error> {
/// #         self.repository.execute(event, &*self.view).await
/// #     }
/// # }
///
/// # async fn example() -> Result<(), String> {
/// // Create view component and repository
/// let view = Arc::new(Projection::new(
///     |s: &State, e: &Event| {
///         let mut new_state = s.clone();
///         match e {
///             Event::AccountOpened { .. } => {
///                 new_state.balance = 0;
///             }
///             Event::MoneyDeposited { amount, .. } => {
///                 new_state.balance += amount;
///             }
///             Event::MoneyWithdrawn { amount, .. } => {
///                 new_state.balance = new_state.balance.saturating_sub(*amount);
///             }
///         }
///         new_state
///     },
///     || State::default(),
/// ));
/// let repository = Arc::new(MyRepository);
///
/// // Create handler
/// let handler = MaterializedViewHandler::new(view, repository);
///
/// // Handle events without passing view each time
/// let state = handler.handle(Event::AccountOpened { id: "123".to_string() }).await?;
/// let state = handler.handle(Event::MoneyDeposited { id: "123".to_string(), amount: 100 }).await?;
/// let state = handler.handle(Event::MoneyWithdrawn { id: "123".to_string(), amount: 50 }).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(not(feature = "single-threaded"))]
pub struct MaterializedViewHandler<E, S, V, R>
where
    E: EventMeta,
    V: ViewTrait<S, S, E> + Send + Sync,
    R: ViewRepository<E, S> + Send + Sync,
{
    view: std::sync::Arc<V>,
    repository: std::sync::Arc<R>,
    _phantom: std::marker::PhantomData<(E, S)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
where
    E: EventMeta,
    V: ViewTrait<S, S, E> + Send + Sync,
    R: ViewRepository<E, S> + Send + Sync,
{
    /// Create a new materialized view handler.
    ///
    /// This constructor encapsulates a view component and a repository, providing a
    /// convenient API for repeated event processing without needing to pass the view
    /// on every call.
    ///
    /// # Parameters
    ///
    /// - `view`: The view component implementing state evolution logic
    /// - `repository`: The view repository for persistence
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
    /// # #[derive(Clone, Debug)]
    /// # enum Event { ItemAdded(String) }
    /// # impl EventMeta for Event {
    /// #     fn event_type(&self) -> &str {
    /// #         match self {
    /// #             Event::ItemAdded(_) => "ItemAdded",
    /// #         }
    /// #     }
    /// #     fn tags(&self) -> Vec<Tag> {
    /// #         match self {
    /// #             Event::ItemAdded(item) => vec![Tag::new("item", item.clone())],
    /// #         }
    /// #     }
    /// # }
    /// # #[derive(Clone, Debug, Default)]
    /// # struct State { items: Vec<String> }
    /// # struct MyRepository;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # trait ViewRepository<E, S>: Send + Sync
    /// # where
    /// #     E: EventMeta,
    /// # {
    /// #     type Error;
    /// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
    /// #     where V: ViewTrait<S, S, E> + Send + Sync;
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl ViewRepository<Event, State> for MyRepository {
    /// #     type Error = String;
    /// #     async fn execute<V>(&self, event: Event, view: &V) -> Result<State, Self::Error>
    /// #     where V: ViewTrait<State, State, Event> + Send + Sync
    /// #     { Ok(State::default()) }
    /// # }
    /// # use std::marker::PhantomData;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # struct MaterializedViewHandler<E, S, V, R>
    /// # where
    /// #     E: EventMeta,
    /// #     V: ViewTrait<S, S, E> + Send + Sync,
    /// #     R: ViewRepository<E, S> + Send + Sync,
    /// # {
    /// #     view: Arc<V>,
    /// #     repository: Arc<R>,
    /// #     _phantom: PhantomData<(E, S)>,
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
    /// # where
    /// #     E: EventMeta,
    /// #     V: ViewTrait<S, S, E> + Send + Sync,
    /// #     R: ViewRepository<E, S> + Send + Sync,
    /// # {
    /// #     fn new(view: Arc<V>, repository: Arc<R>) -> Self {
    /// #         Self { view, repository, _phantom: PhantomData }
    /// #     }
    /// # }
    ///
    /// let view = Arc::new(Projection::new(
    ///     |s: &State, e: &Event| {
    ///         let mut new_state = s.clone();
    ///         if let Event::ItemAdded(item) = e {
    ///             new_state.items.push(item.clone());
    ///         }
    ///         new_state
    ///     },
    ///     || State::default(),
    /// ));
    /// let repository = Arc::new(MyRepository);
    ///
    /// let handler = MaterializedViewHandler::new(view, repository);
    /// ```
    pub fn new(view: std::sync::Arc<V>, repository: std::sync::Arc<R>) -> Self {
        Self {
            view,
            repository,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle an event by executing it through the view repository.
    ///
    /// This method delegates to `repository.execute(event, &view)`, providing a cleaner
    /// API that doesn't require passing the view on every call.
    ///
    /// # Parameters
    ///
    /// - `event`: The event to process
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The newly persisted state on success
    /// - `Err(R::Error)`: Any error during fetch, evolve, or save stages
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
    /// # #[derive(Clone, Debug)]
    /// # enum Event { ItemAdded(String), ItemRemoved(String) }
    /// # impl EventMeta for Event {
    /// #     fn event_type(&self) -> &str {
    /// #         match self {
    /// #             Event::ItemAdded(_) => "ItemAdded",
    /// #             Event::ItemRemoved(_) => "ItemRemoved",
    /// #         }
    /// #     }
    /// #     fn tags(&self) -> Vec<Tag> {
    /// #         match self {
    /// #             Event::ItemAdded(item) => vec![Tag::new("item", item.clone())],
    /// #             Event::ItemRemoved(item) => vec![Tag::new("item", item.clone())],
    /// #         }
    /// #     }
    /// # }
    /// # #[derive(Clone, Debug, Default)]
    /// # struct State { items: Vec<String> }
    /// # struct MyRepository;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # trait ViewRepository<E, S>: Send + Sync
    /// # where
    /// #     E: EventMeta,
    /// # {
    /// #     type Error;
    /// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
    /// #     where V: ViewTrait<S, S, E> + Send + Sync;
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl ViewRepository<Event, State> for MyRepository {
    /// #     type Error = String;
    /// #     async fn execute<V>(&self, event: Event, view: &V) -> Result<State, Self::Error>
    /// #     where V: ViewTrait<State, State, Event> + Send + Sync
    /// #     { Ok(State::default()) }
    /// # }
    /// # use std::marker::PhantomData;
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # struct MaterializedViewHandler<E, S, V, R>
    /// # where
    /// #     E: EventMeta,
    /// #     V: ViewTrait<S, S, E> + Send + Sync,
    /// #     R: ViewRepository<E, S> + Send + Sync,
    /// # {
    /// #     view: Arc<V>,
    /// #     repository: Arc<R>,
    /// #     _phantom: PhantomData<(E, S)>,
    /// # }
    /// # #[cfg(not(feature = "single-threaded"))]
    /// # impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
    /// # where
    /// #     E: EventMeta,
    /// #     V: ViewTrait<S, S, E> + Send + Sync,
    /// #     R: ViewRepository<E, S> + Send + Sync,
    /// # {
    /// #     fn new(view: Arc<V>, repository: Arc<R>) -> Self {
    /// #         Self { view, repository, _phantom: PhantomData }
    /// #     }
    /// #     async fn handle(&self, event: E) -> Result<S, R::Error> {
    /// #         self.repository.execute(event, &*self.view).await
    /// #     }
    /// # }
    /// # async fn example() -> Result<(), String> {
    /// # let view = Arc::new(Projection::new(
    /// #     |s: &State, e: &Event| s.clone(),
    /// #     || State::default(),
    /// # ));
    /// # let repository = Arc::new(MyRepository);
    /// # let handler = MaterializedViewHandler::new(view, repository);
    /// // Handle multiple events
    /// let state1 = handler.handle(Event::ItemAdded("item1".to_string())).await?;
    /// let state2 = handler.handle(Event::ItemAdded("item2".to_string())).await?;
    /// let state3 = handler.handle(Event::ItemRemoved("item1".to_string())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn handle(&self, event: E) -> Result<S, R::Error> {
        self.repository.execute(event, &*self.view).await
    }

    /// Handle a batch of events by executing them through the view repository.
    ///
    /// This method delegates to `repository.execute_batch(events, &view)`. Events are applied
    /// in order and one persisted state is returned per input event.
    ///
    /// # Parameters
    ///
    /// - `events`: The ordered list of events to process
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<S>)`: The persisted states, one per input event, in order
    /// - `Err(R::Error)`: Any error during fetch, evolve, or save stages
    pub async fn handle_batch(&self, events: Vec<E>) -> Result<Vec<S>, R::Error> {
        self.repository.execute_batch(events, &*self.view).await
    }
}

/// Handler for materialized views (single-threaded variant).
///
/// This is the single-threaded variant of `MaterializedViewHandler`, enabled with the
/// `single-threaded` feature flag. It has the same API as the multi-threaded variant
/// but uses `Rc` instead of `Arc` and doesn't require `Send + Sync` bounds.
///
/// See the multi-threaded `MaterializedViewHandler` documentation for detailed usage
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
/// - `Rc`-based view components
/// - Performance-critical single-threaded scenarios
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "single-threaded")]
/// # {
/// use std::rc::Rc;
/// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
/// # #[derive(Clone, Debug)]
/// # enum Event { ItemAdded(String) }
/// # impl EventMeta for Event {
/// #     fn event_type(&self) -> &str {
/// #         match self {
/// #             Event::ItemAdded(_) => "ItemAdded",
/// #         }
/// #     }
/// #     fn tags(&self) -> Vec<Tag> {
/// #         match self {
/// #             Event::ItemAdded(item) => vec![Tag::new("item", item.clone())],
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { items: Vec<String> }
/// # struct MyRepository;
/// # trait ViewRepository<E, S>
/// # where
/// #     E: EventMeta,
/// # {
/// #     type Error;
/// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
/// #     where V: ViewTrait<S, S, E>;
/// # }
/// # impl ViewRepository<Event, State> for MyRepository {
/// #     type Error = String;
/// #     async fn execute<V>(&self, event: Event, view: &V) -> Result<State, Self::Error>
/// #     where V: ViewTrait<State, State, Event>
/// #     { Ok(State::default()) }
/// # }
/// # use std::marker::PhantomData;
/// # struct MaterializedViewHandler<E, S, V, R>
/// # where
/// #     E: EventMeta,
/// #     V: ViewTrait<S, S, E>,
/// #     R: ViewRepository<E, S>,
/// # {
/// #     view: Rc<V>,
/// #     repository: Rc<R>,
/// #     _phantom: PhantomData<(E, S)>,
/// # }
/// # impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
/// # where
/// #     E: EventMeta,
/// #     V: ViewTrait<S, S, E>,
/// #     R: ViewRepository<E, S>,
/// # {
/// #     fn new(view: Rc<V>, repository: Rc<R>) -> Self {
/// #         Self { view, repository, _phantom: PhantomData }
/// #     }
/// #     async fn handle(&self, event: E) -> Result<S, R::Error> {
/// #         self.repository.execute(event, &*self.view).await
/// #     }
/// # }
///
/// // Single-threaded handler using Rc instead of Arc
/// let view = Rc::new(Projection::new(
///     |s: &State, e: &Event| {
///         let mut new_state = s.clone();
///         if let Event::ItemAdded(item) = e {
///             new_state.items.push(item.clone());
///         }
///         new_state
///     },
///     || State::default(),
/// ));
/// let repository = Rc::new(MyRepository);
///
/// let handler = MaterializedViewHandler::new(view, repository);
/// # }
/// ```
#[cfg(feature = "single-threaded")]
pub struct MaterializedViewHandler<E, S, V, R>
where
    E: EventMeta,
    V: ViewTrait<S, S, E>,
    R: ViewRepository<E, S>,
{
    view: std::rc::Rc<V>,
    repository: std::rc::Rc<R>,
    _phantom: std::marker::PhantomData<(E, S)>,
}

#[cfg(feature = "single-threaded")]
impl<E, S, V, R> MaterializedViewHandler<E, S, V, R>
where
    E: EventMeta,
    V: ViewTrait<S, S, E>,
    R: ViewRepository<E, S>,
{
    /// Create a new materialized view handler (single-threaded variant).
    ///
    /// This constructor encapsulates a view component and a repository, providing a
    /// convenient API for repeated event processing without needing to pass the view
    /// on every call.
    ///
    /// # Parameters
    ///
    /// - `view`: The view component implementing state evolution logic
    /// - `repository`: The view repository for persistence
    ///
    /// See the multi-threaded variant documentation for detailed usage information.
    pub fn new(view: std::rc::Rc<V>, repository: std::rc::Rc<R>) -> Self {
        Self {
            view,
            repository,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle an event by executing it through the view repository (single-threaded variant).
    ///
    /// This method delegates to `repository.execute(event, &view)`, providing a cleaner
    /// API that doesn't require passing the view on every call.
    ///
    /// # Parameters
    ///
    /// - `event`: The event to process
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The newly persisted state on success
    /// - `Err(R::Error)`: Any error during fetch, evolve, or save stages
    ///
    /// See the multi-threaded variant documentation for detailed usage information.
    pub async fn handle(&self, event: E) -> Result<S, R::Error> {
        self.repository.execute(event, &*self.view).await
    }

    /// Handle a batch of events by executing them through the view repository.
    ///
    /// This method delegates to `repository.execute_batch(events, &view)`. Events are applied
    /// in order and one persisted state is returned per input event.
    ///
    /// # Parameters
    ///
    /// - `events`: The ordered list of events to process
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<S>)`: The persisted states, one per input event, in order
    /// - `Err(R::Error)`: Any error during fetch, evolve, or save stages
    pub async fn handle_batch(&self, events: Vec<E>) -> Result<Vec<S>, R::Error> {
        self.repository.execute_batch(events, &*self.view).await
    }
}

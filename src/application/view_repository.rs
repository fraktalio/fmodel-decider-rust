use crate::{EventMeta, ViewTrait};

// ================================================================================================
// ViewRepository Trait
// ================================================================================================

/// Repository trait for materialized views.
///
/// This trait defines the contract for persisting and retrieving view state in a materialized
/// view system. It encapsulates the complete transactional flow: fetch state → evolve state
/// → save state.
///
/// # Type Parameters
///
/// - `E`: Event type that triggers view updates. Must implement [`EventMeta`] so repository
///   implementations can build secondary indexes.
/// - `S`: State type (both current and evolved state)
///
/// # Associated Types
///
/// - `Error`: Repository-specific error type for fetch, evolve, or save failures
///
/// # The Execute Pattern
///
/// The `execute` method implements a three-stage transactional pattern:
///
/// ```text
/// 1. FETCH  → Load current view state from storage
/// 2. EVOLVE → Call evolve on the provided view component
/// 3. SAVE   → Persist the newly evolved state
/// ```
///
/// This pattern ensures atomicity: all three stages succeed or fail together.
///
/// # Difference from EventRepository and StateRepository
///
/// While `EventRepository` works with commands and event-sourced aggregates, and
/// `StateRepository` works with commands and state snapshots, `ViewRepository` works
/// with events and materialized views. The key differences:
///
/// - **EventRepository**: Command → Events (write-side, event-sourced)
/// - **StateRepository**: Command → State (write-side, state-stored)
/// - **ViewRepository**: Event → State (read-side, materialized views)
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
/// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
///
/// # #[derive(Clone, Debug)]
/// # enum Event { AccountOpened { id: String }, MoneyDeposited { id: String, amount: u32 } }
/// # impl EventMeta for Event {
/// #     fn event_type(&self) -> &str {
/// #         match self {
/// #             Event::AccountOpened { .. } => "AccountOpened",
/// #             Event::MoneyDeposited { .. } => "MoneyDeposited",
/// #         }
/// #     }
/// #     fn tags(&self) -> Vec<Tag> {
/// #         match self {
/// #             Event::AccountOpened { id } => vec![Tag::new("id", id.clone())],
/// #             Event::MoneyDeposited { id, .. } => vec![Tag::new("id", id.clone())],
/// #         }
/// #     }
/// # }
/// # #[derive(Clone, Debug, Default)]
/// # struct State { balance: u32 }
///
/// // Define your repository implementation
/// struct InMemoryViewRepository {
///     views: Arc<Mutex<HashMap<String, State>>>,
/// }
///
/// # #[cfg(not(feature = "single-threaded"))]
/// # trait ViewRepository<E, S>: Send + Sync
/// # where
/// #     E: EventMeta,
/// # {
/// #     type Error;
/// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
/// #     where V: ViewTrait<S, S, E> + Send + Sync;
/// # }
///
/// # #[cfg(not(feature = "single-threaded"))]
/// impl ViewRepository<Event, State> for InMemoryViewRepository {
///     type Error = String;
///
///     async fn execute<V>(
///         &self,
///         event: Event,
///         view: &V,
///     ) -> Result<State, Self::Error>
///     where
///         V: ViewTrait<State, State, Event> + Send + Sync,
///     {
///         // 1. FETCH: Extract view ID and load current state
///         let view_id = match &event {
///             Event::AccountOpened { id } => id.clone(),
///             Event::MoneyDeposited { id, .. } => id.clone(),
///         };
///         let mut views = self.views.lock().unwrap();
///         let current_state = views
///             .get(&view_id)
///             .cloned()
///             .unwrap_or_else(|| view.initial_state());
///
///         // 2. EVOLVE: Apply view logic to evolve state
///         let new_state = view.evolve(&current_state, &event);
///
///         // 3. SAVE: Persist evolved state
///         views.insert(view_id, new_state.clone());
///
///         Ok(new_state)
///     }
/// }
///
/// // Usage with a view component
/// # async fn example() -> Result<(), String> {
/// # let view = Projection::new(
/// #     |s: &State, e: &Event| {
/// #         let mut new_state = s.clone();
/// #         match e {
/// #             Event::MoneyDeposited { amount, .. } => {
/// #                 new_state.balance += amount;
/// #             }
/// #             _ => {}
/// #         }
/// #         new_state
/// #     },
/// #     || State::default(),
/// # );
/// # let repository = InMemoryViewRepository {
/// #     views: Arc::new(Mutex::new(HashMap::new())),
/// # };
/// let event = Event::MoneyDeposited { id: "acc-123".to_string(), amount: 100 };
///
/// // Execute event through repository
/// let state = repository.execute(event, &view).await?;
/// println!("Updated view state: {:?}", state);
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
/// - **Evolve failures**: View logic errors (though typically views are pure and don't fail)
/// - **Save failures**: Storage persistence errors (write conflicts, transaction failures, etc.)
///
/// Implementers should provide rich error types that capture context about which stage failed:
///
/// ```rust
/// #[derive(Debug)]
/// enum RepositoryError {
///     FetchFailed(String),
///     EvolveFailed(String),
///     SaveFailed(String),
///     TransactionFailed { stage: String, cause: String },
/// }
/// ```
#[cfg(not(feature = "single-threaded"))]
pub trait ViewRepository<E, S>: Send + Sync
where
    E: EventMeta,
{
    /// Error type for repository operations.
    ///
    /// This should capture both infrastructure errors (fetch/save failures) and view
    /// errors (evolve failures, though typically rare). Implementers can define rich
    /// error types that provide context about which stage of the execute pattern failed.
    type Error;

    /// Execute an event against a materialized view.
    ///
    /// This method encapsulates the complete transactional flow:
    /// 1. **Fetch**: Load current view state from storage
    /// 2. **Evolve**: Call `evolve` on the provided view component
    /// 3. **Save**: Persist the newly evolved state
    /// 4. **Return**: The persisted state
    ///
    /// # Type Parameters
    ///
    /// - `V`: The view implementing `ViewTrait<S, S, E>`
    ///
    /// # Parameters
    ///
    /// - `event`: The event to process (contains view ID and event data)
    /// - `view`: Reference to the view component that evolves state
    ///
    /// # Returns
    ///
    /// - `Ok(S)`: The newly persisted state on success
    /// - `Err(Self::Error)`: Any error during fetch, evolve, or save stages
    ///
    /// # Atomicity
    ///
    /// Implementations should ensure atomicity: if any stage fails, no state changes are
    /// persisted. The exact mechanism depends on the underlying storage (transactions,
    /// optimistic locking, etc.).
    ///
    /// # Example
    ///
    /// ```rust,no_run
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
    /// # async fn example() -> Result<(), String> {
    /// # let repository = MyRepository;
    /// # let view = Projection::new(
    /// #     |s: &State, e: &Event| {
    /// #         let mut new_state = s.clone();
    /// #         if let Event::ItemAdded(item) = e {
    /// #             new_state.items.push(item.clone());
    /// #         }
    /// #         new_state
    /// #     },
    /// #     || State::default(),
    /// # );
    /// let event = Event::ItemAdded("item1".to_string());
    /// let state = repository.execute(event, &view).await?;
    /// # Ok(())
    /// # }
    /// ```
    fn execute<V>(
        &self,
        event: E,
        view: &V,
    ) -> impl std::future::Future<Output = Result<S, Self::Error>> + Send
    where
        V: ViewTrait<S, S, E> + Send + Sync;

    /// Execute a batch of events against a materialized view.
    ///
    /// This is the batch counterpart of [`execute`](Self::execute). It processes an ordered
    /// list of events and returns the persisted state after each one, as a `Vec<S>` aligned
    /// with the input events.
    ///
    /// # Semantics
    ///
    /// Events are applied **in order**. Because events in a batch may target different view
    /// instances (identified independently by each event), this method returns one persisted
    /// state per input event rather than a single folded state — the `i`-th element of the
    /// result is the state persisted after processing `events[i]`.
    ///
    /// # Atomicity
    ///
    /// Implementations should treat the batch as a single unit of work: either all events are
    /// applied and their resulting states persisted, or none are. The exact mechanism depends
    /// on the underlying storage (transactions, optimistic locking, etc.).
    ///
    /// # Type Parameters
    ///
    /// - `V`: The view implementing `ViewTrait<S, S, E>`
    ///
    /// # Parameters
    ///
    /// - `events`: The ordered list of events to process
    /// - `view`: Reference to the view component that evolves state
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<S>)`: The persisted states, one per input event, in order
    /// - `Err(Self::Error)`: Any error during fetch, evolve, or save stages
    fn execute_batch<V>(
        &self,
        events: Vec<E>,
        view: &V,
    ) -> impl std::future::Future<Output = Result<Vec<S>, Self::Error>> + Send
    where
        V: ViewTrait<S, S, E> + Send + Sync;
}

/// Repository trait for materialized views (single-threaded variant).
///
/// This is the single-threaded variant of `ViewRepository`, enabled with the
/// `single-threaded` feature flag. It has the same API as the multi-threaded variant
/// but without `Send + Sync` bounds, allowing use with `Rc`-based view components
/// for better single-threaded performance.
///
/// See the multi-threaded `ViewRepository` documentation for detailed usage information.
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
/// use std::cell::RefCell;
/// use std::collections::HashMap;
/// # use fmodel_decider_rust::{ViewTrait, Projection, EventMeta, Tag};
///
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
///
/// // Single-threaded repository using Rc instead of Arc
/// struct SingleThreadedViewRepository {
///     views: Rc<RefCell<HashMap<String, State>>>,
/// }
///
/// # trait ViewRepository<E, S>
/// # where
/// #     E: EventMeta,
/// # {
/// #     type Error;
/// #     async fn execute<V>(&self, event: E, view: &V) -> Result<S, Self::Error>
/// #     where V: ViewTrait<S, S, E>;
/// # }
///
/// impl ViewRepository<Event, State> for SingleThreadedViewRepository {
///     type Error = String;
///
///     async fn execute<V>(
///         &self,
///         event: Event,
///         view: &V,
///     ) -> Result<State, Self::Error>
///     where
///         V: ViewTrait<State, State, Event>,
///     {
///         // Implementation using Rc/RefCell instead of Arc/Mutex
///         # Ok(State::default())
///     }
/// }
/// # }
/// ```
#[cfg(feature = "single-threaded")]
pub trait ViewRepository<E, S>
where
    E: EventMeta,
{
    /// Error type for repository operations.
    type Error;

    /// Execute an event against a materialized view.
    ///
    /// See the multi-threaded variant documentation for detailed information.
    fn execute<V>(
        &self,
        event: E,
        view: &V,
    ) -> impl std::future::Future<Output = Result<S, Self::Error>>
    where
        V: ViewTrait<S, S, E>;

    /// Execute a batch of events against a materialized view.
    ///
    /// See the multi-threaded variant documentation for detailed information.
    fn execute_batch<V>(
        &self,
        events: Vec<E>,
        view: &V,
    ) -> impl std::future::Future<Output = Result<Vec<S>, Self::Error>>
    where
        V: ViewTrait<S, S, E>;
}

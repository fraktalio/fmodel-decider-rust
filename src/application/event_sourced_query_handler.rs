use crate::{EventMeta, Tag, ViewTrait};

// ================================================================================================
// QueryTuple - Event Query Specification
// ================================================================================================

/// A query specification for fetching events from the event store.
///
/// Consists of an event type and zero or more tags that identify the event stream to query.
/// This is the core crate's dependency-free equivalent of the TypeScript `QueryTuple` type
/// from fmodel-ts: `[...tags, eventType]`.
///
/// # Fields
///
/// - `event_type`: The event type identifier (e.g., `"RestaurantCreatedEvent"`)
/// - `tags`: Tags to filter by. Empty vec queries all events of the type. These are matched
///   against the tags an event exposes via [`EventMeta::tags`].
#[derive(Debug, Clone)]
pub struct QueryTuple {
    /// The event type identifier to query.
    pub event_type: String,
    /// Tags to filter by.
    pub tags: Vec<Tag>,
}

// ================================================================================================
// EventLoader Trait
// ================================================================================================

/// Read-only interface for loading events by query tuples.
///
/// Provides tuple-based event loading without the decide-persist cycle.
/// Useful for ad-hoc queries, debugging, or building on-the-fly projections
/// where a materialized read model is not needed.
///
/// Adapts the TypeScript `IEventLoader<Ei>` interface from fmodel-ts.
///
/// # Type Parameters
///
/// - `E`: Event type to load. Must implement [`EventMeta`], since `QueryTuple` queries by
///   the exact `event_type`/`tags` shape `EventMeta` produces.
#[cfg(not(feature = "single-threaded"))]
pub trait EventLoader<E>: Send + Sync
where
    E: EventMeta,
{
    /// Error type for load operations.
    type Error;

    /// Loads events matching the given query tuples, returned in chronological order.
    fn load(
        &self,
        query_tuples: &[QueryTuple],
    ) -> impl std::future::Future<Output = Result<Vec<E>, Self::Error>> + Send;
}

/// Read-only interface for loading events by query tuples (single-threaded variant).
///
/// See the multi-threaded `EventLoader` documentation for details.
#[cfg(feature = "single-threaded")]
pub trait EventLoader<E>
where
    E: EventMeta,
{
    /// Error type for load operations.
    type Error;

    /// Loads events matching the given query tuples, returned in chronological order.
    fn load(
        &self,
        query_tuples: &[QueryTuple],
    ) -> impl std::future::Future<Output = Result<Vec<E>, Self::Error>>;
}

// ================================================================================================
// EventSourcedQueryHandler - Convenience Layer
// ================================================================================================

/// Query handler for on-demand event-sourced projections (multi-threaded variant).
///
/// Loads events via query tuples and folds them through a `ViewTrait` projection
/// to compute state on the fly — without persisting the result. Useful for ad-hoc
/// queries where a materialized read model is not needed or not yet available.
///
/// Adapts the TypeScript `EventSourcedQueryHandler` class from fmodel-ts.
///
/// # Type Parameters
///
/// - `E`: Event type
/// - `S`: State type (projected result)
/// - `V`: View implementing `ViewTrait<S, S, E>`
/// - `L`: Event loader implementing `EventLoader<E>`
#[cfg(not(feature = "single-threaded"))]
pub struct EventSourcedQueryHandler<E, S, V, L>
where
    E: EventMeta,
    V: ViewTrait<S, S, E> + Send + Sync,
    L: EventLoader<E> + Send + Sync,
{
    view: std::sync::Arc<V>,
    event_loader: std::sync::Arc<L>,
    _phantom: std::marker::PhantomData<(E, S)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<E, S, V, L> EventSourcedQueryHandler<E, S, V, L>
where
    E: EventMeta,
    V: ViewTrait<S, S, E> + Send + Sync,
    L: EventLoader<E> + Send + Sync,
{
    /// Create a new event-sourced query handler.
    pub fn new(view: std::sync::Arc<V>, event_loader: std::sync::Arc<L>) -> Self {
        Self {
            view,
            event_loader,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a query by loading events and folding them through the view projection.
    ///
    /// Returns the projected state computed on-the-fly (not persisted).
    pub async fn handle(&self, query_tuples: &[QueryTuple]) -> Result<S, L::Error> {
        let events = self.event_loader.load(query_tuples).await?;
        let state = events
            .iter()
            .fold(self.view.initial_state(), |s, e| self.view.evolve(&s, e));
        Ok(state)
    }

    /// Handle a batch of independent queries.
    ///
    /// Each element of `queries` is an independent set of query tuples; this method runs
    /// [`handle`](Self::handle) for each and returns one projected state per query, in order.
    /// Queries are evaluated sequentially and the projected states are not persisted.
    ///
    /// # Parameters
    ///
    /// - `queries`: The list of query-tuple sets to evaluate
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<S>)`: One projected state per input query, in order
    /// - `Err(L::Error)`: Any error while loading events for a query
    pub async fn handle_batch(&self, queries: Vec<Vec<QueryTuple>>) -> Result<Vec<S>, L::Error> {
        let mut states = Vec::with_capacity(queries.len());
        for query_tuples in queries {
            states.push(self.handle(&query_tuples).await?);
        }
        Ok(states)
    }
}

/// Query handler for on-demand event-sourced projections (single-threaded variant).
///
/// See the multi-threaded `EventSourcedQueryHandler` documentation for details.
#[cfg(feature = "single-threaded")]
pub struct EventSourcedQueryHandler<E, S, V, L>
where
    E: EventMeta,
    V: ViewTrait<S, S, E>,
    L: EventLoader<E>,
{
    view: std::rc::Rc<V>,
    event_loader: std::rc::Rc<L>,
    _phantom: std::marker::PhantomData<(E, S)>,
}

#[cfg(feature = "single-threaded")]
impl<E, S, V, L> EventSourcedQueryHandler<E, S, V, L>
where
    E: EventMeta,
    V: ViewTrait<S, S, E>,
    L: EventLoader<E>,
{
    /// Create a new event-sourced query handler (single-threaded variant).
    pub fn new(view: std::rc::Rc<V>, event_loader: std::rc::Rc<L>) -> Self {
        Self {
            view,
            event_loader,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a query by loading events and folding them through the view projection.
    ///
    /// Returns the projected state computed on-the-fly (not persisted).
    pub async fn handle(&self, query_tuples: &[QueryTuple]) -> Result<S, L::Error> {
        let events = self.event_loader.load(query_tuples).await?;
        let state = events
            .iter()
            .fold(self.view.initial_state(), |s, e| self.view.evolve(&s, e));
        Ok(state)
    }

    /// Handle a batch of independent queries.
    ///
    /// Each element of `queries` is an independent set of query tuples; this method runs
    /// [`handle`](Self::handle) for each and returns one projected state per query, in order.
    /// Queries are evaluated sequentially and the projected states are not persisted.
    ///
    /// # Parameters
    ///
    /// - `queries`: The list of query-tuple sets to evaluate
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<S>)`: One projected state per input query, in order
    /// - `Err(L::Error)`: Any error while loading events for a query
    pub async fn handle_batch(&self, queries: Vec<Vec<QueryTuple>>) -> Result<Vec<S>, L::Error> {
        let mut states = Vec::with_capacity(queries.len());
        for query_tuples in queries {
            states.push(self.handle(&query_tuples).await?);
        }
        Ok(states)
    }
}

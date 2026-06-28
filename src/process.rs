use crate::{EventComputationTrait, StateComputationTrait};
use std::convert::Infallible;
use std::marker::PhantomData;

// ============================================================================
// Multi-threaded Process (default)
// ============================================================================

/// # `Process` — Reactive business logic (multi-threaded)
///
/// Enforces `Send + Sync` bounds on all behavioral components.
///
/// A `Process` represents reactive business logic:
///
/// - `decide` transforms an **action result** and current **state** into **events**
/// - `evolve` applies a single **event** to a **state**
/// - `initial_state` produces the initial state
/// - `react` generates **actions** in response to **events** (reactive behavior)
/// - `pending` generates **actions** based on current **state** (proactive behavior)
#[cfg(not(feature = "single-threaded"))]
pub struct Process<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError = Infallible,
> where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    /// Decision function: `(action_result, state) -> events`
    pub decide: DecideFn,
    /// Evolution function: `(state, event) -> new_state`
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    /// Reactive function: `(state, event) -> actions`
    pub react: ReactFn,
    /// Pending actions function: `(state) -> actions`
    pub pending: PendingFn,
    _marker: PhantomData<(AR, S, E, A)>,
}

#[cfg(not(feature = "single-threaded"))]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
>
    Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    /// Creates a new thread-safe `Process`.
    pub fn new(
        decide: DecideFn,
        evolve: EvolveFn,
        initial_state: InitFn,
        react: ReactFn,
        pending: PendingFn,
    ) -> Self {
        Self {
            decide,
            evolve,
            initial_state,
            react,
            pending,
            _marker: PhantomData,
        }
    }
}

// ============================================================================
// Single-threaded Process
// ============================================================================

/// # `Process` — Reactive business logic (single-threaded)
///
/// Does not impose `Send + Sync` bounds. Uses `Rc` instead of `Arc` for lower overhead.
#[cfg(feature = "single-threaded")]
pub struct Process<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError = Infallible,
> where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    /// Decision function
    pub decide: DecideFn,
    /// Evolution function
    pub evolve: EvolveFn,
    /// Initial state factory
    pub initial_state: InitFn,
    /// Reactive function
    pub react: ReactFn,
    /// Pending actions function
    pub pending: PendingFn,
    _marker: PhantomData<(AR, S, E, A)>,
}

#[cfg(feature = "single-threaded")]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
>
    Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    /// Creates a new single-threaded `Process`.
    pub fn new(
        decide: DecideFn,
        evolve: EvolveFn,
        initial_state: InitFn,
        react: ReactFn,
        pending: PendingFn,
    ) -> Self {
        Self {
            decide,
            evolve,
            initial_state,
            react,
            pending,
            _marker: PhantomData,
        }
    }
}

// ============================================================================
// ViewTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> crate::ViewTrait<S, S, E>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    fn evolve(&self, state: &S, event: &E) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

#[cfg(feature = "single-threaded")]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> crate::ViewTrait<S, S, E>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    fn evolve(&self, state: &S, event: &E) -> S {
        (self.evolve)(state, event)
    }

    fn initial_state(&self) -> S {
        (self.initial_state)()
    }
}

// ============================================================================
// DeciderTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> crate::DeciderTrait<AR, S, S, E, E>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn decide(&self, command: &AR, state: &S) -> Result<Self::Events, Self::Error> {
        (self.decide)(command, state)
    }
}

#[cfg(feature = "single-threaded")]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> crate::DeciderTrait<AR, S, S, E, E>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn decide(&self, command: &AR, state: &S) -> Result<Self::Events, Self::Error> {
        (self.decide)(command, state)
    }
}

// ============================================================================
// ProcessTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> crate::ProcessTrait<AR, S, S, E, E, A>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    type Actions = ResultActions;

    fn react(&self, state: &S, event: &E) -> Self::Actions {
        (self.react)(state, event)
    }

    fn pending(&self, state: &S) -> Self::Actions {
        (self.pending)(state)
    }
}

#[cfg(feature = "single-threaded")]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> crate::ProcessTrait<AR, S, S, E, E, A>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    type Actions = ResultActions;

    fn react(&self, state: &S, event: &E) -> Self::Actions {
        (self.react)(state, event)
    }

    fn pending(&self, state: &S) -> Self::Actions {
        (self.pending)(state)
    }
}

// ============================================================================
// EventComputationTrait and StateComputationTrait Implementations
// ============================================================================

#[cfg(not(feature = "single-threaded"))]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> EventComputationTrait<AR, E, E>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn compute_new_events(
        &self,
        current_events: &[E],
        command: &AR,
    ) -> Result<Self::Events, Self::Error> {
        let state = fold_events((self.initial_state)(), current_events, &self.evolve);
        (self.decide)(command, &state)
    }
}

#[cfg(not(feature = "single-threaded"))]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> StateComputationTrait<AR, S>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError> + Send + Sync,
    EvolveFn: Fn(&S, &E) -> S + Send + Sync,
    InitFn: Fn() -> S + Send + Sync,
    ReactFn: Fn(&S, &E) -> ResultActions + Send + Sync,
    PendingFn: Fn(&S) -> ResultActions + Send + Sync,
    ResultEvents: IntoIterator<Item = E> + Send + Sync,
    ResultActions: IntoIterator<Item = A> + Send + Sync,
{
    type Error = ResultError;

    fn compute_new_state(&self, current_state: Option<S>, command: &AR) -> Result<S, Self::Error> {
        let base_state = current_state.unwrap_or_else(|| (self.initial_state)());
        let events = (self.decide)(command, &base_state)?;
        Ok(events
            .into_iter()
            .fold(base_state, |state, event| (self.evolve)(&state, &event)))
    }
}

#[cfg(feature = "single-threaded")]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> EventComputationTrait<AR, E, E>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    type Events = ResultEvents;
    type Error = ResultError;

    fn compute_new_events(
        &self,
        current_events: &[E],
        command: &AR,
    ) -> Result<Self::Events, Self::Error> {
        let state = fold_events((self.initial_state)(), current_events, &self.evolve);
        (self.decide)(command, &state)
    }
}

#[cfg(feature = "single-threaded")]
impl<
    AR,
    S,
    E,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
> StateComputationTrait<AR, S>
    for Process<
        AR,
        S,
        E,
        A,
        DecideFn,
        EvolveFn,
        InitFn,
        ReactFn,
        PendingFn,
        ResultEvents,
        ResultActions,
        ResultError,
    >
where
    DecideFn: Fn(&AR, &S) -> Result<ResultEvents, ResultError>,
    EvolveFn: Fn(&S, &E) -> S,
    InitFn: Fn() -> S,
    ReactFn: Fn(&S, &E) -> ResultActions,
    PendingFn: Fn(&S) -> ResultActions,
    ResultEvents: IntoIterator<Item = E>,
    ResultActions: IntoIterator<Item = A>,
{
    type Error = ResultError;

    fn compute_new_state(&self, current_state: Option<S>, command: &AR) -> Result<S, Self::Error> {
        let base_state = current_state.unwrap_or_else(|| (self.initial_state)());
        let events = (self.decide)(command, &base_state)?;
        Ok(events
            .into_iter()
            .fold(base_state, |state, event| (self.evolve)(&state, &event)))
    }
}

fn fold_events<S, E, F>(mut state: S, events: &[E], evolve: &F) -> S
where
    F: Fn(&S, &E) -> S,
{
    for event in events {
        state = evolve(&state, event);
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeciderTrait, ProcessTrait, ViewTrait};
    use std::convert::Infallible;
    use std::iter::{Once, once};

    // Order Management ToDo List Example
    #[derive(Debug, PartialEq, Clone)]
    struct OrderActionResult {
        action: String,
        success: bool,
        order_id: u32,
    }

    #[derive(Debug, PartialEq)]
    enum OrderEvent {
        OrderCreated { order_id: u32 },
        PaymentProcessed { order_id: u32, amount: f64 },
        PaymentFailed { order_id: u32, reason: String },
        OrderShipped { order_id: u32, tracking: String },
    }

    #[derive(Debug, PartialEq, Clone)]
    enum OrderAction {
        ProcessPayment { order_id: u32, amount: f64 },
        SendConfirmationEmail { order_id: u32, email: String },
        UpdateInventory { order_id: u32, items: Vec<String> },
        ShipOrder { order_id: u32, address: String },
        SendTrackingInfo { order_id: u32, tracking: String },
        RefundPayment { order_id: u32, amount: f64 },
        NotifyCustomerService { order_id: u32, issue: String },
        RetryPayment { order_id: u32 },
    }

    #[derive(Debug, PartialEq, Clone)]
    enum OrderState {
        Created {
            order_id: u32,
            amount: f64,
            email: String,
            items: Vec<String>,
        },
        PaymentPending {
            order_id: u32,
            amount: f64,
            email: String,
            items: Vec<String>,
        },
        Paid {
            order_id: u32,
            email: String,
            items: Vec<String>,
            address: String,
        },
        Shipped {
            order_id: u32,
            tracking: String,
        },
        Failed {
            order_id: u32,
            reason: String,
        },
    }

    fn make_order_process() -> Process<
        OrderActionResult,
        OrderState,
        OrderEvent,
        OrderAction,
        impl Fn(&OrderActionResult, &OrderState) -> Result<Once<OrderEvent>, Infallible>,
        impl Fn(&OrderState, &OrderEvent) -> OrderState,
        impl Fn() -> OrderState,
        impl Fn(&OrderState, &OrderEvent) -> Vec<OrderAction>,
        impl Fn(&OrderState) -> Vec<OrderAction>,
        Once<OrderEvent>,
        Vec<OrderAction>,
    > {
        Process::new(
            |action_result: &OrderActionResult,
             state: &OrderState|
             -> Result<Once<OrderEvent>, Infallible> {
                match (action_result.action.as_str(), action_result.success, state) {
                    ("create_order", true, _) => Ok(once(OrderEvent::OrderCreated {
                        order_id: action_result.order_id,
                    })),
                    ("process_payment", true, OrderState::PaymentPending { order_id, .. }) => {
                        Ok(once(OrderEvent::PaymentProcessed {
                            order_id: *order_id,
                            amount: 99.99,
                        }))
                    }
                    ("process_payment", false, OrderState::PaymentPending { order_id, .. }) => {
                        Ok(once(OrderEvent::PaymentFailed {
                            order_id: *order_id,
                            reason: "Card declined".to_string(),
                        }))
                    }
                    ("ship_order", true, OrderState::Paid { order_id, .. }) => {
                        Ok(once(OrderEvent::OrderShipped {
                            order_id: *order_id,
                            tracking: "TRK123456".to_string(),
                        }))
                    }
                    _ => Ok(once(OrderEvent::OrderCreated { order_id: 0 })),
                }
            },
            |state: &OrderState, event: &OrderEvent| match (state, event) {
                (_, OrderEvent::OrderCreated { order_id }) => OrderState::PaymentPending {
                    order_id: *order_id,
                    amount: 99.99,
                    email: "customer@example.com".to_string(),
                    items: vec!["Widget".to_string()],
                },
                (
                    OrderState::PaymentPending {
                        order_id,
                        email,
                        items,
                        ..
                    },
                    OrderEvent::PaymentProcessed { .. },
                ) => OrderState::Paid {
                    order_id: *order_id,
                    email: email.clone(),
                    items: items.clone(),
                    address: "123 Main St".to_string(),
                },
                (
                    OrderState::PaymentPending { order_id, .. },
                    OrderEvent::PaymentFailed { reason, .. },
                ) => OrderState::Failed {
                    order_id: *order_id,
                    reason: reason.clone(),
                },
                (OrderState::Paid { order_id, .. }, OrderEvent::OrderShipped { tracking, .. }) => {
                    OrderState::Shipped {
                        order_id: *order_id,
                        tracking: tracking.clone(),
                    }
                }
                _ => state.clone(),
            },
            || OrderState::Created {
                order_id: 1,
                amount: 99.99,
                email: "customer@example.com".to_string(),
                items: vec!["Widget".to_string()],
            },
            |state: &OrderState, event: &OrderEvent| -> Vec<OrderAction> {
                match (state, event) {
                    (
                        OrderState::Paid {
                            order_id, email, ..
                        },
                        OrderEvent::PaymentProcessed { .. },
                    ) => {
                        vec![OrderAction::SendConfirmationEmail {
                            order_id: *order_id,
                            email: email.clone(),
                        }]
                    }
                    (_, OrderEvent::PaymentFailed { order_id, .. }) => {
                        vec![
                            OrderAction::RetryPayment {
                                order_id: *order_id,
                            },
                            OrderAction::NotifyCustomerService {
                                order_id: *order_id,
                                issue: "Payment failed".to_string(),
                            },
                        ]
                    }
                    (
                        OrderState::Shipped {
                            order_id, tracking, ..
                        },
                        OrderEvent::OrderShipped { .. },
                    ) => {
                        vec![OrderAction::SendTrackingInfo {
                            order_id: *order_id,
                            tracking: tracking.clone(),
                        }]
                    }
                    _ => vec![],
                }
            },
            |state: &OrderState| -> Vec<OrderAction> {
                match state {
                    OrderState::Created {
                        order_id,
                        amount,
                        email,
                        items,
                    } => {
                        vec![
                            OrderAction::ProcessPayment {
                                order_id: *order_id,
                                amount: *amount,
                            },
                            OrderAction::UpdateInventory {
                                order_id: *order_id,
                                items: items.clone(),
                            },
                            OrderAction::SendConfirmationEmail {
                                order_id: *order_id,
                                email: email.clone(),
                            },
                        ]
                    }
                    OrderState::PaymentPending {
                        order_id, amount, ..
                    } => {
                        vec![
                            OrderAction::ProcessPayment {
                                order_id: *order_id,
                                amount: *amount,
                            },
                            OrderAction::RetryPayment {
                                order_id: *order_id,
                            },
                        ]
                    }
                    OrderState::Paid {
                        order_id,
                        email,
                        items,
                        address,
                    } => {
                        vec![
                            OrderAction::SendConfirmationEmail {
                                order_id: *order_id,
                                email: email.clone(),
                            },
                            OrderAction::UpdateInventory {
                                order_id: *order_id,
                                items: items.clone(),
                            },
                            OrderAction::ShipOrder {
                                order_id: *order_id,
                                address: address.clone(),
                            },
                        ]
                    }
                    OrderState::Shipped { order_id, tracking } => {
                        vec![OrderAction::SendTrackingInfo {
                            order_id: *order_id,
                            tracking: tracking.clone(),
                        }]
                    }
                    OrderState::Failed { order_id, .. } => {
                        vec![
                            OrderAction::RetryPayment {
                                order_id: *order_id,
                            },
                            OrderAction::NotifyCustomerService {
                                order_id: *order_id,
                                issue: "Payment failed".to_string(),
                            },
                            OrderAction::RefundPayment {
                                order_id: *order_id,
                                amount: 99.99,
                            },
                        ]
                    }
                }
            },
        )
    }

    #[test]
    fn todo_list_complete_vs_filtered_actions() {
        let process = make_order_process();

        let created_state = process.initial_state();

        let all_created_actions = process.pending(&created_state);
        assert_eq!(all_created_actions.len(), 3);

        let order_created_event = OrderEvent::OrderCreated { order_id: 1 };
        let payment_pending_state = process.evolve(&created_state, &order_created_event);

        let all_pending_actions = process.pending(&payment_pending_state);
        assert_eq!(all_pending_actions.len(), 2);
        assert!(all_pending_actions.contains(&OrderAction::ProcessPayment {
            order_id: 1,
            amount: 99.99
        }));
        assert!(all_pending_actions.contains(&OrderAction::RetryPayment { order_id: 1 }));

        let event_specific_actions = process.react(&payment_pending_state, &order_created_event);
        assert!(event_specific_actions.is_empty());
    }

    #[test]
    fn todo_list_payment_success_workflow() {
        let process = make_order_process();

        let payment_result = OrderActionResult {
            action: "process_payment".to_string(),
            success: true,
            order_id: 1,
        };

        let payment_pending_state = OrderState::PaymentPending {
            order_id: 1,
            amount: 99.99,
            email: "customer@example.com".to_string(),
            items: vec!["Widget".to_string()],
        };

        let events: Vec<_> = process
            .decide(&payment_result, &payment_pending_state)
            .unwrap()
            .into_iter()
            .collect();

        let paid_state = process.evolve(&payment_pending_state, &events[0]);

        let all_paid_actions = process.pending(&paid_state);
        assert_eq!(all_paid_actions.len(), 3);

        let event_specific_actions = process.react(&paid_state, &events[0]);
        assert_eq!(event_specific_actions.len(), 1);
        assert_eq!(
            event_specific_actions[0],
            OrderAction::SendConfirmationEmail {
                order_id: 1,
                email: "customer@example.com".to_string()
            }
        );

        assert!(
            event_specific_actions
                .iter()
                .all(|action| all_paid_actions.contains(action))
        );
    }

    #[test]
    fn todo_list_payment_failure_workflow() {
        let process = make_order_process();

        let payment_pending_state = OrderState::PaymentPending {
            order_id: 1,
            amount: 99.99,
            email: "customer@example.com".to_string(),
            items: vec!["Widget".to_string()],
        };

        let payment_result = OrderActionResult {
            action: "process_payment".to_string(),
            success: false,
            order_id: 1,
        };

        let events: Vec<_> = process
            .decide(&payment_result, &payment_pending_state)
            .unwrap()
            .into_iter()
            .collect();

        let failed_state = process.evolve(&payment_pending_state, &events[0]);

        let all_failed_actions = process.pending(&failed_state);
        assert_eq!(all_failed_actions.len(), 3);

        let event_specific_actions = process.react(&failed_state, &events[0]);
        assert_eq!(event_specific_actions.len(), 2);

        assert!(
            event_specific_actions
                .iter()
                .all(|action| all_failed_actions.contains(action))
        );

        assert!(event_specific_actions.contains(&OrderAction::RetryPayment { order_id: 1 }));
        assert!(
            event_specific_actions.contains(&OrderAction::NotifyCustomerService {
                order_id: 1,
                issue: "Payment failed".to_string()
            })
        );
    }

    #[test]
    fn todo_list_trait_usage() {
        let process = make_order_process();

        fn demonstrate_todo_list<P>(
            p: &P,
            state: &OrderState,
            event: &OrderEvent,
        ) -> (Vec<OrderAction>, Vec<OrderAction>)
        where
            P: ProcessTrait<
                    OrderActionResult,
                    OrderState,
                    OrderState,
                    OrderEvent,
                    OrderEvent,
                    OrderAction,
                >,
        {
            let complete_todo_list: Vec<_> = p.pending(state).into_iter().collect();
            let filtered_todo_list: Vec<_> = p.react(state, event).into_iter().collect();
            (complete_todo_list, filtered_todo_list)
        }

        let paid_state = OrderState::Paid {
            order_id: 1,
            email: "test@example.com".to_string(),
            items: vec!["Widget".to_string()],
            address: "123 Main St".to_string(),
        };
        let payment_event = OrderEvent::PaymentProcessed {
            order_id: 1,
            amount: 99.99,
        };

        let (complete_list, filtered_list) =
            demonstrate_todo_list(&process, &paid_state, &payment_event);

        assert_eq!(complete_list.len(), 3);
        assert_eq!(filtered_list.len(), 1);
        assert!(
            filtered_list
                .iter()
                .all(|action| complete_list.contains(action))
        );
    }

    #[test]
    fn todo_list_state_evolution_changes_available_actions() {
        let process = make_order_process();

        let mut current_state = process.initial_state();
        let mut all_todo_lists = Vec::new();

        all_todo_lists.push((
            format!("{:?}", current_state),
            process.pending(&current_state),
        ));

        let order_created_event = OrderEvent::OrderCreated { order_id: 1 };
        current_state = process.evolve(&current_state, &order_created_event);
        all_todo_lists.push((
            format!("{:?}", current_state),
            process.pending(&current_state),
        ));

        let payment_event = OrderEvent::PaymentProcessed {
            order_id: 1,
            amount: 99.99,
        };
        current_state = process.evolve(&current_state, &payment_event);
        all_todo_lists.push((
            format!("{:?}", current_state),
            process.pending(&current_state),
        ));

        let shipping_event = OrderEvent::OrderShipped {
            order_id: 1,
            tracking: "TRK123".to_string(),
        };
        current_state = process.evolve(&current_state, &shipping_event);
        all_todo_lists.push((
            format!("{:?}", current_state),
            process.pending(&current_state),
        ));

        assert_eq!(all_todo_lists.len(), 4);
        assert_eq!(all_todo_lists[0].1.len(), 3);
        assert_eq!(all_todo_lists[1].1.len(), 2);
        assert_eq!(all_todo_lists[2].1.len(), 3);
        assert_eq!(all_todo_lists[3].1.len(), 1);

        assert!(
            all_todo_lists[0]
                .1
                .iter()
                .any(|a| matches!(a, OrderAction::ProcessPayment { .. }))
        );
        assert!(
            all_todo_lists[1]
                .1
                .iter()
                .any(|a| matches!(a, OrderAction::ProcessPayment { .. }))
        );
        assert!(
            all_todo_lists[1]
                .1
                .iter()
                .any(|a| matches!(a, OrderAction::RetryPayment { .. }))
        );
        assert!(
            all_todo_lists[2]
                .1
                .iter()
                .any(|a| matches!(a, OrderAction::ShipOrder { .. }))
        );
        assert!(
            all_todo_lists[3]
                .1
                .iter()
                .any(|a| matches!(a, OrderAction::SendTrackingInfo { .. }))
        );
    }
}

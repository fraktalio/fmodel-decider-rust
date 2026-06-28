use std::collections::HashMap;
use std::convert::Infallible;

#[cfg(not(feature = "single-threaded"))]
use std::sync::Arc;

#[cfg(feature = "single-threaded")]
use std::rc::Rc;

/// Task status enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Task has been started
    Started,
    /// Task has finished (successfully)
    Finished,
}

/// Workflow state that tracks task execution status.
///
/// This simplified state structure matches the TypeScript implementation:
/// ```typescript
/// export type TaskState<Task extends string = string> = {
///   readonly [K in Task]?: TaskStatus;
/// };
///
/// export interface WorkflowState<Task extends string = string> {
///   readonly tasks: TaskState<Task>;
/// }
/// ```
///
/// Maps task names to their current status for consistent state management.
/// Provides O(1) lookup for task status queries in workflow processes.
///
/// ## Type Parameters
///
/// - `Task` — The type used to identify tasks
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowState<Task: std::hash::Hash + Eq> {
    /// Task status mapping - maps task names to their current status
    pub tasks: HashMap<Task, TaskStatus>,
    /// Workflow identifier for tracking
    pub workflow_id: String,
}

/// Workflow events that represent state changes in task execution.
///
/// These events capture the essential task lifecycle within a workflow,
/// allowing the workflow state to evolve based on task outcomes.
///
/// ## Type Parameters
///
/// - `Task` — The type used to identify tasks
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowEvent<Task> {
    /// A task has started execution
    TaskStarted {
        /// The task that started
        task: Task,
        /// The workflow this task belongs to
        workflow_id: String,
    },
    /// A task has completed successfully
    TaskCompleted {
        /// The task that completed
        task: Task,
        /// The workflow this task belongs to
        workflow_id: String,
        /// The result/output of the task
        result: String,
    },
}

/// # Public `Workflow` Type Alias
///
/// A `Process` specialized with `WorkflowState<Task>` and `WorkflowEvent<Task>`,
/// demonstrating **progressive type refinement** by fixing the state and event types
/// to workflow-specific structures.
///
/// Inherits the threading model from `Process`: multi-threaded by default,
/// single-threaded when the `single-threaded` feature is enabled.
pub type Workflow<
    AR,
    A,
    Task,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError = Infallible,
> = crate::process::Process<
    AR,
    WorkflowState<Task>,
    WorkflowEvent<Task>,
    A,
    DecideFn,
    EvolveFn,
    InitFn,
    ReactFn,
    PendingFn,
    ResultEvents,
    ResultActions,
    ResultError,
>;

impl<Task> WorkflowState<Task>
where
    Task: Clone + PartialEq + std::hash::Hash + Eq,
{
    /// Creates a new workflow state with the given workflow ID.
    pub fn new(workflow_id: String) -> Self {
        Self {
            tasks: HashMap::new(),
            workflow_id,
        }
    }

    /// Creates a new workflow state with initial tasks (all start as not started).
    pub fn with_tasks(workflow_id: String, _initial_tasks: Vec<Task>) -> Self {
        let tasks = HashMap::new();
        // In the simplified model, tasks that haven't started yet are simply not in the map
        Self { tasks, workflow_id }
    }

    /// Gets the status of a specific task.
    pub fn task_status(&self, task: &Task) -> Option<&TaskStatus> {
        self.tasks.get(task)
    }

    /// Checks if a task has started (is in the tasks map).
    pub fn is_task_started(&self, task: &Task) -> bool {
        self.tasks.contains_key(task)
    }

    /// Checks if a task has finished.
    pub fn is_task_finished(&self, task: &Task) -> bool {
        matches!(self.tasks.get(task), Some(TaskStatus::Finished))
    }

    /// Gets all tasks that have started.
    pub fn started_tasks(&self) -> Vec<&Task> {
        self.tasks.keys().collect()
    }

    /// Gets all tasks that have finished.
    pub fn finished_tasks(&self) -> Vec<&Task> {
        self.tasks
            .iter()
            .filter_map(|(task, status)| {
                if matches!(status, TaskStatus::Finished) {
                    Some(task)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Gets all tasks that are currently running (started but not finished).
    pub fn running_tasks(&self) -> Vec<&Task> {
        self.tasks
            .iter()
            .filter_map(|(task, status)| {
                if matches!(status, TaskStatus::Started) {
                    Some(task)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Starts a task (sets its status to Started).
    pub fn start_task(&mut self, task: Task) {
        self.tasks.insert(task, TaskStatus::Started);
    }

    /// Completes a task (sets its status to Finished).
    pub fn complete_task(&mut self, task: Task) {
        self.tasks.insert(task, TaskStatus::Finished);
    }
}

/// Helper function to create a workflow process with common workflow patterns.
///
/// This function demonstrates how to create a `Workflow` with
/// typical workflow behavior patterns for task orchestration.
///
/// ## Type Parameters
///
/// - `AR` — Action Result type
/// - `A` — Action type  
/// - `Task` — Task identifier type (must be Clone + PartialEq + std::fmt::Debug + Send + Sync + Hash + Eq)
///
/// ## Returns
///
/// A `Workflow` configured with standard workflow behavior:
/// - **decide**: Processes action results to generate workflow events (TaskStarted/TaskCompleted only)
/// - **evolve**: Updates workflow state based on events
/// - **initial_state**: Creates initial workflow state
/// - **react**: Generates actions in response to specific events
/// - **pending**: Returns all available actions for current state
#[cfg(not(feature = "single-threaded"))]
#[allow(dead_code)]
pub fn create_workflow_process<AR, A, Task>(
    workflow_id: String,
    initial_tasks: Vec<Task>,
    decide_fn: impl Fn(&AR, &WorkflowState<Task>) -> Result<Vec<WorkflowEvent<Task>>, String>
    + Send
    + Sync
    + 'static,
    action_generator: impl Fn(&WorkflowState<Task>, Option<&WorkflowEvent<Task>>) -> Vec<A>
    + Send
    + Sync
    + 'static,
) -> Workflow<
    AR,
    A,
    Task,
    impl Fn(&AR, &WorkflowState<Task>) -> Result<Vec<WorkflowEvent<Task>>, String> + Send + Sync,
    impl Fn(&WorkflowState<Task>, &WorkflowEvent<Task>) -> WorkflowState<Task> + Send + Sync,
    impl Fn() -> WorkflowState<Task> + Send + Sync,
    impl Fn(&WorkflowState<Task>, &WorkflowEvent<Task>) -> Vec<A> + Send + Sync,
    impl Fn(&WorkflowState<Task>) -> Vec<A> + Send + Sync,
    Vec<WorkflowEvent<Task>>,
    Vec<A>,
    String,
>
where
    Task: Clone + PartialEq + std::fmt::Debug + Send + Sync + std::hash::Hash + Eq + 'static,
    AR: Clone + Send + Sync + 'static,
    A: Clone + Send + Sync + 'static,
{
    let workflow_id_clone = workflow_id.clone();
    let initial_tasks_clone = initial_tasks.clone();
    let action_generator = Arc::new(action_generator);
    let action_generator_clone = Arc::clone(&action_generator);

    Workflow::new(
        // decide: action_result + state -> events
        decide_fn,
        // evolve: state + event -> new_state (simplified to handle only 2 event types)
        move |state: &WorkflowState<Task>, event: &WorkflowEvent<Task>| {
            let mut new_state = state.clone();

            match event {
                WorkflowEvent::TaskStarted { task, .. } => {
                    // Set task status to Started
                    new_state.start_task(task.clone());
                }
                WorkflowEvent::TaskCompleted { task, .. } => {
                    // Set task status to Finished
                    new_state.complete_task(task.clone());
                }
            }

            new_state
        },
        // initial_state: Create initial workflow state
        move || WorkflowState::with_tasks(workflow_id_clone.clone(), initial_tasks_clone.clone()),
        // react: state + event -> actions (filtered ToDo list based on event)
        move |state: &WorkflowState<Task>, event: &WorkflowEvent<Task>| {
            action_generator_clone(state, Some(event))
        },
        // pending: state -> actions (complete ToDo list for current state)
        move |state: &WorkflowState<Task>| action_generator(state, None),
    )
}

/// Helper function to create a workflow process with common workflow patterns (single-threaded).
#[cfg(feature = "single-threaded")]
#[allow(dead_code)]
pub fn create_workflow_process<AR, A, Task>(
    workflow_id: String,
    initial_tasks: Vec<Task>,
    decide_fn: impl Fn(&AR, &WorkflowState<Task>) -> Result<Vec<WorkflowEvent<Task>>, String> + 'static,
    action_generator: impl Fn(&WorkflowState<Task>, Option<&WorkflowEvent<Task>>) -> Vec<A> + 'static,
) -> Workflow<
    AR,
    A,
    Task,
    impl Fn(&AR, &WorkflowState<Task>) -> Result<Vec<WorkflowEvent<Task>>, String>,
    impl Fn(&WorkflowState<Task>, &WorkflowEvent<Task>) -> WorkflowState<Task>,
    impl Fn() -> WorkflowState<Task>,
    impl Fn(&WorkflowState<Task>, &WorkflowEvent<Task>) -> Vec<A>,
    impl Fn(&WorkflowState<Task>) -> Vec<A>,
    Vec<WorkflowEvent<Task>>,
    Vec<A>,
    String,
>
where
    Task: Clone + PartialEq + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    AR: Clone + 'static,
    A: Clone + 'static,
{
    let workflow_id_clone = workflow_id.clone();
    let initial_tasks_clone = initial_tasks.clone();
    let action_generator = Rc::new(action_generator);
    let action_generator_clone = Rc::clone(&action_generator);

    Workflow::new(
        // decide: action_result + state -> events
        decide_fn,
        // evolve: state + event -> new_state (simplified to handle only 2 event types)
        move |state: &WorkflowState<Task>, event: &WorkflowEvent<Task>| {
            let mut new_state = state.clone();

            match event {
                WorkflowEvent::TaskStarted { task, .. } => {
                    // Set task status to Started
                    new_state.start_task(task.clone());
                }
                WorkflowEvent::TaskCompleted { task, .. } => {
                    // Set task status to Finished
                    new_state.complete_task(task.clone());
                }
            }

            new_state
        },
        // initial_state: Create initial workflow state
        move || WorkflowState::with_tasks(workflow_id_clone.clone(), initial_tasks_clone.clone()),
        // react: state + event -> actions (filtered ToDo list based on event)
        move |state: &WorkflowState<Task>, event: &WorkflowEvent<Task>| {
            action_generator_clone(state, Some(event))
        },
        // pending: state -> actions (complete ToDo list for current state)
        move |state: &WorkflowState<Task>| action_generator(state, None),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeciderTrait, ProcessTrait, ViewTrait};

    #[test]
    fn restaurant_order_workflow_complete_flow() {
        // Restaurant order task types
        #[derive(Debug, Clone, PartialEq, Hash, Eq)]
        enum RestaurantTask {
            ValidateOrder,
            CheckInventory,
            ProcessPayment,
            PrepareFood,
            QualityCheck,
            PackageOrder,
            AssignDelivery,
            DeliverOrder,
            ConfirmDelivery,
        }

        // Restaurant action results
        #[derive(Debug, Clone, PartialEq)]
        struct RestaurantActionResult {
            task: RestaurantTask,
            success: bool,
            details: String,
            order_id: String,
        }

        // Restaurant workflow actions
        #[derive(Debug, Clone, PartialEq)]
        enum RestaurantAction {
            StartTask(RestaurantTask),
            NotifyKitchen(String),
            NotifyCustomer(String),
            UpdateInventory(String),
        }

        let restaurant_workflow = create_workflow_process(
            "restaurant-order-wf-001".to_string(),
            vec![
                RestaurantTask::ValidateOrder,
                RestaurantTask::CheckInventory,
                RestaurantTask::ProcessPayment,
                RestaurantTask::PrepareFood,
                RestaurantTask::QualityCheck,
                RestaurantTask::PackageOrder,
                RestaurantTask::AssignDelivery,
                RestaurantTask::DeliverOrder,
                RestaurantTask::ConfirmDelivery,
            ],
            // Decision function: how action results generate events (simplified to 2 event types)
            |action_result: &RestaurantActionResult, _state: &WorkflowState<RestaurantTask>| {
                let workflow_id = "restaurant-order-wf-001".to_string();

                if action_result.success {
                    match action_result.task {
                        RestaurantTask::ValidateOrder => Ok(vec![
                            WorkflowEvent::TaskStarted {
                                task: RestaurantTask::ValidateOrder,
                                workflow_id: workflow_id.clone(),
                            },
                            WorkflowEvent::TaskCompleted {
                                task: RestaurantTask::ValidateOrder,
                                workflow_id,
                                result: format!(
                                    "Order {} validated: {}",
                                    action_result.order_id, action_result.details
                                ),
                            },
                        ]),
                        _ => Ok(vec![
                            WorkflowEvent::TaskStarted {
                                task: action_result.task.clone(),
                                workflow_id: workflow_id.clone(),
                            },
                            WorkflowEvent::TaskCompleted {
                                task: action_result.task.clone(),
                                workflow_id,
                                result: format!(
                                    "Task {:?} completed for order {}: {}",
                                    action_result.task,
                                    action_result.order_id,
                                    action_result.details
                                ),
                            },
                        ]),
                    }
                } else {
                    // For failures, we need to handle them in the state directly
                    // Since we only have TaskStarted and TaskCompleted events,
                    // we'll use the action generator to handle failure logic
                    Ok(vec![]) // No events for failures - handle in action generator
                }
            },
            // Action generator: what actions are available based on state and events
            |state: &WorkflowState<RestaurantTask>,
             event: Option<&WorkflowEvent<RestaurantTask>>| {
                let mut actions = Vec::new();

                match event {
                    Some(WorkflowEvent::TaskCompleted { task, .. }) => {
                        // Event-specific actions: what to do when specific tasks complete
                        match task {
                            RestaurantTask::ValidateOrder => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::CheckInventory,
                                ));
                                actions.push(RestaurantAction::NotifyKitchen(
                                    "New order validated".to_string(),
                                ));
                            }
                            RestaurantTask::CheckInventory => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::ProcessPayment,
                                ));
                                actions.push(RestaurantAction::UpdateInventory(
                                    "Reserve ingredients".to_string(),
                                ));
                            }
                            RestaurantTask::ProcessPayment => {
                                actions
                                    .push(RestaurantAction::StartTask(RestaurantTask::PrepareFood));
                                actions.push(RestaurantAction::NotifyKitchen(
                                    "Payment confirmed, start cooking".to_string(),
                                ));
                            }
                            RestaurantTask::PrepareFood => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::QualityCheck,
                                ));
                            }
                            RestaurantTask::QualityCheck => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::PackageOrder,
                                ));
                            }
                            RestaurantTask::PackageOrder => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::AssignDelivery,
                                ));
                            }
                            RestaurantTask::AssignDelivery => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::DeliverOrder,
                                ));
                                actions.push(RestaurantAction::NotifyCustomer(
                                    "Driver assigned, order on the way".to_string(),
                                ));
                            }
                            RestaurantTask::DeliverOrder => {
                                actions.push(RestaurantAction::StartTask(
                                    RestaurantTask::ConfirmDelivery,
                                ));
                            }
                            RestaurantTask::ConfirmDelivery => {
                                actions.push(RestaurantAction::NotifyCustomer(
                                    "Order delivered successfully".to_string(),
                                ));
                            }
                        }
                    }
                    Some(WorkflowEvent::TaskStarted { .. }) => {
                        // Task started events don't typically trigger immediate actions
                        // Actions are usually triggered by completion
                    }
                    None => {
                        // Complete ToDo list: all actions available in current state
                        // In the simplified model, we need to track which tasks are available to start
                        // This would typically be managed by the application logic

                        // For demonstration, let's assume we can start ValidateOrder if nothing has started
                        if state.tasks.is_empty() {
                            actions
                                .push(RestaurantAction::StartTask(RestaurantTask::ValidateOrder));
                        }

                        // Add general management actions based on current state
                        if !state.running_tasks().is_empty() {
                            actions.push(RestaurantAction::NotifyKitchen(
                                "Tasks in progress".to_string(),
                            ));
                        }
                    }
                }

                actions
            },
        );

        // Test initial state
        let mut current_state = restaurant_workflow.initial_state();
        assert_eq!(current_state.workflow_id, "restaurant-order-wf-001");
        assert!(current_state.tasks.is_empty()); // No tasks started yet in simplified model

        // Test complete ToDo list for initial state
        let all_initial_actions = restaurant_workflow.pending(&current_state);
        assert!(
            all_initial_actions
                .contains(&RestaurantAction::StartTask(RestaurantTask::ValidateOrder))
        );

        // Simulate successful order validation
        let validate_result = RestaurantActionResult {
            task: RestaurantTask::ValidateOrder,
            success: true,
            details: "Order items valid, customer verified".to_string(),
            order_id: "ORD-001".to_string(),
        };

        let validation_events: Vec<_> = restaurant_workflow
            .decide(&validate_result, &current_state)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(validation_events.len(), 2); // TaskStarted + TaskCompleted

        // Apply events to evolve state
        for event in &validation_events {
            current_state = restaurant_workflow.evolve(&current_state, event);
        }

        // Verify task is now finished in the simplified state
        assert!(current_state.is_task_finished(&RestaurantTask::ValidateOrder));
        assert_eq!(
            current_state.task_status(&RestaurantTask::ValidateOrder),
            Some(&TaskStatus::Finished)
        );

        // Test event-specific actions for validation completion
        let validation_completed_event = &validation_events[1]; // TaskCompleted event
        let validation_actions =
            restaurant_workflow.react(&current_state, validation_completed_event);

        assert!(
            validation_actions
                .contains(&RestaurantAction::StartTask(RestaurantTask::CheckInventory))
        );
        assert!(
            validation_actions
                .iter()
                .any(|a| matches!(a, RestaurantAction::NotifyKitchen(_)))
        );

        // Simulate inventory check success
        let inventory_result = RestaurantActionResult {
            task: RestaurantTask::CheckInventory,
            success: true,
            details: "Ingredients available".to_string(),
            order_id: "ORD-001".to_string(),
        };

        let inventory_events: Vec<_> = restaurant_workflow
            .decide(&inventory_result, &current_state)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(inventory_events.len(), 2); // TaskStarted + TaskCompleted

        // Apply inventory events
        for event in &inventory_events {
            current_state = restaurant_workflow.evolve(&current_state, event);
        }

        // Verify both tasks are now finished
        assert!(current_state.is_task_finished(&RestaurantTask::ValidateOrder));
        assert!(current_state.is_task_finished(&RestaurantTask::CheckInventory));

        // Verify the simplified state structure
        assert_eq!(current_state.finished_tasks().len(), 2);
        assert!(
            current_state
                .finished_tasks()
                .contains(&&RestaurantTask::ValidateOrder)
        );
        assert!(
            current_state
                .finished_tasks()
                .contains(&&RestaurantTask::CheckInventory)
        );

        println!(
            "Simplified workflow state successfully tracks task completion with HashMap structure"
        );
    }
}

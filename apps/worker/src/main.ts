/**
 * flux worker — Temporal process.
 *
 * At startup: ManagedRuntime.make(AppLayer) ONCE, shared by all
 * activities; clean runtime shutdown in the worker lifecycle
 * (ARCHITECTURE.md D7). Later: worker tuner, versioning, sinks.
 */
console.log("flux worker — N0 scaffold, Temporal worker coming next")

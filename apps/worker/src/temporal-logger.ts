import type { Logger, LogLevel, LogMetadata } from "@temporalio/worker"
import { Effect, type ManagedRuntime } from "effect"

/**
 * Temporal's logs, including a workflow's own, routed into Effect's logger.
 *
 * This is the half of D24 that was deferred: activity and use-case logs already
 * went through Effect, with D29's `annotateLogs` carrying correlation, while
 * anything a *workflow* logged went out through Temporal's own logger to
 * stderr. Two log streams for one deployment, only one of them correlated.
 *
 * **Not a sink, despite how the debt was written down.** `defaultSinks()` and
 * `LoggerSinks` are both deprecated in SDK 1.23, and their deprecation notes
 * point the same way: "To configure a custom logger, set the `Runtime.logger`
 * property instead". The SDK now forwards workflow log messages to the Runtime
 * logger on its own, so the custom sink the debt described would be building a
 * mechanism the SDK has since absorbed. Reading the deprecation was most of the
 * work.
 *
 * **The D6 boundary is what makes this legal.** A workflow's `log.info` is
 * captured inside the deterministic VM and replayed there, but it is *delivered*
 * here, on the main thread, which is the same side of the line as the payload
 * codec and the client interceptors. No Effect crosses into the sandbox.
 *
 * One limit worth stating rather than discovering: the SDK's own docs note that
 * messages from its native side are printed straight to the console
 * "independently of `RuntimeOptions.logger`", so the Rust core's logs do not
 * come through here.
 */
export const effectLogger = <A, E>(runtime: ManagedRuntime.ManagedRuntime<A, E>): Logger => {
  const emit = (level: LogLevel, message: string, meta?: LogMetadata): void => {
    const write = level === "ERROR"
      ? Effect.logError
      : level === "WARN"
      ? Effect.logWarning
      : level === "DEBUG"
      ? Effect.logDebug
      : level === "TRACE"
      ? Effect.logTrace
      : Effect.logInfo
    // The metadata is where the value is: a workflow's message arrives with
    // `workflowId`, `runId`, `workflowType` and `sdkComponent`, so it lands
    // beside the activity logs of the same deployment instead of in a stream
    // of its own.
    const annotated = meta === undefined
      ? write(message)
      : Effect.annotateLogs(write(message), meta as Record<string, unknown>)
    try {
      runtime.runSync(annotated)
    } catch {
      // A logger that can take the worker down is worse than a lost line.
    }
  }

  return {
    log: (level: LogLevel, message: string, meta?: LogMetadata) => emit(level, message, meta),
    trace: (message: string, meta?: LogMetadata) => emit("TRACE", message, meta),
    debug: (message: string, meta?: LogMetadata) => emit("DEBUG", message, meta),
    info: (message: string, meta?: LogMetadata) => emit("INFO", message, meta),
    warn: (message: string, meta?: LogMetadata) => emit("WARN", message, meta),
    error: (message: string, meta?: LogMetadata) => emit("ERROR", message, meta)
  }
}

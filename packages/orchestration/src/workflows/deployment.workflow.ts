import {
  ActivityFailure,
  ApplicationFailure,
  CancellationScope,
  condition,
  continueAsNew,
  defineQuery,
  defineSignal,
  defineUpdate,
  isCancellation,
  log,
  patched,
  proxyActivities,
  proxyLocalActivities,
  setHandler,
  upsertSearchAttributes,
  workflowInfo
} from "@temporalio/workflow"
import type { DeploymentActivities } from "../activities/types.ts"
import {
  type DeploymentInput,
  type DeploymentResult,
  type DeploymentState,
  type DeploymentStepInput,
  type DeploymentStrategy,
  SEARCH_ATTRIBUTES
} from "../deployment-input.ts"

/**
 * Canary deployment workflow — deterministic, plain TypeScript, ZERO Effect.
 *
 * Rollback is a saga: the first traffic shift registers a compensation
 * that restores the previous version. Every non-success termination — a
 * threshold breach, an abort, or an unexpected failure — runs the compensation
 * stack, so traffic is never left stranded on a bad version. approve/abort are
 * validated Updates; progress is exposed through the `status` query.
 *
 * Activity shapes: the health check is a **local activity** (a quick
 * call, no task-queue round-trip); monitoring is a regular activity that
 * **heartbeats** and runs inside a **CancellationScope**, so an abort cancels
 * the in-flight monitor immediately instead of waiting for it to finish.
 */

// Traffic shifts and notifications: ordinary activities.
const acts = proxyActivities<Pick<DeploymentActivities, "setTrafficWeight" | "notify" | "recordOutcome">>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 }
})

// The rollback compensation runs at the top task-queue priority (1 of 5, where
// lower is higher). When several deployments share a worker and one has to roll
// back, restoring the previous version jumps ahead of the others' forward
// traffic shifts, so users come off a bad version before new rollouts get slots.
const rollbackActs = proxyActivities<Pick<DeploymentActivities, "setTrafficWeight">>({
  startToCloseTimeout: "5 minutes",
  retry: { maximumAttempts: 3 },
  priority: { priorityKey: 1 }
})

// Monitoring is long-running and heartbeats, so it can be cancelled promptly and
// resumed if a worker dies mid-window.
const monitorActs = proxyActivities<Pick<DeploymentActivities, "monitorStep">>({
  startToCloseTimeout: "1 hour",
  heartbeatTimeout: "30 seconds",
  retry: { maximumAttempts: 3 }
})

// The health probe is fast and side-effect-free: a local activity avoids a
// separate activity task and its scheduling latency.
const localActs = proxyLocalActivities<Pick<DeploymentActivities, "healthCheck">>({
  startToCloseTimeout: "10 seconds",
  retry: { maximumAttempts: 3 }
})

// The rollback postmortem is best-effort colour on top of a completed
// rollback: a short deadline and a single attempt so a slow or absent LLM can
// never stretch out the rollback path.
const postmortemActs = proxyActivities<Pick<DeploymentActivities, "postmortem">>({
  startToCloseTimeout: "30 seconds",
  retry: { maximumAttempts: 1 }
})

/** Approve advancing past a manual-approval gate (rejected if none is open). */
export const approveUpdate = defineUpdate<void, []>("approve")
/** Abort an in-flight deployment (rejected once it has finished). */
export const abortUpdate = defineUpdate<void, []>("abort")
/**
 * Abort via a signal — the fire-and-forget form used by a parent workflow to
 * abort a child (child handles can signal but not update). Same effect as the
 * `abort` update, without the synchronous confirmation.
 */
export const abortSignal = defineSignal("abortSignal")
/**
 * A verdict on one unit of work, pushed in from wherever the truth lives.
 *
 * A signal rather than an update, and the reason is about who is calling. The
 * sender is an outside system (a CI job, a webhook, a reviewer's click) that
 * learned the outcome minutes or hours after flux routed the work. Temporal
 * accepts a signal whether or not a worker is running, and delivers it when one
 * returns; an update needs a live worker and fails without one. Losing a verdict
 * because the workers happened to be mid-redeploy would quietly bias the very
 * sample the decision rests on.
 *
 * Verdicts for any version other than the one being rolled out are dropped. The
 * rule is a limit on the new version, not a comparison between two, so counting
 * the old one's work would dilute the only rate being judged.
 */
export interface TaskOutcome {
  readonly version: string
  readonly success: boolean
}
export const taskOutcomeSignal = defineSignal<[TaskOutcome]>("taskOutcome")

/** Read the live deployment state. */
export const statusQuery = defineQuery<DeploymentState>("status")

const asApplicationFailure = (error: unknown): ApplicationFailure | undefined => {
  if (error instanceof ApplicationFailure) {
    return error
  }
  if (error instanceof ActivityFailure && error.cause instanceof ApplicationFailure) {
    return error.cause
  }
  return undefined
}

/**
 * A one-line account of what could not be settled. The interval is the part
 * that matters: "0.033 over 30 samples spans 0.006..0.167 around 0.05" says why
 * a reading under the limit still did not clear it, which the reading alone
 * never could.
 */
const describeUndecided = (
  pending: ReadonlyArray<{
    readonly metric: string
    readonly observed: number
    readonly limit: number
    readonly sampleSize: number
    readonly lower: number
    readonly upper: number
  }>
): string =>
  pending
    .map((p) =>
      `${p.metric} ${p.observed.toFixed(3)} over ${p.sampleSize} samples spans ` +
      `${p.lower.toFixed(3)}..${p.upper.toFixed(3)} around ${p.limit}`
    )
    .join("; ")

export async function deploymentWorkflow(input: DeploymentInput): Promise<DeploymentResult> {
  // Normalize the strategy. Older histories carry a top-level
  // `steps` array and no `strategy`, so they fall back to `canary` with those
  // steps and replay through the identical command sequence — no `patched()`
  // needed, because the path is selected by input data, not by a code change for
  // the same input.
  const strategy: DeploymentStrategy = input.strategy ?? { kind: "canary", steps: input.steps ?? [] }

  // Set when this run resumed from a continue-as-new mid-rollout.
  const resume = input.resumeFrom
  const completedBefore = resume?.completedSteps ?? 0

  let state: DeploymentState = {
    phase: resume === undefined ? "health-checking" : "shifting",
    service: input.service,
    version: input.version,
    currentPercent: resume?.lastPercent ?? 0,
    stepIndex: completedBefore,
    totalSteps: strategy.kind === "canary" ? completedBefore + strategy.steps.length : 1
  }
  let aborted = false
  let approved = false
  // Set while a monitor activity is in flight, so abort can cancel it at once.
  let cancelMonitor: (() => void) | undefined

  // Saga: undo actions to run (LIFO) on any non-success termination.
  const compensations: Array<() => Promise<void>> = []
  // Returns whether every undo succeeded — a failed undo means traffic may be
  // stranded on the bad version, which the breach path escalates.
  const compensate = async (): Promise<boolean> => {
    state = { ...state, phase: "rolling-back" }
    let restored = true
    while (compensations.length > 0) {
      const undo = compensations.pop()!
      try {
        await undo()
      } catch (error) {
        restored = false
        log.error("compensation failed", { error: String(error) })
      }
    }
    // The percentage has to follow the traffic, not the last thing attempted.
    // It did not, and a k3d run showed what that costs (D50): a rolled-back
    // deployment still reported `currentPercent: 10` while the router was back
    // to 100% on the previous version, so `flux status` told an operator a
    // tenth of production was still pointed at the version that just failed.
    //
    // A *failed* undo is the one case where the old value is the true one:
    // traffic really may be stranded on the bad version, which is what the
    // breach path escalates on. So the distinction the saga already tracks
    // becomes the distinction the state reports.
    if (restored) {
      state = { ...state, currentPercent: 0 }
    }
    return restored
  }

  upsertSearchAttributes({
    [SEARCH_ATTRIBUTES.service]: [input.service],
    [SEARCH_ATTRIBUTES.version]: [input.version],
    [SEARCH_ATTRIBUTES.status]: ["running"]
  })

  const abort = (): void => {
    aborted = true
    // If we are mid-window, cancel the monitor so the abort takes effect now.
    cancelMonitor?.()
  }

  // Seeded from the previous run, so bounding history does not discard evidence.
  let outcomes = input.resumeFrom?.outcomes ?? { total: 0, failures: 0 }

  setHandler(statusQuery, () => state)
  setHandler(taskOutcomeSignal, (outcome) => {
    if (outcome.version !== input.version) return
    outcomes = {
      total: outcomes.total + 1,
      failures: outcomes.failures + (outcome.success ? 0 : 1)
    }
  })
  setHandler(abortSignal, abort)
  setHandler(abortUpdate, abort, {
    validator: () => {
      if (state.phase === "done") {
        throw new Error("deployment already finished")
      }
    }
  })
  setHandler(approveUpdate, () => {
    approved = true
  }, {
    validator: () => {
      if (state.phase !== "awaiting-approval") {
        throw new Error("no approval gate is currently open")
      }
    }
  })

  let result: DeploymentResult
  try {
    result = strategy.kind === "canary" ? await runCanary(strategy.steps) : await runBlueGreen(strategy)
  } catch (error) {
    const failure = asApplicationFailure(error)
    if (failure === undefined) {
      throw error
    }
    log.warn("deployment failed", { type: failure.type })
    await compensate() // restore traffic if anything was shifted before the failure
    result = {
      kind: "Failed",
      service: input.service,
      reason: `${failure.type ?? "error"}: ${failure.message}`
    }
  }

  state = { ...state, phase: "done", outcome: result.kind }
  upsertSearchAttributes({ [SEARCH_ATTRIBUTES.status]: [result.kind] })
  await acts.recordOutcome(result.kind)
  return result

  async function runCanary(steps: ReadonlyArray<DeploymentStepInput>): Promise<DeploymentResult> {
    // 0. Announce the deployment. The `started` notification always
    //    existed in the Notification contract but was never sent — added
    //    behind `patched()` because inserting an activity changes the command
    //    sequence, exactly the edit that would break replay of every history
    //    recorded before it (the committed fixtures prove the guard works: they
    //    replay through the else-branch). `deprecatePatch` is deliberately NOT
    //    next: the committed fixtures stand in for in-flight production
    //    executions, and the replay lock refuses the deprecation while they
    //    exist — that is the patch lifecycle working, not a leftover.
    if (resume === undefined && patched("notify-deployment-started")) {
      await acts.notify({
        kind: "started",
        service: input.service,
        message: `deploying ${input.version} (canary over ${steps.length} steps)`
      })
    }

    // 1. Health-check the new version before shifting any traffic (local activity).
    //    A resumed run already passed this in its first incarnation.
    if (resume === undefined) {
      await localActs.healthCheck({ service: input.service, version: input.version })
    } else if (resume.trafficShifted) {
      // Traffic was already diverted before the continue-as-new — re-arm the
      // rollback compensation so a breach in this run still restores traffic.
      compensations.push(() =>
        rollbackActs.setTrafficWeight({ service: input.service, version: input.previousVersion, weight: 100 }))
    }

    // 2. Progressive canary steps.
    let lastPercent = resume?.lastPercent ?? 0
    for (let stepIndex = 0; stepIndex < steps.length; stepIndex++) {
      const step = steps[stepIndex]!
      if (aborted) {
        await compensate()
        return { kind: "Aborted", service: input.service, atPercent: lastPercent }
      }
      lastPercent = step.percent

      state = { ...state, phase: "shifting", currentPercent: step.percent, stepIndex: completedBefore + stepIndex }
      await acts.setTrafficWeight({
        service: input.service,
        version: input.version,
        weight: step.percent,
        // Lets a router with no state for the service (first deployment, or an
        // adapter restarted mid-canary) seed the complement correctly.
        previousVersion: input.previousVersion
      })
      // Register the compensation the first time traffic diverts to the new version.
      if (compensations.length === 0) {
        compensations.push(() =>
          rollbackActs.setTrafficWeight({
            service: input.service,
            version: input.previousVersion,
            weight: 100
          }))
      }

      state = { ...state, phase: "monitoring" }
      const evaluation = await observe(step.monitorMs)

      if (evaluation._tag === "Aborted") {
        await compensate()
        return { kind: "Aborted", service: input.service, atPercent: step.percent }
      }
      if (evaluation._tag === "Breached") {
        return await handleBreach(step.percent, evaluation.breaches)
      }
      // Undecided after the whole budget: no breach to point at, and no grounds
      // to promote either. Traffic goes back, because leaving a share of it on a
      // version nothing could vouch for is the one option that is not a
      // decision.
      if (evaluation._tag === "Inconclusive") {
        return await handleBreach(step.percent, [], describeUndecided(evaluation.pending))
      }

      // 3. Optional manual-approval gate.
      if (step.requiresApproval) {
        state = { ...state, phase: "awaiting-approval" }
        await condition(() => approved || aborted, step.approvalTimeoutMs)
        if (aborted) {
          await compensate()
          return { kind: "Aborted", service: input.service, atPercent: step.percent }
        }
        approved = false
      }

      // 4. Bound history: continue-as-new with the remaining steps when Temporal
      //    suggests it (long history) or an explicit step bound is reached.
      const isLastStep = stepIndex === steps.length - 1
      const reachedBound = input.continueAsNewAfterSteps !== undefined &&
        stepIndex + 1 >= input.continueAsNewAfterSteps
      if (!isLastStep && (workflowInfo().continueAsNewSuggested || reachedBound)) {
        await continueAsNew<typeof deploymentWorkflow>({
          ...input,
          // Carry the remaining steps forward as the canary strategy (the mapper
          // never sets the legacy top-level `steps`, so `...input` doesn't either).
          strategy: { kind: "canary", steps: steps.slice(stepIndex + 1) },
          resumeFrom: {
            completedSteps: completedBefore + stepIndex + 1,
            trafficShifted: compensations.length > 0,
            lastPercent: step.percent,
            outcomes
          }
        })
      }
    }

    // 5. Full rollout succeeded — commit (drop compensations, keep new version live).
    compensations.length = 0
    await acts.notify({ kind: "succeeded", service: input.service, message: input.version })
    return { kind: "Succeeded", service: input.service, version: input.version }
  }

  // The rollback path shared by both strategies: compensate, verify the
  // previous version is healthy again, notify, draft a postmortem, and return
  // `RolledBack` or the louder `RollbackFailed`. Extracting it keeps the two
  // strategies' breach handling identical — and the command sequence unchanged
  // for the committed canary histories.
  /**
   * Observe until the readings decide, or until the budget runs out.
   *
   * A window can now end without an answer. With few observations a rate that
   * has not crossed its limit is not the same claim as a rate we are confident
   * sits below it: 1 failure in 30 reads as 3.3% against a 5% limit, and the
   * true rate consistent with that sample reaches 16.7%. Promoting on it is a
   * coin flip with a number written on it. See `confidence.ts`.
   *
   * So an undecided window is extended rather than resolved by guesswork, up to
   * `maxMonitorMs`. Without that budget this runs exactly one window and the
   * behaviour is what it always was, which is also what happens to every
   * deployment whose rules carry no `sampleSize`: nothing else can produce an
   * undecided verdict.
   *
   * No `patched()` guards this, and the reason is worth writing down rather
   * than trusting. The extra iterations are unreachable for any execution that
   * started before this existed: an `Inconclusive` verdict requires a rule
   * carrying `sampleSize`, a field those inputs do not have, and on replay the
   * verdict comes from history, where only `Within` and `Breached` were ever
   * recorded. The command sequence is therefore identical.
   */
  async function observe(
    windowMs: number
  ): Promise<Awaited<ReturnType<DeploymentActivities["monitorStep"]>> | { readonly _tag: "Aborted" }> {
    // Two guards, both of which this loop needs to terminate at all, which in a
    // durable workflow means a hang that survives restarts rather than a hung
    // process someone kills.
    //
    //   - A window of zero cannot accumulate evidence, so it cannot be extended
    //     either. Without this, `spentMs` never grows and the loop runs forever
    //     on any undecided verdict.
    //   - A budget below the window means no extension, never a window cut
    //     short to fit the budget.
    const budgetMs = windowMs > 0 ? Math.max(windowMs, input.maxMonitorMs ?? windowMs) : 0
    let spentMs = 0

    for (;;) {
      const thisWindowMs = Math.min(windowMs, budgetMs - spentMs)
      // Its own scope per window, so an abort still cancels mid-observation.
      const monitorScope = new CancellationScope()
      cancelMonitor = () => monitorScope.cancel()
      let evaluation: Awaited<ReturnType<DeploymentActivities["monitorStep"]>>
      try {
        evaluation = await monitorScope.run(() =>
          monitorActs.monitorStep({
            service: input.service,
            version: input.version,
            windowMs: thisWindowMs,
            pollIntervalMs: input.pollIntervalMs,
            rules: input.rules,
            // Read at the top of each window rather than passed once: verdicts
            // keep arriving while one runs, and an extension exists precisely
            // to give the later ones a chance to land.
            outcomeRule: input.outcomeRule,
            outcomes
          }))
      } catch (error) {
        if (aborted && isCancellation(error)) return { _tag: "Aborted" }
        throw error
      } finally {
        cancelMonitor = undefined
      }

      spentMs += thisWindowMs
      if (evaluation._tag !== "Inconclusive" || spentMs >= budgetMs) return evaluation

      log.info("readings cannot decide yet, extending the window", {
        spentMs,
        budgetMs,
        undecided: evaluation.pending.map((pending) => pending.metric).join(",")
      })
    }
  }

  async function handleBreach(
    atPercent: number,
    breaches: ReadonlyArray<{ readonly metric: string; readonly observed: number; readonly limit: number }>,
    // Set when the rollback is for want of evidence rather than for a breach.
    // The outcome is the same (traffic goes back) and the reason is not, so the
    // operator reading the notification should not have to guess which it was.
    undecided?: string
  ): Promise<DeploymentResult> {
    log.warn(undecided === undefined ? "threshold breached, rolling back" : "evidence never arrived, rolling back", {
      atPercent
    })
    const restored = await compensate()

    let rollbackFailed = false
    let rollbackFailureReason = ""
    if (patched("verify-rollback")) {
      if (!restored) {
        rollbackFailed = true
        rollbackFailureReason = "compensation failed — traffic may be stranded on the bad version"
      } else {
        try {
          await localActs.healthCheck({ service: input.service, version: input.previousVersion })
        } catch {
          rollbackFailed = true
          rollbackFailureReason = `previous version ${input.previousVersion} is not healthy after rollback`
        }
      }
    }

    await acts.notify({
      kind: rollbackFailed ? "rollback-failed" : "rolled-back",
      service: input.service,
      message: rollbackFailed
        ? `rollback to ${input.previousVersion} did NOT restore health, needs attention`
        : undecided === undefined
        ? `regression at ${atPercent}%, rolled back to ${input.previousVersion}`
        : `not enough evidence at ${atPercent}% (${undecided}), rolled back to ${input.previousVersion}`
    })

    if (patched("rollback-postmortem")) {
      try {
        await postmortemActs.postmortem({
          service: input.service,
          version: input.version,
          previousVersion: input.previousVersion,
          atPercent,
          breaches
        })
      } catch (error) {
        log.warn("postmortem activity failed", { error: String(error) })
      }
    }

    if (rollbackFailed) {
      return {
        kind: "RollbackFailed",
        service: input.service,
        version: input.version,
        toVersion: input.previousVersion,
        atPercent,
        reason: rollbackFailureReason
      }
    }
    return {
      kind: "RolledBack",
      service: input.service,
      toVersion: input.previousVersion,
      atPercent,
      breaches
    }
  }

  // Blue/green: deploy the new version alongside the old, health-check it,
  // then flip 100% at once (optionally behind an approval) and bake. Because the
  // old version is never scaled down, a breach rolls back with a single shift.
  async function runBlueGreen(
    bg: Extract<DeploymentStrategy, { readonly kind: "blue-green" }>
  ): Promise<DeploymentResult> {
    if (patched("notify-deployment-started")) {
      await acts.notify({ kind: "started", service: input.service, message: `deploying ${input.version} (blue/green)` })
    }

    // 1. Health-check the new (green) version before any cutover.
    await localActs.healthCheck({ service: input.service, version: input.version })
    if (aborted) {
      return { kind: "Aborted", service: input.service, atPercent: 0 }
    }

    // 2. Optional approval before the flip.
    if (bg.requiresApproval) {
      state = { ...state, phase: "awaiting-approval" }
      await condition(() => approved || aborted, bg.approvalTimeoutMs)
      if (aborted) {
        return { kind: "Aborted", service: input.service, atPercent: 0 }
      }
      approved = false
    }

    // 3. Flip 100% at once (no split); arm the instant rollback.
    state = { ...state, phase: "shifting", currentPercent: 100, stepIndex: 0 }
    await acts.setTrafficWeight({
      service: input.service,
      version: input.version,
      weight: 100,
      previousVersion: input.previousVersion
    })
    compensations.push(() =>
      rollbackActs.setTrafficWeight({ service: input.service, version: input.previousVersion, weight: 100 }))

    // 4. Bake: one monitor over the bake window, cancellable by an abort.
    state = { ...state, phase: "monitoring" }
    const evaluation = await observe(bg.bakeMs)

    if (evaluation._tag === "Aborted") {
      await compensate()
      return { kind: "Aborted", service: input.service, atPercent: 100 }
    }
    if (evaluation._tag === "Breached") {
      return await handleBreach(100, evaluation.breaches)
    }
    if (evaluation._tag === "Inconclusive") {
      return await handleBreach(100, [], describeUndecided(evaluation.pending))
    }

    // 5. Bake passed — commit.
    compensations.length = 0
    await acts.notify({ kind: "succeeded", service: input.service, message: input.version })
    return { kind: "Succeeded", service: input.service, version: input.version }
  }
}

// Versioning behavior (PINNED — an in-flight deployment finishes on the worker
// version that started it) is set by the worker's `defaultVersioningBehavior`
// when it runs in versioned mode (FLUX_WORKER_BUILD_ID). It can't be declared
// statically here: Temporal rejects a versioning behavior when the worker isn't
// versioned, which is the default in dev and tests.

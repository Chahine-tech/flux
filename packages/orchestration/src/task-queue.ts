/**
 * Read a task queue's pending backlog and active poller count over the raw
 * DescribeTaskQueue gRPC. That backlog is the signal the server uses to drive
 * poller autoscaling, so it's handy to be able to look at it.
 *
 * The high-level Temporal client doesn't expose this call, so we reach for the
 * workflowService on a Connection (client.connection.workflowService). It's
 * typed structurally here, just the one method and the two fields we read, so
 * importing this doesn't drag the whole @temporalio runtime into the caller.
 */

// temporal.api.enums.v1.TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY
const TASK_QUEUE_TYPE_ACTIVITY = 2

export interface TaskQueueBacklog {
  /** Approximate number of tasks waiting to be dispatched on the queue. */
  readonly backlogCount: number
  /** Number of pollers currently attached to the queue. */
  readonly pollerCount: number
}

/** The one gRPC method this module needs, and only the response fields it reads. */
export interface DescribeTaskQueueService {
  describeTaskQueue(request: {
    readonly namespace: string
    readonly taskQueue: { readonly name: string }
    readonly taskQueueType: number
    readonly reportStats: boolean
    readonly reportPollers: boolean
  }): Promise<{
    readonly stats?: { readonly approximateBacklogCount?: number | { toNumber(): number } | null } | null
    readonly pollers?: ReadonlyArray<unknown> | null
  }>
}

/** `int64` fields arrive as protobufjs `Long` objects; coerce to a plain number. */
const toNumber = (value: number | { toNumber(): number } | null | undefined): number => {
  if (typeof value === "number") return value
  if (value != null && typeof value.toNumber === "function") return value.toNumber()
  return 0
}

export const taskQueueBacklog = async (
  service: DescribeTaskQueueService,
  params: { readonly namespace: string; readonly taskQueue: string }
): Promise<TaskQueueBacklog> => {
  const response = await service.describeTaskQueue({
    namespace: params.namespace,
    taskQueue: { name: params.taskQueue },
    taskQueueType: TASK_QUEUE_TYPE_ACTIVITY,
    reportStats: true,
    reportPollers: true
  })
  return {
    backlogCount: toNumber(response.stats?.approximateBacklogCount),
    pollerCount: response.pollers?.length ?? 0
  }
}

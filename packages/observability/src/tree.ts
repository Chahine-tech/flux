import { Effect, Exit, Layer, Logger, Option, Tracer } from "effect"

/**
 * The span tree, drawn in the terminal, with each log line sitting under the
 * span that emitted it.
 *
 * A development view, not an observability backend, and the difference is worth
 * stating because it is a real limit rather than a caveat. This can only draw
 * the spans of **one process**. flux's interesting trace crosses three, from
 * `flux deploy` through the control plane's HTTP handler into the worker's
 * activities, and Jaeger is where you go to see that. What this gives you is
 * the shape of one process's work without leaving the terminal, which is the
 * thing you want while changing code.
 *
 * It wraps the installed tracer rather than replacing it, so OTLP export keeps
 * working. Replacing it would have traded Jaeger for a pretty print, which is a
 * bad trade and an easy one to make by accident.
 */

interface Line {
  readonly seq: number
  readonly level: string
  readonly message: string
  readonly annotations: Readonly<Record<string, unknown>>
}

interface Node {
  readonly seq: number
  /** Set when the span ends. A span that failed has to look like it failed. */
  failed?: boolean
  readonly span: Tracer.Span
  readonly children: Array<Node>
  readonly lines: Array<Line>
  readonly parentId: string | undefined
}

const nodes = new Map<string, Node>()

/**
 * Children and log lines are interleaved by the order they happened in, which
 * has to be one clock. The first version mixed two: span times come from a
 * monotonic nanosecond source, while a log line only carries a `Date`. The
 * visible symptom was a warning drawn after the two calls it was emitted
 * between. A counter is exact here, because this only ever orders events from
 * a single process, which is all this view claims to draw anyway.
 */
let sequence = 0

const colour = process.stdout.isTTY === true
const c = (code: string, text: string): string => (colour ? `\u001b[${code}m${text}\u001b[0m` : text)
const dim = (text: string) => c("2", text)
const cyan = (text: string) => c("36", text)
const green = (text: string) => c("32", text)
const red = (text: string) => c("31", text)
const yellow = (text: string) => c("33", text)
const bold = (text: string) => c("1", text)

/**
 * Inbound or outbound, taken from the span's `kind` rather than from its name.
 * Seeing at a glance what entered and what left is most of what a trace is for,
 * and Effect already records it.
 */
const direction = (kind: Tracer.SpanKind): string =>
  kind === "server" || kind === "consumer"
    ? dim("←")
    : kind === "client" || kind === "producer"
    ? dim("→")
    : " "

const durationOf = (span: Tracer.Span): string => {
  if (span.status._tag !== "Ended") return dim("…")
  const ms = Number(span.status.endTime - span.status.startTime) / 1e6
  return dim(`${ms < 10 ? ms.toFixed(1) : Math.round(ms)}ms`)
}

/**
 * Braces around the set, the key dimmed and the value not. With five
 * annotations a flat `k=v k=v` run is unreadable, and the value is the part
 * being looked for.
 */
const annotationsOf = (line: Line): string => {
  const entries = Object.entries(line.annotations)
  if (entries.length === 0) return ""
  const pairs = entries.map(([k, v]) => `${dim(k + "=")}${String(v)}`).join(" ")
  return ` ${dim("{")} ${pairs} ${dim("}")}`
}

const levelColour = (level: string): string =>
  level === "Error" || level === "Fatal" ? red(level.toUpperCase()) : level === "Warn" ? yellow("WARN") : dim(level.toUpperCase())

/**
 * One node and everything under it. Children and log lines are merged into a
 * single time-ordered list, which is the whole point: a log line is only
 * informative next to the work that surrounded it.
 */
const render = (node: Node, prefix: string, last: boolean, root: boolean, out: Array<string>): void => {
  // Whether to draw a branch is about being the root, not about the prefix
  // being empty: the root's own children have an empty prefix too, and
  // conflating the two drew them as roots of their own.
  const branch = root ? "" : last ? "└─ " : "├─ "
  // Shape and outcome are two different questions, and one glyph answering both
  // was the first version's mistake: a failure broke the tree's rhythm because
  // the node marker itself changed. `◆`/`◇` says where you are, `✓`/`✗` says how
  // it went, and they sit on opposite sides of the name.
  const shape = dim(root ? "◆" : "◇")
  const status = node.span.status._tag !== "Ended" ? dim("·") : node.failed === true ? red("✗") : green("✓")
  const name = node.failed === true ? red(node.span.name) : cyan(node.span.name)
  out.push(
    `${prefix}${branch}${shape} ${direction(node.span.kind)} ${name}  ${status} ${durationOf(node.span)}`
  )

  const childPrefix = root ? "" : prefix + (last ? "   " : "│  ")
  const items: Array<{ seq: number; draw: (p: string, l: boolean) => void }> = []
  for (const child of node.children) {
    items.push({
      seq: child.seq,
      draw: (p, l) => render(child, p, l, false, out)
    })
  }
  for (const line of node.lines) {
    items.push({
      seq: line.seq,
      draw: (p, l) => {
        const b = l ? "└─ " : "├─ "
        out.push(`${p}${b}${yellow("●")} ${levelColour(line.level)} ${bold(line.message)}${annotationsOf(line)}`)
      }
    })
  }
  items.sort((a, b) => a.seq - b.seq)
  items.forEach((item, i) => item.draw(childPrefix, i === items.length - 1))
}

/** Drop a finished tree from the buffer once it has been printed. */
const forget = (node: Node): void => {
  nodes.delete(node.span.spanId)
  for (const child of node.children) forget(child)
}

/**
 * A local root is a span with no parent *in this process*. A span parented on
 * an `ExternalSpan` is one whose parent arrived in a `traceparent` header, so
 * it is a root here even though it is not the root of the trace. Rendering it
 * is the honest thing to do: this process cannot draw what it never saw.
 */
const isLocalRoot = (node: Node): boolean => node.parentId === undefined || !nodes.has(node.parentId)

const flushIfRoot = (node: Node): void => {
  if (!isLocalRoot(node)) return
  const out: Array<string> = []
  render(node, "", true, true, out)
  globalThis.console.log(out.join("\n"))
  forget(node)
}

const record = (span: Tracer.Span): Tracer.Span => {
  const parentId = Option.match(span.parent, {
    onNone: () => undefined,
    onSome: (p) => (p._tag === "Span" ? p.spanId : undefined)
  })
  const node: Node = { seq: sequence++, span, children: [], lines: [], parentId }
  nodes.set(span.spanId, node)
  if (parentId !== undefined) nodes.get(parentId)?.children.push(node)

  const end = span.end.bind(span)
  return new Proxy(span, {
    get(target, property, receiver) {
      if (property !== "end") return Reflect.get(target, property, receiver)
      return (endTime: bigint, exit: Exit.Exit<unknown, unknown>) => {
        end(endTime, exit)
        node.failed = Exit.isFailure(exit)
        flushIfRoot(node)
      }
    }
  })
}

/** The tracer half: wraps whatever is installed so export keeps working. */
const treeTracer = Layer.effect(
  Tracer.Tracer,
  Effect.map(Tracer.Tracer, (inner): Tracer.Tracer => Tracer.make({ span: (options) => record(inner.span(options)) }))
)

/**
 * The logger half. A line with no current span has nowhere to go in a tree, so
 * it is printed on its own rather than dropped: losing output to make a picture
 * tidier would be the wrong trade.
 */
const treeLogger = (currentSpan: (fiber: Logger.Options<unknown>["fiber"]) => Tracer.AnySpan | undefined) =>
  Logger.make((options: Logger.Options<unknown>) => {
    const span = currentSpan(options.fiber)
    const line: Line = {
      seq: sequence++,
      level: String(options.logLevel),
      // A row in a tree is one row. The workflow bundler logs thirty lines of
      // webpack output as a single message, and embedded newlines would pose as
      // tree rows and shred the drawing around them.
      message: String(options.message).replace(/\s*\n\s*/g, " "),
      annotations: Logger.formatStructured.log(options).annotations
    }
    const node = span === undefined ? undefined : nodes.get(span.spanId)
    if (node === undefined) {
      globalThis.console.log(`${yellow("●")} ${levelColour(line.level)} ${bold(line.message)}${annotationsOf(line)}`)
      return
    }
    node.lines.push(line)
  })

export const treeLayer = (
  currentSpan: (fiber: Logger.Options<unknown>["fiber"]) => Tracer.AnySpan | undefined
): Layer.Layer<never> => Layer.mergeAll(Logger.layer([treeLogger(currentSpan), Logger.tracerLogger]), treeTracer)

import { Connection } from "@temporalio/client"
import { SEARCH_ATTRIBUTES } from "@flux/orchestration"

// temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD
const KEYWORD = 2

/**
 * Ensure flux's custom search attributes exist on the target Temporal, so the
 * workflow can `upsertSearchAttributes` and `flux history` can query them.
 *
 * Registered by the app itself at startup (idempotent, infra-agnostic) rather
 * than a separate manual step — the worker owns the attributes it needs.
 */
export const ensureSearchAttributes = async (
  address: string,
  namespace: string
): Promise<void> => {
  const connection = await Connection.connect({ address })
  try {
    await connection.operatorService.addSearchAttributes({
      namespace,
      searchAttributes: {
        [SEARCH_ATTRIBUTES.service]: KEYWORD,
        [SEARCH_ATTRIBUTES.version]: KEYWORD,
        [SEARCH_ATTRIBUTES.status]: KEYWORD
      }
    })
    console.log("[flux] search attributes registered")
  } catch (error) {
    // Already present → nothing to do. Anything else is logged but non-fatal.
    const message = error instanceof Error ? error.message : String(error)
    if (!/already exist/i.test(message)) {
      console.warn(`[flux] could not register search attributes: ${message}`)
    }
  } finally {
    await connection.close()
  }
}

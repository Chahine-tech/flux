import * as nexus from "nexus-rpc"
import type { DeploymentInput, DeploymentResult } from "../deployment-input.ts"

/**
 * The Nexus service contract for flux-as-a-service (N9/D25): a caller
 * namespace triggers a canary in the platform namespace without any access to
 * it, through a Nexus endpoint the operator registers between them.
 *
 * Pure contract, no `@temporalio/nexus`/no handler logic — safe to import
 * from the caller workflow bundled into the VM (D6): `nexus.service`/
 * `nexus.operation` just build a plain descriptor, no I/O.
 */
export const DeployService = nexus.service("DeployService", {
  runCanary: nexus.operation<DeploymentInput, DeploymentResult>()
})

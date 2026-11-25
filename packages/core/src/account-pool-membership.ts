import { Effect } from "effect"
import { Credential } from "./credential.js"
import { Integration } from "./integration.js"
import type { Config } from "./account-pool-config.js"

export const validateMembership = (integrationID: Integration.ID, config: typeof Config.Type) =>
  Effect.gen(function* () {
    const credentials = yield* Credential.Service
    const saved = yield* credentials.list(integrationID)
    const methods = new Set<string>()
    for (const account of config.accounts) {
      const entry = saved.find((credential) => credential.id === account.credentialID)
      if (!entry || entry.integrationID !== integrationID || entry.value.type === "external") {
        return yield* new Integration.AuthorizationError({ cause: "Pool account is unavailable or unsupported" })
      }
      methods.add(entry.value.type === "oauth" ? `oauth:${entry.value.methodID}` : entry.value.type)
    }
    if (methods.size > 1) {
      return yield* new Integration.AuthorizationError({
        cause: "Pool accounts must use the same authentication method",
      })
    }
  })

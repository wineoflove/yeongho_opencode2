export * as AccountPool from "./account-pool.js"

import { Schema } from "effect"
import { Credential } from "./credential.js"
import { Integration } from "./integration.js"

export const Failure = Schema.Literals([
  "access-denied",
  "authentication",
  "rate-limited",
  "service",
  "transport",
  "unknown",
])

export const Status = Schema.Struct({
  integrationID: Integration.ID,
  enabled: Schema.Boolean,
  strategy: Schema.NullOr(Schema.Literal("round-robin")),
  paused: Schema.Boolean,
  lastSelectedCredentialID: Schema.NullOr(Credential.ID),
  accounts: Schema.Array(
    Schema.Struct({
      credentialID: Credential.ID,
      enabled: Schema.Boolean,
      failure: Schema.NullOr(Failure),
      authInvalid: Schema.Boolean,
      cooldownUntil: Schema.NullOr(Schema.Number),
      observedAt: Schema.NullOr(Schema.Number),
    }),
  ),
}).annotate({ identifier: "AccountPool.Status" })

export type Status = typeof Status.Type

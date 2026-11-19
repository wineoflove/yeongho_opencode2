import { Schema } from "effect"
import { Credential } from "./credential.js"

export const Config = Schema.Struct({
  strategy: Schema.Literal("round-robin"),
  accounts: Schema.Array(Schema.Struct({ credentialID: Credential.ID, enabled: Schema.Boolean })).check(
    Schema.isMaxLength(20),
    Schema.makeFilter((accounts) => new Set(accounts.map((account) => account.credentialID)).size === accounts.length, {
      expected: "unique account credential IDs",
    }),
  ),
})

import { expect } from "bun:test"
import { Effect, Layer } from "effect"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { validateMembership } from "@opencode/core/account-pool-membership"
import { it } from "./lib/effect"

const integrationID = Integration.ID.make("openai")
const key = Credential.Key.make({ type: "key", key: "fixture-secret" })
const oauth = (method: string) =>
  Credential.OAuth.make({
    type: "oauth",
    methodID: Integration.MethodID.make(method),
    access: "fixture-access",
    refresh: "fixture-refresh",
    expires: 0,
  })

for (const scenario of [
  { name: "same keys", values: [key, key], valid: true },
  { name: "same OAuth method", values: [oauth("legacy"), oauth("legacy")], valid: true },
  { name: "mixed key and OAuth", values: [key, oauth("legacy")], valid: false },
  { name: "different OAuth methods", values: [oauth("legacy"), oauth("sharing")], valid: false },
  { name: "missing credential", values: [key], valid: false },
  {
    name: "external marker",
    values: [Credential.External.make({ type: "external", methodID: Integration.MethodID.make("login") }), key],
    valid: false,
  },
]) {
  it.effect(`validates configured membership for ${scenario.name} without refreshing`, () => {
    const entries = scenario.values.map((value, index) =>
      Credential.Info.make({
        id: Credential.ID.make(`cred_${index}`),
        integrationID,
        label: `Fixture ${index}`,
        value,
      }),
    )
    return Effect.gen(function* () {
      const result = yield* validateMembership(integrationID, {
        strategy: "round-robin",
        accounts: [0, 1].map((index) => ({ credentialID: Credential.ID.make(`cred_${index}`), enabled: index === 0 })),
      }).pipe(Effect.result)
      expect(result._tag).toBe(scenario.valid ? "Success" : "Failure")
    }).pipe(Effect.provide(Layer.mock(Credential.Service, { list: () => Effect.succeed(entries) })))
  })
}

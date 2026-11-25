import { expect } from "bun:test"
import { Clock, Effect } from "effect"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { IntegrationConnection } from "@opencode/core/integration/connection"
import { PluginTestLayer } from "./fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(PluginTestLayer)

for (const registered of [true, false]) {
  it.effect(`rejects expired OAuth without a refresh implementation, registered=${registered}`, () =>
    Effect.gen(function* () {
      const integrations = yield* Integration.Service
      const credentials = yield* Credential.Service
      const integrationID = Integration.ID.make("anthropic")
      const methodID = Integration.MethodID.make("fixture-no-refresh")
      if (registered)
        yield* integrations.transform((editor) =>
          editor.method.update({
            integrationID,
            method: { id: methodID, type: "oauth", label: "Fixture" },
            authorize: () => Effect.die("Authorization must not run"),
          }),
        )
      const saved = yield* credentials.create({
        integrationID,
        value: Credential.OAuth.make({
          type: "oauth",
          methodID,
          access: "expired-fixture",
          refresh: "private-refresh",
          expires: 1,
        }),
      })
      const connection = IntegrationConnection.CredentialInfo.make({
        type: "credential",
        id: saved.id,
        label: saved.label,
        method: "oauth",
      })
      const failure = yield* integrations.connection.resolve(connection).pipe(Effect.flip)
      expect(failure._tag).toBe("Integration.Authorization")
      expect(String(failure)).not.toContain("private-refresh")
      expect(yield* credentials.get(saved.id)).toEqual(saved)
    }),
  )
}

it.effect("preserves a still-valid OAuth token without invoking a missing refresh implementation", () =>
  Effect.gen(function* () {
    const integrations = yield* Integration.Service
    const credentials = yield* Credential.Service
    const saved = yield* credentials.create({
      integrationID: Integration.ID.make("anthropic"),
      value: Credential.OAuth.make({
        type: "oauth",
        methodID: Integration.MethodID.make("fixture-no-refresh"),
        access: "valid-fixture",
        refresh: "fixture-refresh",
        expires: (yield* Clock.currentTimeMillis) + 3600000,
      }),
    })
    const connection = IntegrationConnection.CredentialInfo.make({
      type: "credential",
      id: saved.id,
      label: saved.label,
      method: "oauth",
    })
    expect(yield* integrations.connection.resolve(connection)).toEqual(saved.value)
    expect(yield* credentials.get(saved.id)).toEqual(saved)
  }),
)

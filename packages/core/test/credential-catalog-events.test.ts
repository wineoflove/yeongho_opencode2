import { expect } from "bun:test"
import { Effect } from "effect"
import { Bus } from "@opencode/core/bus"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(LayerNode.group([Credential.node, Bus.node])))

for (const change of ["tokens", "metadata", "method", "type"] as const) {
  it.effect(`publishes catalog invalidation appropriately for ${change} changes`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const bus = yield* Bus.Service
      const original = Credential.OAuth.make({
        type: "oauth",
        methodID: Integration.MethodID.make("original"),
        access: "old-access",
        refresh: "old-refresh",
        expires: 1,
        metadata: { accountID: "original" },
      })
      const saved = yield* credentials.create({ integrationID: Integration.ID.make("openai"), value: original })
      let updates = 0
      yield* bus.listen((event) =>
        Effect.sync(() => {
          if (event.type === Credential.Event.Updated.type) updates++
        }),
      )
      const value =
        change === "type"
          ? Credential.Key.make({ type: "key", key: "fixture" })
          : Credential.OAuth.make({
              ...original,
              access: "new-access",
              refresh: "new-refresh",
              expires: 2,
              methodID: Integration.MethodID.make(change === "method" ? "changed" : "original"),
              metadata: { accountID: change === "metadata" ? "changed" : "original" },
            })
      yield* credentials.update(saved.id, { value })
      expect(updates).toBe(change === "tokens" ? 0 : 1)
      expect((yield* credentials.get(saved.id))?.value).toEqual(value)
    }),
  )
}

import { expect } from "bun:test"
import { Effect, Layer } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/schema/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { it } from "./lib/effect"
import { Database } from "@opencode/core/database/database"
import { EffectFlock } from "@opencode/util/effect-flock"

const first = Credential.ID.make("cred_pauseA")
const second = Credential.ID.make("cred_pauseB")
const layer = LayerNode.compile(AccountPool.node)

it.live("persists a denial and refuses another account before any operation runs", () =>
  Effect.gen(function* () {
    const integrationID = Integration.ID.make("denied-fixture")
    yield* AccountPool.Service.use((pool) =>
      pool.reportFailure({ integrationID, credentialID: first }, "access-denied"),
    ).pipe(Effect.provide(AccountPool.layer))
    yield* Effect.gen(function* () {
      const pool = yield* AccountPool.Service
      expect((yield* pool.select(integrationID, [first, second]).pipe(Effect.flip))._tag).toBe("AccountPool.Paused")
      let sent = false
      const failure = yield* pool
        .withAccount(
          { integrationID, credentialID: second },
          Effect.sync(() => {
            sent = true
          }),
        )
        .pipe(Effect.flip)
      expect(failure._tag).toBe("AccountPool.Paused")
      expect(sent).toBe(false)
      expect(yield* pool.select(Integration.ID.make("unaffected-fixture"), [first, second])).toBe(first)
    }).pipe(Effect.provide(AccountPool.layer))
  }).pipe(Effect.provide(Layer.mergeAll(LayerNode.compile(Database.node), LayerNode.compile(EffectFlock.node)))),
)

it.live("does not reinterpret transport failure as access denial", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("network-fixture")
    yield* pool.reportFailure({ integrationID, credentialID: first }, "transport")
    expect(yield* pool.select(integrationID, [first, second])).toBe(first)
  }).pipe(Effect.provide(layer)),
)

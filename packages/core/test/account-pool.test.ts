import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/schema/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Database } from "@opencode/core/database/database"
import { KV } from "@opencode/core/kv"
import { EffectFlock } from "@opencode/util/effect-flock"
import { it } from "./lib/effect"

const first = Credential.ID.make("cred_first")
const second = Credential.ID.make("cred_second")
const persistent = LayerNode.compile(AccountPool.node)

describe("account pool selection", () => {
  it.effect("does not consume a rotation slot while previewing the next account", () =>
    Effect.gen(function* () {
      const provider = Integration.ID.make("fixture-preview")
      const pool = yield* AccountPool.Service
      expect(yield* pool.select(provider, [first, second], { advance: false })).toBe(first)
      expect(yield* pool.select(provider, [first, second], { advance: false })).toBe(first)
      expect((yield* pool.status(provider)).lastSelectedCredentialID).toBeNull()
      expect(yield* pool.select(provider, [first, second])).toBe(first)
      expect(yield* pool.select(provider, [first, second], { advance: false })).toBe(second)
      expect((yield* pool.status(provider)).lastSelectedCredentialID).toBe(first)
      expect(yield* pool.select(provider, [first, second])).toBe(second)
    }).pipe(Effect.provide(persistent)),
  )
  it.effect("reserves successive slots atomically for concurrent callers", () =>
    Effect.gen(function* () {
      const provider = Integration.ID.make("fixture-concurrent")
      const pool = yield* AccountPool.Service
      const result = yield* Effect.all(
        Array.from({ length: 4 }, () => pool.select(provider, [first, second])),
        { concurrency: "unbounded" },
      )
      expect(result).toEqual([first, second, first, second])
    }).pipe(Effect.provide(persistent)),
  )

  it.effect("keeps independent provider cursors and excludes removed candidates", () =>
    Effect.gen(function* () {
      const provider = Integration.ID.make("fixture-independent")
      const pool = yield* AccountPool.Service
      expect(yield* pool.select(provider, [first, second])).toBe(first)
      expect(yield* pool.select(Integration.ID.make("other"), [first, second])).toBe(first)
      expect(yield* pool.select(provider, [second])).toBe(second)
      expect(yield* pool.select(provider, [first, second])).toBe(first)
    }).pipe(Effect.provide(persistent)),
  )

  it.effect("fails an empty pool without changing its next eligible slot", () =>
    Effect.gen(function* () {
      const provider = Integration.ID.make("fixture-empty")
      const pool = yield* AccountPool.Service
      yield* pool.select(provider, [first, second])
      const error = yield* pool.select(provider, []).pipe(Effect.flip)
      expect(error._tag).toBe("AccountPool.Unavailable")
      expect(yield* pool.select(provider, [first, second])).toBe(second)
    }).pipe(Effect.provide(persistent)),
  )

  it.effect("continues from the persisted cursor in a newly created service", () =>
    Effect.gen(function* () {
      const provider = Integration.ID.make("fixture-recreate")
      const choose = AccountPool.Service.use((pool) => pool.select(provider, [first, second]))
      expect(yield* choose.pipe(Effect.provide(AccountPool.layer))).toBe(first)
      expect(yield* choose.pipe(Effect.provide(AccountPool.layer))).toBe(second)
      expect(yield* choose.pipe(Effect.provide(AccountPool.layer))).toBe(first)
    }).pipe(Effect.provide(Layer.mergeAll(LayerNode.compile(Database.node), LayerNode.compile(EffectFlock.node)))),
  )

  it.effect("rejects corrupt persisted state without overwriting it", () =>
    Effect.gen(function* () {
      const kv = yield* KV.Service
      const key = "ocm/account-pool/cursor/fixture-corrupt"
      yield* kv.set(key, { version: 999, credentialID: "unknown" })
      const error = yield* AccountPool.Service.use((pool) =>
        pool.select(Integration.ID.make("fixture-corrupt"), [first, second]),
      ).pipe(Effect.provide(AccountPool.layer), Effect.flip)
      expect(error._tag).toBe("AccountPool.StateUnavailable")
      expect(yield* kv.get(key)).toEqual({ version: 999, credentialID: "unknown" })
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          LayerNode.compile(KV.node),
          LayerNode.compile(Database.node),
          LayerNode.compile(EffectFlock.node),
        ),
      ),
    ),
  )
})

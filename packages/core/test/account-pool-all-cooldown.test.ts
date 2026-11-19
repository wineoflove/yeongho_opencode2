import { expect } from "bun:test"
import { Clock, Effect, Exit, Layer } from "effect"
import { TestClock } from "effect/testing"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/schema/integration"
import { Database } from "@opencode/core/database/database"
import { KV } from "@opencode/core/kv"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { EffectFlock } from "@opencode/util/effect-flock"
import { it } from "./lib/effect"
import { planOpenAiRotationTransfer } from "/Users/yeonghokim/apps/ocm-v2-staging/port/backend/src/services/openai-rotation-transfer"

const dependencies = Layer.mergeAll(
  LayerNode.compile(KV.node),
  LayerNode.compile(Database.node),
  LayerNode.compile(EffectFlock.node),
)

for (const rotationIndex of [0, 1, 7]) {
  it.effect(`imported all-cooldown plan with cursor ${rotationIndex} refuses selection until each exact expiry`, () =>
    Effect.gen(function* () {
      yield* TestClock.adjust("60000 millis")
      const now = yield* Clock.currentTimeMillis
      const account = (alias: string, until: number) => ({
        accountId: alias,
        accessToken: "synthetic-access",
        refreshToken: "synthetic-refresh",
        expiresAt: now + 7200000,
        planType: "pro",
        usageCount: 1,
        rateLimitedUntil: until,
      })
      const source = {
        version: 2,
        activeAlias: "a",
        rotationIndex,
        settings: { rotationStrategy: "round-robin" },
        accounts: { a: account("a", now + 1200000), b: account("b", now + 600000) },
      }
      const plan = planOpenAiRotationTransfer(source, { now, executionBudgetMs: 60000, preserveRateLimits: true })
      expect(plan.accountStates).toHaveLength(2)
      const candidates = plan.pool.accounts.map((entry) => Credential.ID.make(entry.credentialID))
      const kv = yield* KV.Service
      yield* kv.set(plan.cursor.key, plan.cursor.value)
      for (const entry of plan.accountStates ?? []) yield* kv.set(entry.key, entry.value)
      const select = () =>
        AccountPool.Service.use((pool) => pool.select(Integration.ID.make("openai"), candidates)).pipe(
          Effect.provide(AccountPool.layer),
          Effect.exit,
        )
      const chosen = (exit: Exit.Exit<Credential.ID, unknown>) => (Exit.isSuccess(exit) ? exit.value : "refused")
      expect(chosen(yield* select())).toBe("refused")
      yield* TestClock.adjust("599999 millis")
      expect(chosen(yield* select())).toBe("refused")
      yield* TestClock.adjust("1 millis")
      expect(chosen(yield* select())).toBe(candidates[1])
      expect(chosen(yield* select())).toBe(candidates[1])
      yield* TestClock.adjust("600000 millis")
      const afterAll = []
      for (let index = 0; index < 4; index++) afterAll.push(chosen(yield* select()))
      expect(new Set(afterAll)).toEqual(new Set(candidates))
      expect(afterAll[0]).not.toBe(afterAll[1])
      expect(afterAll[1]).not.toBe(afterAll[2])
    }).pipe(Effect.provide(dependencies)),
  )
}

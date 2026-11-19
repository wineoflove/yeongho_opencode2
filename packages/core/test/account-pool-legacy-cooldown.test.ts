import { expect } from "bun:test"
import { Clock, Effect, Layer } from "effect"
import { TestClock } from "effect/testing"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/schema/integration"
import { Database } from "@opencode/core/database/database"
import { KV } from "@opencode/core/kv"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { EffectFlock } from "@opencode/util/effect-flock"
import { it } from "./lib/effect"
import { snapshotOpenAiRateLimits } from "/Users/yeonghokim/apps/ocm-v2-staging/port/backend/src/services/openai-rate-limit-snapshot"
import { mapLegacyOpenAiCredentials } from "/Users/yeonghokim/apps/ocm-v2-staging/port/backend/src/services/openai-credential-transfer"
import { planOpenAiRotationTransfer } from "/Users/yeonghokim/apps/ocm-v2-staging/port/backend/src/services/openai-rotation-transfer"

const dependencies = Layer.mergeAll(
  LayerNode.compile(KV.node),
  LayerNode.compile(Database.node),
  LayerNode.compile(EffectFlock.node),
)

for (const cooledIndex of [0, 1, 2]) {
  for (const rotationIndex of [0, 1, 7]) {
    it.effect(`preserves six eligible selections with cooldown ${cooledIndex} and cursor ${rotationIndex}`, () =>
      Effect.gen(function* () {
        yield* TestClock.adjust("60000 millis")
        const now = yield* Clock.currentTimeMillis
        const source = {
          version: 2,
          activeAlias: "a",
          rotationIndex,
          settings: { rotationStrategy: "round-robin" },
          accounts: Object.fromEntries(
            ["a", "b", "c"].map((alias, index) => [
              alias,
              {
                accountId: alias,
                accessToken: "synthetic-access",
                refreshToken: "synthetic-refresh",
                expiresAt: now + 7200000,
                planType: "pro",
                usageCount: 1,
                ...(index === cooledIndex ? { rateLimitedUntil: now + 3600000 } : {}),
              },
            ]),
          ),
        }
        const plan = planOpenAiRotationTransfer(source, { now, executionBudgetMs: 60000, preserveRateLimits: true })
        if (!plan.accountStates) throw new Error("MISSING_COOLDOWN_STATE")
        expect(plan.accountStates).toHaveLength(1)
        const candidates = plan.pool.accounts.map((entry) => Credential.ID.make(entry.credentialID))
        const eligible = candidates.filter((_, index) => index !== cooledIndex)
        const kv = yield* KV.Service
        yield* kv.set(plan.cursor.key, plan.cursor.value)
        for (const entry of plan.accountStates) yield* kv.set(entry.key, entry.value)
        const actual = []
        for (let index = 0; index < 6; index++) {
          actual.push(
            yield* AccountPool.Service.use((pool) => pool.select(Integration.ID.make("openai"), candidates)).pipe(
              Effect.provide(AccountPool.layer),
            ),
          )
        }
        expect(actual).toEqual(
          Array.from({ length: 6 }, (_, index) => eligible[(rotationIndex + index) % eligible.length]),
        )
      }).pipe(Effect.provide(dependencies)),
    )
  }
}

it.effect("native selection retains imported cooldown across service recreation until its exact expiry", () =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis
    const account = (id: string) => ({
      accountId: id,
      accessToken: "synthetic-access",
      refreshToken: "synthetic-refresh",
      expiresAt: now + 7200000,
    })
    const source = {
      version: 2,
      activeAlias: "first",
      rotationIndex: 0,
      accounts: { first: { ...account("first"), rateLimitedUntil: now + 60000 }, second: account("second") },
    }
    const snapshot = snapshotOpenAiRateLimits(source, now)
    const candidates = mapLegacyOpenAiCredentials(source).credentials.map((entry) => Credential.ID.make(entry.id))
    const kv = yield* KV.Service
    for (const entry of snapshot.entries) yield* kv.set(entry.key, entry.value)
    const select = () =>
      AccountPool.Service.use((pool) => pool.select(Integration.ID.make("openai"), candidates)).pipe(
        Effect.provide(AccountPool.layer),
      )
    expect(yield* select()).toBe(candidates[1])
    yield* TestClock.adjust("59999 millis")
    expect(yield* select()).toBe(candidates[1])
    yield* TestClock.adjust("1 millis")
    expect(yield* select()).toBe(candidates[0])
    for (const entry of snapshot.entries) expect(yield* kv.get(entry.key)).toEqual(entry.value)
  }).pipe(Effect.provide(dependencies)),
)

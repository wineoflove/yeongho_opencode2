import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
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

describe("legacy OpenAI cursor consumed by native account pool", () => {
  for (const rotationIndex of [0, 1, 7]) {
    it.effect(`preserves next six choices across service recreation from cursor ${rotationIndex}`, () =>
      Effect.gen(function* () {
        const now = 1800000000000
        const plan = planOpenAiRotationTransfer(
          {
            version: 2,
            activeAlias: "first",
            rotationIndex,
            settings: { rotationStrategy: "round-robin" },
            accounts: Object.fromEntries(
              ["first", "second", "third"].map((alias) => [
                alias,
                {
                  accountId: `synthetic-${alias}`,
                  accessToken: `synthetic-access-${alias}`,
                  refreshToken: `synthetic-refresh-${alias}`,
                  expiresAt: now + 7200000,
                  planType: "pro",
                  usageCount: 1,
                },
              ]),
            ),
          },
          { now, executionBudgetMs: 60000 },
        )
        const kv = yield* KV.Service
        yield* kv.set(plan.cursor.key, plan.cursor.value)
        const otherKey = "ocm/account-pool/cursor/anthropic"
        const otherCursor = { version: 1, credentialID: "cred_unrelated" }
        yield* kv.set(otherKey, otherCursor)
        const candidates = plan.pool.accounts.map((entry) => Credential.ID.make(entry.credentialID))
        const select = (advance: boolean) =>
          AccountPool.Service.use((pool) => pool.select(Integration.ID.make("openai"), candidates, { advance })).pipe(
            Effect.provide(AccountPool.layer),
          )
        expect(yield* select(false)).toBe(candidates[rotationIndex % candidates.length])
        expect(yield* kv.get(plan.cursor.key)).toEqual(plan.cursor.value)
        const actual = []
        for (let index = 0; index < 6; index++) actual.push(yield* select(true))
        expect(actual).toEqual(
          Array.from({ length: 6 }, (_, index) => candidates[(rotationIndex + index) % candidates.length]),
        )
        expect(yield* kv.get(otherKey)).toEqual(otherCursor)
      }).pipe(Effect.provide(dependencies)),
    )
  }
})

import { expect } from "bun:test"
import { Clock, Effect } from "effect"
import { AIError, HttpContext, RateLimitError } from "@opencode/ai/effect"
import { TestClock } from "effect/testing"
import { AccountPool } from "@opencode/core/account-pool"
import { AccountPoolFailure } from "@opencode/core/account-pool-failure"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/schema/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(AccountPool.node))
const first = Credential.ID.make("cred_healthA")
const second = Credential.ID.make("cred_healthB")

it.effect("excludes an Anthropic account until its unified reset deadline beyond the default cooldown", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("anthropic")
    const now = yield* Clock.currentTimeMillis
    const error = new AIError({
      reason: new RateLimitError({
        message: "fixture",
        http: new HttpContext({
          url: "http://fixture.invalid",
          status: 429,
          headers: { "anthropic-ratelimit-unified-7d-reset": String((now + 604_800_000) / 1000) },
        }),
      }),
    })
    yield* pool.reportFailure(
      { integrationID, credentialID: first },
      AccountPoolFailure.classify(error),
      AccountPoolFailure.retryAfterMs(error, integrationID, now),
    )
    yield* TestClock.adjust("5 minutes")
    expect(yield* pool.select(integrationID, [first, second])).toBe(second)
    yield* TestClock.adjust("604499999 millis")
    expect(yield* pool.select(integrationID, [first, second])).toBe(second)
    yield* TestClock.adjust("1 millis")
    expect(yield* pool.select(integrationID, [first, second])).toBe(first)
  }),
)

it.effect("skips a rate-limited account until the reported deadline without shortening it on later errors", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("deadline-fixture")
    yield* pool.reportFailure({ integrationID, credentialID: first }, "rate-limited", 1000)
    yield* pool.reportFailure({ integrationID, credentialID: first }, "transport")
    expect(yield* pool.select(integrationID, [first, second])).toBe(second)
    yield* TestClock.adjust("999 millis")
    expect(yield* pool.select(integrationID, [first, second])).toBe(second)
    yield* TestClock.adjust("1 millis")
    expect(yield* pool.select(integrationID, [first, second])).toBe(first)
  }),
)

it.effect("keeps failed authentication blocked even after a transport error and elapsed time", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("authentication-fixture")
    const account = { integrationID, credentialID: first }
    yield* pool.reportFailure(account, "authentication")
    yield* pool.reportFailure(account, "transport")
    yield* TestClock.adjust("1 day")
    expect(yield* pool.select(integrationID, [first, second])).toBe(second)
    let executed = false
    const failure = yield* pool
      .withAccount(
        account,
        Effect.sync(() => {
          executed = true
        }),
      )
      .pipe(Effect.flip)
    expect(failure._tag).toBe("AccountPool.Unavailable")
    expect(executed).toBe(false)
  }),
)

it.effect("fails an exhausted pool without selecting its global active account", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("exhausted-fixture")
    yield* pool.reportFailure({ integrationID, credentialID: first }, "service", 1000)
    yield* pool.reportFailure({ integrationID, credentialID: second }, "authentication")
    expect((yield* pool.select(integrationID, [first, second]).pipe(Effect.flip))._tag).toBe("AccountPool.Unavailable")
    yield* TestClock.adjust("1 second")
    expect(yield* pool.select(integrationID, [first, second])).toBe(first)
  }),
)

it.effect("does not re-enable an account omitted from the configured candidates after cooldown", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("configured-disabled-fixture")
    yield* pool.reportFailure({ integrationID, credentialID: first }, "rate-limited", 1)
    yield* TestClock.adjust("1 day")
    expect(yield* pool.select(integrationID, [second])).toBe(second)
  }),
)

it.effect("uses a bounded fallback when retry-after is not finite", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("invalid-delay-fixture")
    yield* pool.reportFailure({ integrationID, credentialID: first }, "rate-limited", Infinity)
    expect(yield* pool.select(integrationID, [first, second])).toBe(second)
    yield* TestClock.adjust("5 minutes")
    expect(yield* pool.select(integrationID, [first, second])).toBe(first)
  }),
)

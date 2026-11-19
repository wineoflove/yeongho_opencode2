import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/schema/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(AccountPool.node))
const binding = (id: string): AccountPool.Binding => ({
  integrationID: Integration.ID.make("execution-fixture"),
  credentialID: Credential.ID.make(id),
})

describe("account execution permits", () => {
  it.live("serializes the same account while another account can execute", () =>
    Effect.gen(function* () {
      const pool = yield* AccountPool.Service
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const events: string[] = []
      const first = yield* pool
        .withAccount(
          binding("cred_A"),
          Effect.gen(function* () {
            events.push("A-start")
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
            events.push("A-end")
          }),
        )
        .pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      const second = yield* pool
        .withAccount(
          binding("cred_A"),
          Effect.sync(() => events.push("A-second")),
        )
        .pipe(Effect.forkScoped)
      yield* pool.withAccount(
        binding("cred_B"),
        Effect.sync(() => events.push("B")),
      )
      expect(events).toEqual(["A-start", "B"])
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
      expect(events).toEqual(["A-start", "B", "A-end", "A-second"])
    }),
  )

  it.live("releases the permit after operation failure", () =>
    Effect.gen(function* () {
      const pool = yield* AccountPool.Service
      const account = binding("cred_failure")
      expect(yield* pool.withAccount(account, Effect.fail("fixture-failure")).pipe(Effect.flip)).toBe("fixture-failure")
      expect(yield* pool.withAccount(account, Effect.succeed("released")).pipe(Effect.timeout("2 seconds"))).toBe(
        "released",
      )
    }),
  )

  it.live("releases an interrupted owner and never executes a cancelled waiter", () =>
    Effect.gen(function* () {
      const pool = yield* AccountPool.Service
      const account = binding("cred_cancel")
      const entered = yield* Deferred.make<void>()
      const first = yield* pool
        .withAccount(account, Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)))
        .pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      let waiterExecuted = false
      const waiter = yield* pool
        .withAccount(
          account,
          Effect.sync(() => {
            waiterExecuted = true
          }),
        )
        .pipe(Effect.forkScoped)
      yield* Fiber.interrupt(waiter)
      yield* Fiber.interrupt(first)
      expect(yield* pool.withAccount(account, Effect.succeed("available")).pipe(Effect.timeout("2 seconds"))).toBe(
        "available",
      )
      expect(waiterExecuted).toBe(false)
    }),
  )
})

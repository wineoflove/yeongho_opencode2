import { expect } from "bun:test"
import { Effect } from "effect"
import { TestClock } from "effect/testing"
import { AccountPool } from "@opencode/core/account-pool"
import { observeCredentialResolution } from "@opencode/core/account-pool-credential"
import { CredentialRequestError, credentialRequestError } from "@opencode/core/credential-request-error"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(AccountPool.node))
const first = Credential.ID.make("cred_refreshA")
const second = Credential.ID.make("cred_refreshB")

for (const code of ["invalid_grant", "invalid_refresh_token"]) {
  for (const nested of [false, true]) {
    it.effect(`rejects exact ${code}, nested=${nested}, without replay or private state`, () =>
      Effect.gen(function* () {
        const pool = yield* AccountPool.Service
        const integrationID = Integration.ID.make(`refresh-${code}-${nested}`)
        const cause = yield* Effect.promise(() =>
          credentialRequestError(
            Response.json(
              {
                error: nested ? { type: code, message: "fixture-private" } : code,
                error_description: "fixture-private",
              },
              { status: 400 },
            ),
          ),
        )
        const error = new Integration.AuthorizationError({ cause })
        let attempts = 0
        const operation = Effect.suspend(() => {
          attempts++
          return Effect.fail(error)
        })
        expect(
          yield* observeCredentialResolution(operation, pool, { integrationID, credentialID: first }).pipe(Effect.flip),
        ).toBe(error)
        expect(attempts).toBe(1)
        const state = yield* pool.status(integrationID, {
          strategy: "round-robin",
          accounts: [{ credentialID: first, enabled: true }],
        })
        expect(state.accounts[0]?.authInvalid).toBe(true)
        expect(state.accounts[0]?.failure).toBe("authentication")
        expect(state.paused).toBe(false)
        expect(JSON.stringify(state)).not.toContain("fixture-private")
        expect(yield* pool.select(integrationID, [first, second])).toBe(second)
      }),
    )
  }
}

for (const status of [429, 503]) {
  for (const retryAfter of ["7200", "0", "malformed", undefined]) {
    it.effect(`honors cooldown boundary for ${status}, Retry-After=${retryAfter}`, () =>
      Effect.gen(function* () {
        const pool = yield* AccountPool.Service
        const integrationID = Integration.ID.make(`refresh-timing-${status}-${retryAfter}`)
        const cause = yield* Effect.promise(() =>
          credentialRequestError(
            new Response("fixture-private", {
              status,
              headers: retryAfter === undefined ? undefined : { "Retry-After": retryAfter },
            }),
          ),
        )
        const error = new Integration.AuthorizationError({ cause })
        let attempts = 0
        yield* observeCredentialResolution(
          Effect.suspend(() => {
            attempts++
            return Effect.fail(error)
          }),
          pool,
          { integrationID, credentialID: first },
        ).pipe(Effect.flip)
        expect(attempts).toBe(1)
        const delay = retryAfter === "7200" ? 7200000 : retryAfter === "0" ? 0 : status === 429 ? 300000 : 30000
        const state = yield* pool.status(integrationID, {
          strategy: "round-robin",
          accounts: [{ credentialID: first, enabled: true }],
        })
        expect(state.accounts[0]?.cooldownUntil).toBe(delay)
        expect(JSON.stringify(state)).not.toContain("fixture-private")
        if (delay > 0) {
          yield* TestClock.adjust(delay - 1)
          expect((yield* pool.select(integrationID, [first]).pipe(Effect.flip))._tag).toBe("AccountPool.Unavailable")
          yield* TestClock.adjust(1)
        }
        expect(yield* pool.select(integrationID, [first])).toBe(first)
      }),
    )
  }
}

for (const status of [401, 403, 429, 503, 400]) {
  it.effect(`records refresh HTTP ${status} without retrying the failed operation`, () =>
    Effect.gen(function* () {
      const pool = yield* AccountPool.Service
      const integrationID = Integration.ID.make(`refresh-${status}`)
      const error = new Integration.AuthorizationError({ cause: new CredentialRequestError({ status }) })
      let attempts = 0
      const operation = Effect.suspend(() => {
        attempts++
        return Effect.fail(error)
      })
      expect(
        yield* observeCredentialResolution(operation, pool, { integrationID, credentialID: first }).pipe(Effect.flip),
      ).toBe(error)
      expect(attempts).toBe(1)
      const state = yield* pool.status(integrationID, {
        strategy: "round-robin",
        accounts: [first, second].map((credentialID) => ({ credentialID, enabled: true })),
      })
      expect(state.paused).toBe(status === 403)
      expect(state.accounts[0]?.authInvalid).toBe(status === 401)
      if (status === 403) {
        expect((yield* pool.select(integrationID, [first, second]).pipe(Effect.flip))._tag).toBe("AccountPool.Paused")
      } else {
        expect(yield* pool.select(integrationID, [first, second])).toBe(status === 400 ? first : second)
      }
    }),
  )
}

it.effect("does not infer authentication failure from an unstructured error message", () =>
  Effect.gen(function* () {
    const pool = yield* AccountPool.Service
    const integrationID = Integration.ID.make("refresh-unstructured")
    const error = new Integration.AuthorizationError({ cause: new Error("Request failed: 401 fixture-secret") })
    yield* observeCredentialResolution(Effect.fail(error), pool, { integrationID, credentialID: first }).pipe(
      Effect.flip,
    )
    const state = yield* pool.status(integrationID, {
      strategy: "round-robin",
      accounts: [{ credentialID: first, enabled: true }],
    })
    expect(state.accounts[0]?.failure).toBe("unknown")
    expect(state.accounts[0]?.authInvalid).toBe(false)
    expect(JSON.stringify(state)).not.toContain("fixture-secret")
  }),
)

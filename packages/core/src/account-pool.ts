export * as AccountPool from "./account-pool.js"

import { Clock, Context, Effect, Layer, Schedule, Schema } from "effect"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Credential } from "./credential.js"
import { Integration } from "@opencode/schema/integration"
import { eq } from "drizzle-orm"
import { Database } from "./database/database.js"
import { KVTable } from "./kv/sql.js"
import { EffectFlock } from "@opencode/util/effect-flock"
import type { AccountPool } from "@opencode/schema/account-pool"

import { Config } from "./account-pool-config.js"
export { Config } from "./account-pool-config.js"

export class UnavailableError extends Schema.TaggedError<UnavailableError>()("AccountPool.Unavailable", {
  integrationID: Integration.ID,
}) {
  override get message() {
    return `No eligible accounts in the pool for ${this.integrationID}`
  }
}

export class StateUnavailableError extends Schema.TaggedError<StateUnavailableError>()("AccountPool.StateUnavailable", {
  integrationID: Integration.ID,
}) {
  override get message() {
    return `Account pool selection state is unavailable for ${this.integrationID}`
  }
}

export class PausedError extends Schema.TaggedError<PausedError>()("AccountPool.Paused", {
  integrationID: Integration.ID,
}) {
  override get message() {
    return `Account pool paused after access denial for ${this.integrationID}`
  }
}

export type Failure = "access-denied" | "authentication" | "rate-limited" | "service" | "transport" | "unknown"

export interface Interface {
  readonly status: (
    integrationID: Integration.ID,
    config?: typeof Config.Type,
  ) => Effect.Effect<AccountPool.Status, StateUnavailableError>
  readonly reportFailure: (
    binding: Binding,
    failure: Failure,
    retryAfterMs?: number,
  ) => Effect.Effect<void, StateUnavailableError>
  readonly withAccount: <A, E, R>(
    binding: Binding,
    operation: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | StateUnavailableError | PausedError | UnavailableError, R>
  readonly select: (
    integrationID: Integration.ID,
    candidates: readonly Credential.ID[],
    options?: { readonly advance?: boolean },
  ) => Effect.Effect<Credential.ID, UnavailableError | StateUnavailableError | PausedError>
}

export interface Binding {
  readonly integrationID: Integration.ID
  readonly credentialID: Credential.ID
  readonly validate?: () => Effect.Effect<void, UnavailableError | StateUnavailableError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AccountPool") {}

const Cursor = Schema.Struct({ version: Schema.Literal(1), credentialID: Credential.ID })
const PoolState = Schema.Struct({ version: Schema.Literal(1), paused: Schema.Boolean })
const AccountState = Schema.Struct({
  version: Schema.Literal(1),
  failure: Schema.Literals(["access-denied", "authentication", "rate-limited", "service", "transport", "unknown"]),
  observedAt: Schema.Number,
  cooldownUntil: Schema.optional(Schema.Number),
  authInvalid: Schema.optional(Schema.Boolean),
})

function eligible(state: typeof AccountState.Type | undefined, now: number) {
  return (
    !state?.authInvalid &&
    state?.failure !== "authentication" &&
    (state?.cooldownUntil === undefined || state.cooldownUntil <= now)
  )
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = (yield* Database.Service).db
    const locks = yield* EffectFlock.Service
    const requireRunning = (binding: Binding) =>
      Effect.gen(function* () {
        const integrationID = binding.integrationID
        const row = yield* db
          .select({ value: KVTable.value })
          .from(KVTable)
          .where(eq(KVTable.key, `ocm/account-pool/state/${integrationID}`))
          .get()
        const state = row ? yield* Schema.decodeUnknownEffect(PoolState)(row.value) : undefined
        if (state?.paused) return yield* new PausedError({ integrationID })
        const accountRow = yield* db
          .select({ value: KVTable.value })
          .from(KVTable)
          .where(eq(KVTable.key, `ocm/account-pool/account/${integrationID}/${binding.credentialID}`))
          .get()
        const account = accountRow ? yield* Schema.decodeUnknownEffect(AccountState)(accountRow.value) : undefined
        if (!eligible(account, yield* Clock.currentTimeMillis)) return yield* new UnavailableError({ integrationID })
      }).pipe(
        Effect.mapError((error) =>
          error instanceof PausedError || error instanceof UnavailableError
            ? error
            : new StateUnavailableError({ integrationID: binding.integrationID }),
        ),
      )
    return Service.of({
      status: (integrationID, config) =>
        Effect.gen(function* () {
          const stateRow = yield* db
            .select({ value: KVTable.value })
            .from(KVTable)
            .where(eq(KVTable.key, `ocm/account-pool/state/${integrationID}`))
            .get()
          const state = stateRow ? yield* Schema.decodeUnknownEffect(PoolState)(stateRow.value) : undefined
          const cursorRow = yield* db
            .select({ value: KVTable.value })
            .from(KVTable)
            .where(eq(KVTable.key, `ocm/account-pool/cursor/${integrationID}`))
            .get()
          const cursor = cursorRow ? yield* Schema.decodeUnknownEffect(Cursor)(cursorRow.value) : undefined
          const accounts = yield* Effect.forEach(config?.accounts ?? [], (account) =>
            Effect.gen(function* () {
              const row = yield* db
                .select({ value: KVTable.value })
                .from(KVTable)
                .where(eq(KVTable.key, `ocm/account-pool/account/${integrationID}/${account.credentialID}`))
                .get()
              const stored = row ? yield* Schema.decodeUnknownEffect(AccountState)(row.value) : undefined
              return {
                credentialID: account.credentialID,
                enabled: account.enabled,
                failure: stored?.failure ?? null,
                authInvalid: stored?.authInvalid === true || stored?.failure === "authentication",
                cooldownUntil: stored?.cooldownUntil ?? null,
                observedAt: stored?.observedAt ?? null,
              }
            }),
          )
          return {
            integrationID,
            enabled: config !== undefined,
            strategy: config?.strategy ?? null,
            paused: state?.paused ?? false,
            lastSelectedCredentialID: cursor?.credentialID ?? null,
            accounts,
          }
        }).pipe(Effect.mapError(() => new StateUnavailableError({ integrationID }))),
      reportFailure: (binding, failure, retryAfterMs) =>
        db
          .transaction(
            (tx) =>
              Effect.gen(function* () {
                const accountKey = `ocm/account-pool/account/${binding.integrationID}/${binding.credentialID}`
                const now = yield* Clock.currentTimeMillis
                const oldRow = yield* tx
                  .select({ value: KVTable.value })
                  .from(KVTable)
                  .where(eq(KVTable.key, accountKey))
                  .get()
                const previous = oldRow ? yield* Schema.decodeUnknownEffect(AccountState)(oldRow.value) : undefined
                const delay = failure === "rate-limited" ? 300000 : failure === "service" ? 30000 : 0
                const retry =
                  retryAfterMs !== undefined && Number.isFinite(retryAfterMs) && retryAfterMs >= 0
                    ? Math.min(retryAfterMs, Number.MAX_SAFE_INTEGER - now)
                    : delay
                const accountValue = {
                  version: 1,
                  failure,
                  observedAt: now,
                  authInvalid:
                    failure === "authentication" ||
                    previous?.authInvalid === true ||
                    previous?.failure === "authentication",
                  cooldownUntil: Math.max(
                    previous?.cooldownUntil ?? 0,
                    failure === "rate-limited" || failure === "service" ? now + retry : 0,
                  ),
                }
                yield* tx
                  .insert(KVTable)
                  .values({ key: accountKey, value: accountValue })
                  .onConflictDoUpdate({ target: KVTable.key, set: { value: accountValue, time_updated: Date.now() } })
                  .run()
                if (failure !== "access-denied") return
                const key = `ocm/account-pool/state/${binding.integrationID}`
                const value = { version: 1, paused: true }
                yield* tx
                  .insert(KVTable)
                  .values({ key, value })
                  .onConflictDoUpdate({ target: KVTable.key, set: { value, time_updated: Date.now() } })
                  .run()
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.mapError(() => new StateUnavailableError({ integrationID: binding.integrationID }))),
      withAccount: (binding, operation) =>
        Effect.gen(function* () {
          yield* requireRunning(binding)
          yield* locks
            .acquire(`ocm-account-pool:${binding.integrationID}:${binding.credentialID}`, undefined, { timeoutMs: 250 })
            .pipe(
              Effect.retry({
                while: (error) => error._tag === "LockTimeoutError",
                schedule: Schedule.spaced("25 millis"),
              }),
              Effect.timeout("90 seconds"),
              Effect.mapError(() => new StateUnavailableError({ integrationID: binding.integrationID })),
            )
          yield* requireRunning(binding)
          if (binding.validate) yield* binding.validate()
          return yield* operation
        }).pipe(Effect.scoped),
      select: (integrationID, candidates, options) => {
        if (candidates.length === 0) return Effect.fail(new UnavailableError({ integrationID }))
        const key = `ocm/account-pool/cursor/${integrationID}`
        return db
          .transaction(
            (tx) =>
              Effect.gen(function* () {
                const stateRow = yield* tx
                  .select({ value: KVTable.value })
                  .from(KVTable)
                  .where(eq(KVTable.key, `ocm/account-pool/state/${integrationID}`))
                  .get()
                const state = stateRow ? yield* Schema.decodeUnknownEffect(PoolState)(stateRow.value) : undefined
                if (state?.paused) return yield* new PausedError({ integrationID })
                const now = yield* Clock.currentTimeMillis
                const availability = yield* Effect.forEach(candidates, (id) =>
                  Effect.gen(function* () {
                    const row = yield* tx
                      .select({ value: KVTable.value })
                      .from(KVTable)
                      .where(eq(KVTable.key, `ocm/account-pool/account/${integrationID}/${id}`))
                      .get()
                    return eligible(row ? yield* Schema.decodeUnknownEffect(AccountState)(row.value) : undefined, now)
                  }),
                )
                const row = yield* tx.select({ value: KVTable.value }).from(KVTable).where(eq(KVTable.key, key)).get()
                const previous = row ? yield* Schema.decodeUnknownEffect(Cursor)(row.value) : undefined
                const index = previous ? candidates.indexOf(previous.credentialID) : -1
                const selectedIndex = candidates.findIndex(
                  (_, offset) => availability[(index + 1 + offset) % candidates.length],
                )
                const next =
                  selectedIndex === -1 ? undefined : candidates[(index + 1 + selectedIndex) % candidates.length]
                if (!next) return yield* new UnavailableError({ integrationID })
                const value = { version: 1, credentialID: next }
                if (options?.advance !== false)
                  yield* tx
                    .insert(KVTable)
                    .values({ key, value })
                    .onConflictDoUpdate({ target: KVTable.key, set: { value, time_updated: Date.now() } })
                    .run()
                return next
              }),
            { behavior: "immediate" },
          )
          .pipe(
            Effect.mapError((error) =>
              error instanceof UnavailableError || error instanceof PausedError
                ? error
                : new StateUnavailableError({ integrationID }),
            ),
          )
      },
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [Database.node, EffectFlock.node] })

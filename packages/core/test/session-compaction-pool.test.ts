import { expect } from "bun:test"
import { CompactionPart, Message } from "@opencode/ai/effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { RequestAuthContext } from "@opencode/core/request-auth"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionMessageTable } from "@opencode/core/session/sql"
import { DateTime, Effect, Option, Schema } from "effect"
import { bodies, it, sent, setup } from "./session-compaction-pool-fixture.test"

for (const pooled of [false, true]) {
  it.effect(`summary preserves hook routing and HTTP options across a nudge (pool=${pooled})`, () =>
    Effect.gen(function* () {
      const fixture = yield* setup(pooled, "summary")
      const hooks = yield* PluginHooks.Service
      yield* hooks.register("session", "model.request", (event) =>
        Effect.sync(() => {
          event.baseURL = "https://hook.invalid/custom"
          event.headers["x-hook"] = "preserved"
          event.headers["Authorization"] = "Bearer fixture-old-hook"
          event.headers["chatgpt-account-id"] = "old-account"
        }),
      )
      yield* hooks.register("session", "compaction", (event) =>
        Effect.sync(() => {
          event.options.temperature = 0.3
          event.options.store = false
        }),
      )
      yield* hooks.register("session", "http.request", (event) =>
        Effect.gen(function* () {
          const auth = yield* Effect.serviceOption(RequestAuthContext)
          event.request.headers.set(
            "x-http-hook",
            Option.match(auth, {
              onNone: () => "missing-auth-context",
              onSome: (value) => `${value.integrationID}:${value.method}:${event.kind}`,
            }),
          )
        }),
      )
      expect(yield* fixture.compact("manual")).toEqual({ status: "completed" })
      expect(sent.map((request) => new URL(request.url).origin + new URL(request.url).pathname)).toEqual([
        "https://hook.invalid/custom/responses",
        "https://hook.invalid/custom/responses",
      ])
      expect(sent.map((request) => request.headers.get("authorization"))).toEqual(
        pooled ? ["Bearer fixture-A", "Bearer fixture-B"] : ["Bearer fixture-original", "Bearer fixture-original"],
      )
      expect(sent.map((request) => request.headers.get("chatgpt-account-id"))).toEqual(
        pooled ? [null, null] : ["old-account", "old-account"],
      )
      expect(sent.map((request) => request.headers.get("x-hook"))).toEqual(["preserved", "preserved"])
      expect(sent.map((request) => request.headers.get("x-http-hook"))).toEqual([
        "test:null:compaction",
        "test:null:compaction",
      ])
      expect(bodies.map((body) => ({ temperature: body.temperature, store: body.store }))).toEqual([
        { temperature: 0.3, store: false },
        { temperature: 0.3, store: false },
      ])
    }),
  )
}

for (const [mechanism, withCheckpoint] of [
  ["trigger", false],
  ["endpoint", false],
  ["trigger", true],
  ["endpoint", true],
  ["summary", true],
] as const) {
  for (const reason of ["manual", "auto", "overflow"] as const) {
    it.effect(`pooled ${mechanism} refuses ${reason} without replacing history (checkpoint=${withCheckpoint})`, () =>
      Effect.gen(function* () {
        const fixture = yield* setup(true, mechanism)
        const checkpoint = SessionMessage.CompactionCompleted.make({
          id: SessionMessage.ID.create(),
          type: "compaction",
          status: "completed",
          reason: "manual",
          summary: "",
          model: fixture.context.model.ref,
          time: { created: DateTime.makeUnsafe(0) },
          native: {
            messages: [
              Message.assistant([
                CompactionPart.make({ provider: fixture.context.model.model.provider, encrypted: "opaque-checkpoint" }),
              ]),
            ],
          },
        })
        const measured = SessionMessage.Assistant.make({
          id: SessionMessage.ID.create(),
          type: "assistant",
          agent: Agent.defaultID,
          model: fixture.context.model.ref,
          content: [],
          time: { created: DateTime.makeUnsafe(0) },
          tokens: { input: 190_000, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        })
        if (withCheckpoint) fixture.context.messages.unshift(checkpoint)
        fixture.context.messages.push(measured)
        yield* fixture.db
          .insert(SessionMessageTable)
          .values(
            fixture.context.messages.map((message, index) => {
              const { id, type, ...data } = Schema.encodeSync(SessionMessage.Info)(message)
              return {
                id: SessionMessage.ID.make(id),
                type,
                data,
                session_id: fixture.context.session.id,
                seq: index + 1,
                time_created: 0,
              }
            }),
          )
          .run()
        yield* Bus.reserveSequence(fixture.db, fixture.context.session.id, fixture.context.messages.length)
        const before = yield* fixture.store.messages({ sessionID: fixture.context.session.id, order: "asc" })
        const pools = yield* AccountPool.Service
        if (reason === "manual" && mechanism !== "summary")
          yield* pools.reportFailure(
            { integrationID: fixture.integrationID, credentialID: fixture.accounts[0].id },
            "access-denied",
          )
        const outcome = yield* fixture.compact(reason)
        expect(outcome).toMatchObject({ status: "failed", error: { type: "provider.unsupported-operation" } })
        expect(sent).toHaveLength(0)
        const after = yield* fixture.store.messages({ sessionID: fixture.context.session.id, order: "asc" })
        expect(after.slice(0, before.length)).toEqual(before)
        expect(after.at(-1)).toMatchObject({ type: "compaction", status: "failed" })
      }),
    )
  }
}

for (const mechanism of ["trigger", "endpoint"] as const) {
  it.effect(`nonpool native ${mechanism} still stores a checkpoint`, () =>
    Effect.gen(function* () {
      const fixture = yield* setup(false, mechanism)
      expect(yield* fixture.compact("manual")).toEqual({ status: "completed" })
      expect(sent).toHaveLength(1)
      const messages = yield* fixture.store.context(fixture.context.session.id)
      expect(messages.at(-1)).toMatchObject({ type: "compaction", status: "completed", summary: "" })
      expect(JSON.stringify(messages.at(-1))).toContain("new-encrypted")
    }),
  )
}

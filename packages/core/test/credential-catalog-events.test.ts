import { expect } from "bun:test"
import { Context, Deferred, Effect, Fiber, Layer } from "effect"
import { Bus } from "@opencode/core/bus"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"
import { Database } from "@opencode/core/database/database"
import { tmpdirScoped } from "./fixture/tmpdir"

const it = testEffect(LayerNode.compile(LayerNode.group([Credential.node, Bus.node])))

it.live("refresh CAS rejects a metadata writer on an independent SQLite connection", () =>
  Effect.gen(function* () {
    const directory = yield* tmpdirScoped()
    const graph = LayerNode.compile(LayerNode.group([Credential.node, Database.node]), {
      replacements: [Database.node.replace(Database.configured({ path: `${directory.path}/credentials.db` }))],
    })
    const scope = yield* Effect.scope
    const first = yield* Layer.buildWithMemoMap(graph, yield* Layer.makeMemoMap, scope)
    const second = yield* Layer.buildWithMemoMap(graph, yield* Layer.makeMemoMap, scope)
    expect(Context.get(first, Database.Service).db).not.toBe(Context.get(second, Database.Service).db)
    const a = Context.get(first, Credential.Service)
    const b = Context.get(second, Credential.Service)
    const original = Credential.OAuth.make({
      type: "oauth",
      methodID: Integration.MethodID.make("fixture"),
      access: "old",
      refresh: "refresh",
      expires: 1,
      metadata: { accountID: "original" },
    })
    const stored = yield* a.create({ integrationID: Integration.ID.make("test"), value: original })
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const running = yield* a
      .refresh(stored.id, {
        stale: () => true,
        refresh: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as(Credential.OAuth.make({ ...original, access: "stale-result" })),
          ),
      })
      .pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
    yield* Deferred.await(entered)
    const changed = Credential.OAuth.make({ ...original, metadata: { accountID: "changed" } })
    yield* b.update(stored.id, { value: changed })
    yield* Deferred.succeed(release, undefined)
    expect(yield* Fiber.join(running)).toMatchObject({ _tag: "Failure", failure: { _tag: "Credential.Changed" } })
    expect((yield* a.get(stored.id))?.value).toEqual(changed)
  }).pipe(Effect.scoped),
)

for (const change of ["tokens", "metadata", "method", "type"] as const) {
  it.effect(`publishes catalog invalidation appropriately for ${change} changes`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const bus = yield* Bus.Service
      const original = Credential.OAuth.make({
        type: "oauth",
        methodID: Integration.MethodID.make("original"),
        access: "old-access",
        refresh: "old-refresh",
        expires: 1,
        metadata: { accountID: "original" },
      })
      const saved = yield* credentials.create({ integrationID: Integration.ID.make("openai"), value: original })
      let updates = 0
      yield* bus.listen((event) =>
        Effect.sync(() => {
          if (event.type === Credential.Event.Updated.type) updates++
        }),
      )
      const value =
        change === "type"
          ? Credential.Key.make({ type: "key", key: "fixture" })
          : Credential.OAuth.make({
              ...original,
              access: "new-access",
              refresh: "new-refresh",
              expires: 2,
              methodID: Integration.MethodID.make(change === "method" ? "changed" : "original"),
              metadata: { accountID: change === "metadata" ? "changed" : "original" },
            })
      yield* credentials.update(saved.id, { value })
      expect(updates).toBe(change === "tokens" ? 0 : 1)
      expect((yield* credentials.get(saved.id))?.value).toEqual(value)
    }),
  )
}

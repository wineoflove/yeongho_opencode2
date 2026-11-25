import { LanguageModel } from "@opencode/ai/effect"
import { OpenAI } from "@opencode/ai/providers"
import { AccountPool } from "@opencode/core/account-pool"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Credential } from "@opencode/core/credential"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Integration } from "@opencode/core/integration"
import { Location } from "@opencode/core/location"
import { Model } from "@opencode/core/model"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { SessionCompaction } from "@opencode/core/session/compaction"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionSchema } from "@opencode/core/session/schema"
import { SessionStore } from "@opencode/core/session/store"
import { httpClient } from "@opencode/util/effect/app-node-platform"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { DateTime, Effect, Layer, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import { testEffect } from "./lib/effect"

export const sent: Request[] = []
export const bodies: Record<string, unknown>[] = []
const wire = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.gen(function* () {
      const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
      sent.push(web)
      const body = yield* Effect.promise(() => web.json())
      bodies.push(Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))(body))
      const checkpoint = { type: "compaction", id: "cmp_new", encrypted_content: "new-encrypted" }
      const native = JSON.stringify(body).includes("compaction_trigger")
      if (web.url.endsWith("/compact"))
        return HttpClientResponse.fromWeb(
          request,
          Response.json({
            id: "cmp",
            object: "response.compaction",
            output: [checkpoint],
            usage: { input_tokens: 20, output_tokens: 4, total_tokens: 24 },
          }),
        )
      const text = sent.length === 1 && !native ? "needs a nudge" : "## Objective\n- preserved"
      return HttpClientResponse.fromWeb(
        request,
        new Response(
          [
            { type: "response.output_item.added", item: { type: "message", id: "msg" } },
            { type: "response.output_text.delta", item_id: "msg", output_index: 0, content_index: 0, delta: text },
            {
              type: "response.completed",
              response: { id: "resp", status: "completed", output: native ? [checkpoint] : [] },
            },
          ]
            .map((event) => `data: ${JSON.stringify(event)}\n\n`)
            .join(""),
          { headers: { "content-type": "text/event-stream" } },
        ),
      )
    }),
  ),
)
export const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      SessionCompaction.node,
      Agent.node,
      SessionStore.node,
      SessionProjector.node,
      PluginHooks.node,
      Credential.node,
      Integration.node,
      Provider.node,
      AccountPool.node,
      Bus.node,
      Database.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Location.node.replace(Location.boundNode({ directory: AbsolutePath.make("/project") })),
      httpClient.replace(wire),
    ],
  ),
)

export const setup = Effect.fnUntraced(function* (pooled: boolean, mechanism: "summary" | "trigger" | "endpoint") {
  sent.length = 0
  bodies.length = 0
  const providers = yield* Provider.Service
  const credentials = yield* Credential.Service
  const integrations = yield* Integration.Service
  const bus = yield* Bus.Service
  const db = (yield* Database.Service).db
  const store = yield* SessionStore.Service
  const integrationID = Integration.ID.make("test")
  const providerID = Provider.ID.make("test")
  yield* integrations.transform((registry) =>
    registry.update(integrationID, (entry) => {
      entry.name = "Fixture"
    }),
  )
  const accounts = yield* Effect.forEach(["fixture-A", "fixture-B"], (key) =>
    credentials.create({ integrationID, value: Credential.Key.make({ type: "key", key }) }),
  )
  yield* providers.transform((catalog) => {
    catalog.update(providerID, (draft) => {
      draft.activation = "enabled"
      draft.package = "@opencode/ai/providers/openai"
      draft.settings = {
        baseURL: "https://configured.invalid/v1",
        ...(pooled
          ? {
              accountPool: {
                strategy: "round-robin",
                accounts: accounts.map((a) => ({ credentialID: a.id, enabled: true })),
              },
            }
          : {}),
      }
    })
    catalog.models.update(providerID, Model.ID.make("summary-model"), (draft) => {
      draft.enabled = true
    })
  })
  const base = OpenAI.configure({ apiKey: "fixture-original", baseURL: "https://configured.invalid/v1" }).responses(
    "summary-model",
  )
  const model = {
    ...SessionRunnerModel.resolved(
      LanguageModel.update(base, {
        provider: "test",
        route:
          mechanism === "endpoint"
            ? base.route.with({ compact: { endpoint: base.route.compact.endpoint } })
            : base.route,
      }),
      {
        capabilities: { tools: true, input: ["text"], output: ["text"] },
        cost: [],
        limit: { context: 200_000, output: 32_000 },
        ...(mechanism === "summary" ? {} : { compaction: { type: "native" as const } }),
      },
    ),
    requestAuth: { integrationID: "test", method: null },
  }
  const sessionID = SessionSchema.ID.create()
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .run()
  yield* bus.publish(SessionEvent.Created, {
    sessionID,
    projectID: Project.ID.global,
    location: { directory: AbsolutePath.make("/project") },
    slug: "compaction-pool",
    version: "test",
  })
  const session = yield* store.get(sessionID)
  if (!session) return yield* Effect.die("Missing fixture session")
  const messages: SessionMessage.Info[] = [
    SessionMessage.User.make({
      id: SessionMessage.ID.create(),
      type: "user",
      text: "Keep this conversation",
      time: { created: DateTime.makeUnsafe(0) },
    }),
  ]
  const context = {
    session,
    model,
    messages,
    initial: [],
    agent: { id: Agent.defaultID, info: Agent.Info.default(Agent.defaultID) },
    tools: { definitions: [], execute: () => Effect.die("Compaction must not execute tools") },
  }
  const compaction = yield* SessionCompaction.Service
  const compact = Effect.fnUntraced(function* (reason: "manual" | "auto" | "overflow") {
    if (reason !== "manual")
      return yield* compaction.compact({ reason, context, estimate: { measured: 190_000, estimated: 0 } })
    const inputID = SessionMessage.ID.create()
    yield* bus.publish(SessionEvent.Compaction.Started, { sessionID, reason, inputID })
    return yield* compaction.compact({ reason, context, inputID })
  })
  return { context, compact, bus, db, store, accounts, integrationID, compaction }
})

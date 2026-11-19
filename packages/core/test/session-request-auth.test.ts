import { expect } from "bun:test"
import { DateTime, Effect } from "effect"
import { OpenAIChat } from "@opencode/ai/protocols"
import { Agent } from "@opencode/schema/agent"
import { Money } from "@opencode/schema/money"
import { Session } from "@opencode/schema/session"
import { Location } from "@opencode/core/location"
import { Project } from "@opencode/core/project"
import { AbsolutePath } from "@opencode/core/schema"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { requestUsesOAuth, withRequestAuth } from "@opencode/core/request-auth"
import { testEffect } from "./lib/effect"
import { PluginTestLayer } from "./plugin/fixture"

const it = testEffect(PluginTestLayer)

it.effect("keeps captured auth in deferred retries despite a different caller context", () =>
  Effect.gen(function* () {
    const hooks = yield* PluginHooks.Service
    const seen: boolean[] = []
    yield* hooks.register("session", "retry", () =>
      Effect.gen(function* () {
        seen.push(yield* requestUsesOAuth(["chatgpt-token-sharing"], false))
      }),
    )
    const session = Session.Info.make({
      id: Session.ID.make("ses_deferred_auth"),
      projectID: Project.ID.global,
      cost: Money.USD.zero,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
      location: Location.Ref.make({ directory: AbsolutePath.make("/fixture") }),
    })
    const base = SessionRunnerModel.resolved(OpenAIChat.route.model({ id: "fixture", provider: "test" }), {
      capabilities: { tools: true, input: ["text"], output: ["text"] },
      cost: [],
      limit: { context: 1000, output: 100 },
    })
    const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
    const prepared = yield* Effect.forEach(["chatgpt-token-sharing", null], (method) =>
      requests.primary({
        session,
        agent: Agent.ID.make("build"),
        system: [],
        messages: [],
        model: { ...base, requestAuth: { integrationID: "openai", method } },
      }),
    )
    for (const request of prepared)
      yield* withRequestAuth(
        request.retry({
          sessionID: session.id,
          agent: Agent.ID.make("build"),
          model: base.ref,
          error: { type: "provider.rate-limit", message: "fixture", status: 429 },
          attempt: 2,
          decision: { retry: true, delay: 1000 },
        }),
        { integrationID: "openai", method: "chatgpt-headless" },
      )
    expect(seen).toEqual([true, false])
  }).pipe(
    Effect.provideService(SessionModelTransport.Service, {
      bind: () => ({ execute: () => Effect.die("Unexpected transport") }),
      close: () => Effect.void,
      closeAll: Effect.void,
    }),
  ),
)

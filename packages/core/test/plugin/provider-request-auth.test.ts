import { expect } from "bun:test"
import { Effect } from "effect"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { Session } from "@opencode/core/session"
import { Agent } from "@opencode/schema/agent"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { OpenAIPlugin } from "@opencode/core/plugin/provider/openai"
import { ChatGPTPlugin } from "@opencode/core/plugin/provider/chatgpt"
import { withRequestAuth } from "@opencode/core/request-auth"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

it.effect("applies the real sharing retry policy only to the captured sharing request", () =>
  Effect.gen(function* () {
    const credentials = yield* Credential.Service
    yield* credentials.create({
      integrationID: Integration.ID.make("openai"),
      value: Credential.Key.make({ type: "key", key: "fixture" }),
    })
    yield* ChatGPTPlugin.effect(yield* PluginHost.make(yield* Plugin.Service, "test"))
    const hooks = yield* PluginHooks.Service
    const results = yield* Effect.all(
      ["chatgpt-token-sharing", null].map((method) =>
        withRequestAuth(
          hooks.trigger("session", "retry", {
            sessionID: Session.ID.make("ses_retry_auth"),
            agent: Agent.ID.make("build"),
            model: Model.Ref.make({ providerID: Provider.ID.openai, id: Model.ID.make("gpt-6-astra") }),
            error: {
              type: "provider.rate-limit",
              message: "fixture",
              status: 429,
              response: { body: '{"error":{"code":"subscription_sharing_usage_limit_exceeded"}}' },
            },
            attempt: 2,
            decision: { retry: true, delay: 1000 },
          }),
          { integrationID: "openai", method },
        ),
      ),
      { concurrency: "unbounded" },
    )
    expect(results.map((event) => event.decision.retry)).toEqual([false, true])
  }),
)

it.effect("routes concurrent hooks by captured request auth rather than the active legacy account", () =>
  Effect.gen(function* () {
    const credentials = yield* Credential.Service
    yield* credentials.create({
      integrationID: Integration.ID.make("openai"),
      value: Credential.OAuth.make({
        type: "oauth",
        methodID: Integration.MethodID.make("chatgpt-headless"),
        access: "fixture",
        refresh: "fixture",
        expires: 0,
        metadata: { accountID: "fixture" },
      }),
    })
    yield* OpenAIPlugin.effect(yield* PluginHost.make(yield* Plugin.Service, "test"))
    const hooks = yield* PluginHooks.Service
    const results = yield* Effect.all(
      [null, "chatgpt-headless", "chatgpt-token-sharing"].map((method) =>
        withRequestAuth(
          hooks.trigger("session", "model.request", {
            sessionID: Session.ID.make("ses_fixture"),
            agent: Agent.ID.make("build"),
            model: Model.Ref.make({ providerID: Provider.ID.openai, id: Model.ID.make("gpt-6-astra") }),
            kind: "primary",
            baseURL: "https://api.openai.com/v1",
            headers: {},
          }),
          { integrationID: "openai", method },
        ),
      ),
      { concurrency: "unbounded" },
    )
    expect(results[0]?.baseURL).toBe("https://api.openai.com/v1")
    expect(results[1]?.baseURL).toBe("https://chatgpt.com/backend-api/codex")
    expect(results[2]?.baseURL).toBe("https://api.openai.com/v1")
    expect(results[0]?.headers.originator).toBeUndefined()
    expect(results[1]?.headers.originator).toBe("opencode")
    expect(results[2]?.headers.originator).toBeUndefined()
  }),
)

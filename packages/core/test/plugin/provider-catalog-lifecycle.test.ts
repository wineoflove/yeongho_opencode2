import { expect } from "bun:test"
import { Effect, Fiber, Stream } from "effect"
import { Bus } from "@opencode/core/bus"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { OpenAIPlugin } from "@opencode/core/plugin/provider/openai"
import { ChatGPTPlugin } from "@opencode/core/plugin/provider/chatgpt"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(PluginTestLayer)

for (const change of ["activate", "update"] as const) {
  it.live(`publishes a coherent new catalog after ${change} changes the active OAuth method`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const providers = yield* Provider.Service
      const models = yield* Model.Service
      const bus = yield* Bus.Service
      const integrationID = Integration.ID.make("openai")
      const modelID = Model.ID.make("gpt-6-astra")
      const accounts = yield* Effect.forEach(["chatgpt-headless", "chatgpt-token-sharing"], (method) =>
        credentials.create({
          integrationID,
          activate: method === "chatgpt-headless",
          value: Credential.OAuth.make({
            type: "oauth",
            methodID: Integration.MethodID.make(method),
            access: "fixture",
            refresh: "fixture",
            expires: Date.now() + 3600000,
            metadata: { accountID: "fixture", clientID: "fixture", scopes: [] },
          }),
        }),
      )
      yield* providers.transform((catalog) => {
        catalog.update(Provider.ID.openai, (draft) => {
          draft.activation = "enabled"
          draft.package = "@opencode/ai/providers/openai"
        })
        catalog.models.update(Provider.ID.openai, modelID, (draft) => {
          draft.enabled = true
        })
      })
      const host = yield* PluginHost.make(yield* Plugin.Service, "test")
      yield* OpenAIPlugin.effect(host)
      yield* ChatGPTPlugin.effect(host)
      const before = yield* models.catalog(Provider.ID.openai, modelID)
      expect(before?.definition.catalogAuth?.method).toBe("chatgpt-headless")
      const events = bus.subscribe(Provider.Event.Updated)
      const observed = yield* events.pipe(
        Stream.mapEffect(() => models.catalog(Provider.ID.openai, modelID)),
        Stream.filter((entry) => entry?.definition.catalogAuth?.method === "chatgpt-token-sharing"),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      )
      const target = accounts[change === "activate" ? 1 : 0]
      if (!target) return yield* Effect.die("Missing target account")
      if (change === "activate") yield* credentials.activate(target.id)
      else
        yield* credentials.update(target.id, {
          value: Credential.OAuth.make({
            type: "oauth",
            methodID: Integration.MethodID.make("chatgpt-token-sharing"),
            access: "updated",
            refresh: "fixture",
            expires: Date.now() + 3600000,
            metadata: { clientID: "fixture", scopes: [] },
          }),
        })
      const entries = yield* Fiber.join(observed).pipe(Effect.timeout("5 seconds"))
      const current = entries[0]
      expect(current?.definition.catalogAuth?.sourceKey).toBe(`credential:${target.id}`)
      expect(current?.definition.provider.settings?.baseURL).toBe("https://api.openai.com/v1")
      expect(before?.definition.catalogAuth?.method).toBe("chatgpt-headless")
      expect(before?.definition.provider.settings?.baseURL).toBe("https://chatgpt.com/backend-api/codex")
    }),
  )
}

for (const method of ["chatgpt-headless", "chatgpt-token-sharing"]) {
  it.effect(`removes catalog auth and restores model policy when ${method} plugins leave their scope`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const providers = yield* Provider.Service
      const models = yield* Model.Service
      const integrationID = Integration.ID.make("openai")
      const modelID = Model.ID.make("gpt-6-astra")
      yield* credentials.create({
        integrationID,
        value: Credential.OAuth.make({
          type: "oauth",
          methodID: Integration.MethodID.make(method),
          access: "fixture-access",
          refresh: "fixture-refresh",
          expires: Date.now() + 3600000,
          metadata: { accountID: "fixture", clientID: "fixture", scopes: [] },
        }),
      })
      yield* providers.transform((catalog) => {
        catalog.update(Provider.ID.openai, (draft) => {
          draft.activation = "enabled"
          draft.package = "@opencode/ai/providers/openai"
          draft.settings = { baseURL: "https://api.openai.com/v1" }
        })
        catalog.models.update(Provider.ID.openai, modelID, (draft) => {
          draft.enabled = true
        })
      })
      const before = yield* models.catalog(Provider.ID.openai, modelID)
      if (!before) return yield* Effect.die("Missing baseline catalog")
      const saved = yield* credentials.all()
      yield* Effect.gen(function* () {
        const host = yield* PluginHost.make(yield* Plugin.Service, "test")
        yield* OpenAIPlugin.effect(host)
        yield* ChatGPTPlugin.effect(host)
        const loaded = yield* models.catalog(Provider.ID.openai, modelID)
        expect(loaded?.definition.catalogAuth?.method).toBe(method)
        expect(loaded?.definition.provider.settings?.baseURL).toBe(
          method === "chatgpt-headless" ? "https://chatgpt.com/backend-api/codex" : "https://api.openai.com/v1",
        )
      }).pipe(Effect.scoped)
      const after = yield* models.catalog(Provider.ID.openai, modelID)
      expect(after?.definition.catalogAuth).toBeUndefined()
      expect(after?.definition.provider).toEqual(before.definition.provider)
      expect(after?.model).toEqual(before.model)
      expect(yield* credentials.all()).toEqual(saved)
    }),
  )
}

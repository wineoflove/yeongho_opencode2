import { expect, spyOn } from "bun:test"
import { Effect, Layer } from "effect"
import { ai } from "@opencode/ai/effect"
import { Headers } from "effect/http"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Model } from "@opencode/core/model"
import { ModelResolver } from "@opencode/core/model-resolver"
import { AccountPool } from "@opencode/core/account-pool"
import { Provider } from "@opencode/core/provider"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { OpenAIPlugin } from "@opencode/core/plugin/provider/openai"
import { ChatGPTPlugin } from "@opencode/core/plugin/provider/chatgpt"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(
  ModelResolver.layer.pipe(Layer.provideMerge(Layer.merge(PluginTestLayer, LayerNode.compile(AccountPool.node)))),
)
const value = (method: string | null) =>
  method === null
    ? Credential.Key.make({ type: "key", key: "fixture-key" })
    : Credential.OAuth.make({
        type: "oauth",
        methodID: Integration.MethodID.make(method),
        access: "expired",
        refresh: "fixture",
        expires: 0,
        metadata: { accountID: "fixture", clientID: "fixture-client", scopes: [] },
      })

for (const method of ["chatgpt-headless", "chatgpt-token-sharing"]) {
  it.effect(`rotates distinct ${method} accounts without requiring the catalog source account`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const integrations = yield* Integration.Service
      const providers = yield* Provider.Service
      const models = yield* Model.Service
      const resolver = yield* ModelResolver.Service
      const integrationID = Integration.ID.make("openai")
      const accounts = yield* Effect.forEach(["A", "B"], (account) =>
        credentials.create({
          integrationID,
          activate: account === "B",
          value: Credential.OAuth.make({
            type: "oauth",
            methodID: Integration.MethodID.make(method),
            access: `fixture-token-${account}`,
            refresh: `fixture-refresh-${account}`,
            expires: Date.now() + 3600000,
            metadata: { accountID: `account-${account}`, clientID: `client-${account}`, scopes: [] },
          }),
        }),
      )
      const host = yield* PluginHost.make(yield* Plugin.Service)
      yield* OpenAIPlugin.effect(host)
      yield* ChatGPTPlugin.effect(host)
      const modelID = Model.ID.make("gpt-6-astra")
      yield* providers.transform((catalog) => {
        catalog.update(Provider.ID.openai, (draft) => {
          draft.activation = "enabled"
          draft.package = "@opencode/ai/providers/openai"
          draft.settings = {
            ...draft.settings,
            accountPool: {
              strategy: "round-robin",
              accounts: accounts.map(({ id }) => ({ credentialID: id, enabled: true })),
            },
          }
        })
        catalog.models.update(Provider.ID.openai, modelID, (draft) => {
          draft.enabled = true
        })
      })
      const entry = yield* models.catalog(Provider.ID.openai, modelID)
      if (!entry) return yield* Effect.die("Expected fixture catalog")
      const before = yield* credentials.all()
      const active = yield* integrations.connection.active(integrationID)
      const headers = yield* Effect.forEach([0, 1, 2, 3], () =>
        Effect.gen(function* () {
          const resolved = yield* resolver.resolveModel(entry.model, undefined, { useAccountPool: true })
          expect(resolved.model.route.endpoint.baseURL).toBe(
            method === "chatgpt-headless" ? "https://chatgpt.com/backend-api/codex" : "https://api.openai.com/v1",
          )
          return yield* resolved.model.route.auth.apply({
            request: ai.request({ model: resolved.model, prompt: "Fixture" }),
            method: "POST",
            url: "https://fixture.invalid/v1/responses",
            body: "{}",
            headers: Headers.fromInput(resolved.model.route.defaults.headers),
          })
        }),
      )
      expect(headers.map((item) => item.authorization)).toEqual([
        "Bearer fixture-token-A",
        "Bearer fixture-token-B",
        "Bearer fixture-token-A",
        "Bearer fixture-token-B",
      ])
      expect(
        headers.map((item) => (Object.hasOwn(item, "chatgpt-account-id") ? item["chatgpt-account-id"] : null)),
      ).toEqual(
        method === "chatgpt-headless" ? ["account-A", "account-B", "account-A", "account-B"] : [null, null, null, null],
      )
      expect(yield* credentials.all()).toEqual(before)
      expect(yield* integrations.connection.active(integrationID)).toEqual(active)
    }),
  )
}

for (const [active, selectedMethod] of [
  ["chatgpt-headless", null],
  [null, "chatgpt-headless"],
  ["chatgpt-headless", "chatgpt-token-sharing"],
  ["chatgpt-token-sharing", "chatgpt-headless"],
]) {
  it.effect(`rejects catalog ${active} with pool ${selectedMethod} before token exchange`, () =>
    Effect.gen(function* () {
      let exchanges = 0
      const intercepted = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(
          async () => {
            exchanges++
            return Response.json({ error: "unexpected" }, { status: 401 })
          },
          { preconnect: globalThis.fetch.preconnect },
        ),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => intercepted.mockRestore()))
      const credentials = yield* Credential.Service
      const providers = yield* Provider.Service
      const models = yield* Model.Service
      const resolver = yield* ModelResolver.Service
      const integrationID = Integration.ID.make("openai")
      const selected = yield* credentials.create({ integrationID, value: value(selectedMethod ?? null) })
      yield* credentials.create({ integrationID, value: value(active ?? null), activate: true })
      const host = yield* PluginHost.make(yield* Plugin.Service)
      yield* OpenAIPlugin.effect(host)
      yield* ChatGPTPlugin.effect(host)
      const modelID = Model.ID.make("gpt-6-astra")
      yield* providers.transform((catalog) => {
        catalog.update(Provider.ID.openai, (draft) => {
          draft.activation = "enabled"
          draft.settings = {
            ...draft.settings,
            accountPool: { strategy: "round-robin", accounts: [{ credentialID: selected.id, enabled: true }] },
          }
        })
        catalog.models.update(Provider.ID.openai, modelID, (draft) => {
          draft.enabled = true
        })
      })
      const catalog = yield* models.catalog(Provider.ID.openai, modelID)
      if (!catalog) return yield* Effect.die("Expected available fixture catalog")
      expect(catalog.definition.catalogAuth?.method).toBe(active ?? null)
      for (const options of [{ useAccountPool: true }, { credentialID: selected.id }]) {
        const failure = yield* resolver.resolveModel(catalog.model, undefined, options).pipe(Effect.flip)
        expect(failure._tag).toBe("Integration.Authorization")
        if (failure._tag === "Integration.Authorization")
          expect(failure.cause).toBe("Selected authentication does not match the model catalog")
      }
      expect(exchanges).toBe(0)
    }).pipe(Effect.scoped),
  )
}

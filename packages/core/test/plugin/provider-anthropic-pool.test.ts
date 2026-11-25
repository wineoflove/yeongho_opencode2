import { expect, spyOn } from "bun:test"
import { Effect, Layer } from "effect"
import { ai } from "@opencode/ai/effect"
import { Headers } from "effect/http"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { ModelResolver } from "@opencode/core/model-resolver"
import { AccountPool } from "@opencode/core/account-pool"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { AnthropicOAuthPlugin } from "@opencode/core/plugin/provider/anthropic-oauth"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { PluginTestLayer } from "./fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(
  ModelResolver.layer.pipe(Layer.provideMerge(Layer.merge(PluginTestLayer, LayerNode.compile(AccountPool.node)))),
)

for (const status of [200, 400, 403]) {
  it.effect(`binds native Anthropic OAuth refresh ${status} to the selected pool account`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const integrations = yield* Integration.Service
      const providers = yield* Provider.Service
      const models = yield* Model.Service
      const resolver = yield* ModelResolver.Service
      const pool = yield* AccountPool.Service
      const plugin = yield* Plugin.Service
      const host = yield* PluginHost.make(plugin, "test")
      yield* AnthropicOAuthPlugin.effect(host)
      const integrationID = Integration.ID.make("anthropic")
      const providerID = Provider.ID.make("anthropic")
      const modelID = Model.ID.make("fixture-claude")
      const accounts = yield* Effect.forEach(["A", "B"], (id) =>
        credentials.create({
          integrationID,
          label: id,
          value: Credential.OAuth.make({
            type: "oauth",
            methodID: Integration.MethodID.make("anthropic-oauth"),
            access: `fixture-${id}`,
            refresh: `refresh-${id}`,
            expires: id === "A" ? 1 : Date.now() + 3600000,
          }),
        }),
      )
      const active = yield* integrations.connection.active(integrationID)
      const config = {
        strategy: "round-robin" as const,
        accounts: accounts.map(({ id }) => ({ credentialID: id, enabled: true })),
      }
      yield* providers.transform((catalog) => {
        catalog.update(providerID, (draft) => {
          draft.activation = "enabled"
          draft.package = "@opencode/ai/providers/anthropic"
          draft.settings = { baseURL: "http://127.0.0.1:1/v1", accountPool: config }
        })
        catalog.models.update(providerID, modelID, (draft) => {
          draft.enabled = true
        })
      })
      const entry = yield* models.catalog(providerID, modelID)
      if (!entry) return yield* Effect.die("Missing fixture model")
      const mock = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(
          async (input: string | URL | Request, init?: RequestInit) => {
            expect(String(input)).toBe("https://platform.claude.com/v1/oauth/token")
            expect(JSON.parse(String(init?.body)).refresh_token).toBe("refresh-A")
            return Response.json(
              status === 200
                ? { access_token: "renewed-A", refresh_token: "rotated-A", expires_in: 3600 }
                : { error: "invalid_grant", error_description: "private-fixture" },
              { status },
            )
          },
          { preconnect: globalThis.fetch.preconnect },
        ),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
      const resolve = () => resolver.resolveModel(entry.model, undefined, { useAccountPool: true })
      if (status !== 200) {
        expect((yield* resolve().pipe(Effect.flip))._tag).toBe("Integration.Authorization")
        const state = yield* pool.status(integrationID, config)
        expect(state.paused).toBe(status === 403)
        expect(state.accounts[0]?.authInvalid).toBe(status === 400)
        if (status === 403) {
          expect((yield* resolve().pipe(Effect.flip))._tag).toBe("Integration.Authorization")
          expect(mock).toHaveBeenCalledTimes(1)
          return
        }
      }
      const tokens = yield* Effect.forEach(status === 200 ? [0, 1, 2] : [0, 1], () =>
        Effect.gen(function* () {
          const resolved = yield* resolve()
          const headers = yield* resolved.model.route.auth.apply({
            request: ai.request({ model: resolved.model, prompt: "Fixture" }),
            method: "POST",
            url: "http://127.0.0.1:1/v1/messages",
            body: "{}",
            headers: Headers.fromInput(resolved.model.options?.headers),
          })
          expect(Object.hasOwn(headers, "x-api-key")).toBe(false)
          expect(headers["anthropic-beta"]?.split(",")).toContain("oauth-2025-04-20")
          return headers.authorization
        }),
      )
      expect(tokens).toEqual(
        status === 200
          ? ["Bearer renewed-A", "Bearer fixture-B", "Bearer renewed-A"]
          : ["Bearer fixture-B", "Bearer fixture-B"],
      )
      if (status === 200) {
        const second = accounts[1]
        if (!second || active?.type !== "credential") return yield* Effect.die("Missing fixture accounts")
        const stored = yield* credentials.get(active.id)
        if (stored?.value.type !== "oauth") return yield* Effect.die("Missing active OAuth credential")
        for (const selection of [
          { options: { credentialID: second.id }, token: "fixture-B" },
          { options: undefined, token: stored.value.access },
        ]) {
          const resolved = yield* resolver.resolveModel(entry.model, undefined, selection.options)
          const headers = yield* resolved.model.route.auth.apply({
            request: ai.request({ model: resolved.model, prompt: "Fixture" }),
            method: "POST",
            url: "http://127.0.0.1:1/v1/messages",
            body: "{}",
            headers: Headers.fromInput({ "X-API-Key": "stale-key", "Anthropic-Beta": "existing-beta" }),
          })
          expect(headers.authorization).toBe(`Bearer ${selection.token}`)
          expect(headers["x-api-key"]).toBeUndefined()
          expect(headers["anthropic-beta"]).toBe("existing-beta,oauth-2025-04-20")
        }
      }
      expect(mock).toHaveBeenCalledTimes(1)
      expect(yield* integrations.connection.active(integrationID)).toEqual(active)
      const other = accounts[1]
      if (!other) return yield* Effect.die("Missing second account")
      expect(yield* credentials.get(other.id)).toEqual(other)
    }).pipe(Effect.scoped),
  )
}

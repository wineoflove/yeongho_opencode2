import { expect, spyOn } from "bun:test"
import { Effect, Layer } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { ModelResolver } from "@opencode/core/model-resolver"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { CredentialRequestError } from "@opencode/core/credential-request-error"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
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

for (const provider of [
  { plugin: OpenAIPlugin, method: "chatgpt-headless", endpoint: "https://auth.openai.com/oauth/token" },
  {
    plugin: ChatGPTPlugin,
    method: "chatgpt-token-sharing",
    endpoint: "https://auth.openai.com/api/accounts/oauth/token",
  },
]) {
  it.live(`isolates successful concurrent refreshes through ${provider.method}`, () =>
    Effect.gen(function* () {
      const exchanges: string[] = []
      const arrived = new Set<string>()
      const barrier = Promise.withResolvers<void>()
      const intercepted = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(
          async (input: string | URL | Request, init?: RequestInit) => {
            expect(input instanceof Request ? input.url : String(input)).toBe(provider.endpoint)
            const body = new URLSearchParams(typeof init?.body === "string" ? init.body : "")
            const refresh = body.get("refresh_token")
            expect(refresh === "fixture-refresh-A" || refresh === "fixture-refresh-B").toBe(true)
            const account = refresh === "fixture-refresh-A" ? "A" : "B"
            if (provider.method === "chatgpt-token-sharing")
              expect(body.get("client_id")).toBe(`fixture-client-${account}`)
            exchanges.push(account)
            arrived.add(account)
            if (arrived.size === 2) barrier.resolve()
            await barrier.promise
            return Response.json({
              id_token: "fixture.e30.signature",
              access_token: `renewed-${account}`,
              refresh_token: `rotated-${account}`,
              expires_in: 3600,
            })
          },
          { preconnect: globalThis.fetch.preconnect },
        ),
      )
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          barrier.resolve()
          intercepted.mockRestore()
        }),
      )
      const credentials = yield* Credential.Service
      const integrations = yield* Integration.Service
      const integrationID = Integration.ID.make("openai")
      const saved = yield* Effect.forEach(["A", "B"], (account) =>
        credentials.create({
          integrationID,
          value: Credential.OAuth.make({
            type: "oauth",
            methodID: Integration.MethodID.make(provider.method),
            access: `expired-${account}`,
            refresh: `fixture-refresh-${account}`,
            expires: 0,
            metadata: {
              accountID: `fixture-account-${account}`,
              clientID: `fixture-client-${account}`,
              scopes: ["fixture-scope"],
            },
          }),
        }),
      )
      yield* provider.plugin.effect(yield* PluginHost.make(yield* Plugin.Service))
      expect(exchanges).toEqual([])
      const active = yield* integrations.connection.active(integrationID)
      const connections = (yield* integrations.get(integrationID))?.connections
      const first = connections?.find((entry) => entry.type === "credential" && entry.id === saved[0]?.id)
      const second = connections?.find((entry) => entry.type === "credential" && entry.id === saved[1]?.id)
      if (!first || !second) return yield* Effect.die("Fixture connections missing")
      const resolved = yield* Effect.all(
        [first, first, second].map((connection) => integrations.connection.resolve(connection)),
        {
          concurrency: "unbounded",
        },
      ).pipe(Effect.timeout("10 seconds"))
      expect(exchanges.toSorted()).toEqual(["A", "B"])
      expect(resolved.map((value) => (value?.type === "oauth" ? value.access : undefined))).toEqual([
        "renewed-A",
        "renewed-A",
        "renewed-B",
      ])
      for (const [index, account] of ["A", "B"].entries()) {
        const entry = saved[index]
        if (!entry) return yield* Effect.die("Fixture credential missing")
        const current = yield* credentials.get(entry.id)
        expect(current?.value).toMatchObject({
          type: "oauth",
          access: `renewed-${account}`,
          refresh: `rotated-${account}`,
          metadata:
            provider.method === "chatgpt-token-sharing"
              ? { clientID: `fixture-client-${account}`, scopes: ["fixture-scope"] }
              : {
                  accountID: `fixture-account-${account}`,
                  clientID: `fixture-client-${account}`,
                  scopes: ["fixture-scope"],
                },
        })
      }
      expect(yield* integrations.connection.active(integrationID)).toEqual(active)
    }).pipe(Effect.scoped),
  )
  for (const status of [400, 401, 403, 429, 503]) {
    it.effect(`preserves refresh HTTP ${status} through ${provider.method}`, () =>
      Effect.gen(function* () {
        const calls: { url: string; refresh: string | null }[] = []
        const intercepted = spyOn(globalThis, "fetch").mockImplementation(
          Object.assign(
            async (input: string | URL | Request, init?: RequestInit) => {
              const url = input instanceof Request ? input.url : String(input)
              expect(url).toBe(provider.endpoint)
              expect(init?.method).toBe("POST")
              expect(typeof init?.body).toBe("string")
              const body = new URLSearchParams(typeof init?.body === "string" ? init.body : "")
              calls.push({ url, refresh: body.get("refresh_token") })
              return Response.json(
                {
                  error: status === 400 ? "invalid_grant" : "fixture-private-error",
                  error_description: "fixture-private-error",
                },
                { status },
              )
            },
            { preconnect: globalThis.fetch.preconnect },
          ),
        )
        yield* Effect.addFinalizer(() => Effect.sync(() => intercepted.mockRestore()))
        const credentials = yield* Credential.Service
        const resolver = yield* ModelResolver.Service
        const providers = yield* Provider.Service
        const pool = yield* AccountPool.Service
        const integrationID = Integration.ID.make("openai")
        const account = yield* credentials.create({
          integrationID,
          value: Credential.OAuth.make({
            type: "oauth",
            methodID: Integration.MethodID.make(provider.method),
            access: "fixture-expired",
            refresh: "fixture-refresh-A",
            expires: 0,
            metadata: { accountID: "fixture-account-A", clientID: "fixture-client-A", scopes: [] },
          }),
        })
        const before = yield* credentials.get(account.id)
        yield* provider.plugin.effect(yield* PluginHost.make(yield* Plugin.Service))
        expect(calls).toEqual([])
        yield* providers.transform((catalog) => {
          catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-6-astra"), (draft) => {
            draft.enabled = true
          })
          catalog.update(Provider.ID.openai, (draft) => {
            draft.activation = "enabled"
            draft.settings = {
              ...draft.settings,
              accountPool: {
                strategy: "round-robin",
                accounts: [{ credentialID: account.id, enabled: true }],
              },
            }
          })
        })
        const selected = Model.Info.make({
          id: Model.ID.make("gpt-6-astra"),
          modelID: Model.ID.make("gpt-6-astra"),
          providerID: Provider.ID.openai,
          name: "Fixture model",
          package: "@opencode/ai/providers/openai",
          headers: {},
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          variants: [],
          time: { released: 0 },
          cost: [],
          status: "active",
          enabled: true,
          limit: { context: 1000, output: 100 },
        })
        const failure = yield* resolver.resolveModel(selected, undefined, { useAccountPool: true }).pipe(Effect.flip)
        expect(failure._tag).toBe("Integration.Authorization")
        if (failure._tag !== "Integration.Authorization") return yield* Effect.die("Unexpected failure type")
        expect(failure.cause).toBeInstanceOf(CredentialRequestError)
        if (failure.cause instanceof CredentialRequestError) expect(failure.cause.status).toBe(status)
        expect(calls).toEqual([{ url: provider.endpoint, refresh: "fixture-refresh-A" }])
        expect(yield* credentials.get(account.id)).toEqual(before)
        const state = yield* pool.status(integrationID, {
          strategy: "round-robin",
          accounts: [{ credentialID: account.id, enabled: true }],
        })
        expect(state.paused).toBe(status === 403)
        expect(state.accounts[0]?.authInvalid).toBe(status === 400 || status === 401)
        expect(JSON.stringify(state)).not.toContain("fixture-private-error")
        if (status === 403) {
          const blocked = yield* resolver.resolveModel(selected, undefined, { useAccountPool: true }).pipe(Effect.flip)
          expect(blocked._tag).toBe("Integration.Authorization")
          if (blocked._tag === "Integration.Authorization")
            expect(blocked.cause).toBeInstanceOf(AccountPool.PausedError)
          expect(calls).toHaveLength(1)
        }
      }).pipe(Effect.scoped),
    )
  }
}

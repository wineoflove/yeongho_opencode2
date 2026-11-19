import { describe, expect } from "bun:test"
import { ai } from "@opencode/ai/effect"
import { Headers } from "effect/http"
import { Effect, Layer } from "effect"
import { AISDK } from "@opencode/core/aisdk"
import { AccountPool } from "@opencode/core/account-pool"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { IntegrationConnection } from "@opencode/core/integration/connection"
import { Model } from "@opencode/core/model"
import { ModelResolver } from "@opencode/core/model-resolver"
import { Provider } from "@opencode/core/provider"
import { Npm } from "@opencode/util/npm"
import { it } from "./lib/effect"

const integrationID = Integration.ID.make("account-fixture")
const first = Credential.ID.make("cred_first")
const second = Credential.ID.make("cred_second")
const selected = Model.Info.make({
  id: Model.ID.make("fixture-model"),
  modelID: Model.ID.make("fixture-model"),
  providerID: Provider.ID.make("fixture-provider"),
  name: "Fixture model",
  package: "@opencode/ai/providers/openai-compatible",
  settings: { baseURL: "http://127.0.0.1:1/v1" },
  headers: {},
  capabilities: { tools: true, input: ["text"], output: ["text"] },
  variants: [],
  time: { released: 0 },
  cost: [],
  status: "active",
  enabled: true,
  limit: { context: 1000, output: 100 },
})

function fixture(
  options: {
    readonly failActive?: boolean
    readonly disappear?: boolean
    readonly openaiOAuth?: boolean
    readonly external?: boolean
  } = {},
) {
  const targetID = options.openaiOAuth ? Integration.ID.make("openai") : integrationID
  const resolved: string[] = []
  let activeLookups = 0
  const connections = [first, second].map((id) =>
    IntegrationConnection.CredentialInfo.make({
      type: "credential",
      id,
      label: id,
      method: options.external ? "external" : options.openaiOAuth ? "oauth" : "key",
    }),
  )
  const layer = ModelResolver.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(Provider.Service, {
          get: () =>
            Effect.succeed(
              Provider.Info.make({
                ...Provider.Info.empty(selected.providerID),
                integrationID: targetID,
                activation: "enabled",
                package: selected.package ?? "",
                settings: {
                  ...selected.settings,
                  accountPool: {
                    strategy: "round-robin",
                    accounts: [
                      { credentialID: first, enabled: true },
                      { credentialID: second, enabled: true },
                    ],
                  },
                },
              }),
            ),
        }),
        Layer.mock(Model.Service, {
          get: () => Effect.succeed(selected),
          catalog: () =>
            Effect.succeed({
              model: selected,
              definition: {
                catalogAuth: {
                  integrationID: targetID,
                  sourceKey: `credential:${first}`,
                  method: options.openaiOAuth ? "chatgpt-headless" : null,
                },
                provider: Provider.Info.make({
                  ...Provider.Info.empty(selected.providerID),
                  integrationID: targetID,
                  activation: "enabled",
                  package: selected.package ?? "",
                  settings: {
                    ...selected.settings,
                    accountPool: {
                      strategy: "round-robin",
                      accounts: [first, second].map((credentialID) => ({ credentialID, enabled: true })),
                    },
                  },
                }),
                models: new Map([[selected.id, selected]]),
              },
            }),
        }),
        Layer.mock(Credential.Service, {
          get: (id) =>
            Effect.succeed(
              Credential.Info.make({
                id,
                integrationID: targetID,
                label: id,
                value: options.openaiOAuth
                  ? Credential.OAuth.make({
                      type: "oauth",
                      methodID: Integration.MethodID.make("chatgpt-headless"),
                      access: `token-${id}`,
                      refresh: "fixture-refresh",
                      expires: 1,
                      metadata: { accountID: `account-${id}` },
                    })
                  : Credential.Key.make({ type: "key", key: `token-${id}` }),
              }),
            ),
          list: () =>
            Effect.succeed(
              [first, second].map((id) =>
                Credential.Info.make({
                  id,
                  integrationID: targetID,
                  label: id,
                  value: options.external
                    ? Credential.External.make({
                        type: "external",
                        methodID: Integration.MethodID.make("fixture-login"),
                      })
                    : options.openaiOAuth
                      ? Credential.OAuth.make({
                          type: "oauth",
                          methodID: Integration.MethodID.make("chatgpt-headless"),
                          access: `token-${id}`,
                          refresh: "fixture-refresh",
                          expires: 1,
                          metadata: { accountID: `account-${id}` },
                        })
                      : Credential.Key.make({ type: "key", key: `token-${id}` }),
                }),
              ),
            ),
        }),
        Layer.mock(Integration.Service, {
          revision: () => 0,
          oauth: {
            connect: () => Effect.die("No authentication changes allowed"),
            status: () => Effect.die("No authentication changes allowed"),
            complete: () => Effect.die("No authentication changes allowed"),
            cancel: () => Effect.die("No authentication changes allowed"),
          },
          command: {
            connect: () => Effect.die("No authentication changes allowed"),
            status: () => Effect.die("No authentication changes allowed"),
            cancel: () => Effect.die("No authentication changes allowed"),
          },
          get: (id) => Effect.succeed(id === targetID ? { id, name: "Fixture", methods: [], connections } : undefined),
          connection: {
            active: () =>
              Effect.sync(() => {
                activeLookups++
                return connections[1]
              }),
            resolve: (connection) => {
              if (connection.type !== "credential") return Effect.die("Unexpected environment connection")
              resolved.push(connection.id)
              if (options.disappear) return Effect.undefined
              if (connection.id === second && options.failActive)
                return Effect.fail(new Integration.AuthorizationError({ cause: new Error("fixture refresh failed") }))
              return Effect.succeed(
                options.openaiOAuth
                  ? Credential.OAuth.make({
                      type: "oauth",
                      methodID: Integration.MethodID.make("chatgpt-headless"),
                      access: `token-${connection.id}`,
                      refresh: "fixture-refresh",
                      expires: 1,
                      metadata: { accountID: `account-${connection.id}` },
                    })
                  : Credential.Key.make({ type: "key", key: `token-${connection.id}` }),
              )
            },
            key: () => Effect.die("No account writes allowed"),
            external: () => Effect.die("No account writes allowed"),
            activate: () => Effect.die("No global activation allowed"),
            update: () => Effect.die("No account writes allowed"),
            remove: () => Effect.die("No account writes allowed"),
            status: () => Effect.die("No status writes allowed"),
          },
        }),
        Layer.mock(Npm.Service, {}),
        LayerNode.compile(AccountPool.node),
        Layer.mock(AISDK.Service, {
          hook: {
            sdk: () => Effect.die("Native provider only"),
            language: () => Effect.die("Native provider only"),
          },
        }),
      ),
    ),
  )
  return { layer, resolved, activeLookups: () => activeLookups }
}

const authorization = (resolved: ModelResolver.Resolved) =>
  resolved.model.route.auth.apply({
    request: ai.request({ model: resolved.model, prompt: "Fixture" }),
    method: "POST",
    url: "http://127.0.0.1:1/v1/chat/completions",
    body: "{}",
    headers: Headers.fromInput(resolved.model.route.defaults.headers),
  })

describe("request-local model credentials", () => {
  it.effect("previews pool context without refreshing or advancing the next real execution", () => {
    const test = fixture()
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      yield* resolver.resolveModel(selected, undefined, { useAccountPool: true, advanceAccountPool: false })
      yield* resolver.resolveModel(selected, undefined, { useAccountPool: true, advanceAccountPool: false })
      expect(test.resolved).toEqual([])
      const actual = yield* resolver.resolveModel(selected, undefined, { useAccountPool: true })
      expect((yield* authorization(actual)).authorization).toBe(`Bearer token-${first}`)
      expect(test.resolved).toEqual([first])
    }).pipe(Effect.provide(test.layer))
  })
  it.effect("does not mistake multiple external login markers for isolated pool accounts", () => {
    const test = fixture({ external: true })
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const error = yield* resolver.resolveModel(selected, undefined, { useAccountPool: true }).pipe(Effect.flip)
      expect(error._tag).toBe("Integration.Authorization")
      expect(test.resolved).toEqual([])
      expect(test.activeLookups()).toBe(0)
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("rotates opted-in executions without consuming a slot for ordinary model lookup", () => {
    const test = fixture()
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const resolved = yield* Effect.forEach([0, 1, 2, 3], () =>
        Effect.gen(function* () {
          yield* resolver.resolveModel(selected)
          const model = yield* resolver.resolveModel(selected, undefined, { useAccountPool: true })
          return (yield* authorization(model)).authorization
        }),
      )
      expect(resolved).toEqual([
        `Bearer token-${first}`,
        `Bearer token-${second}`,
        `Bearer token-${first}`,
        `Bearer token-${second}`,
      ])
      expect(test.activeLookups()).toBe(4)
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("pairs the outgoing token and account ID for each concurrent OpenAI binding", () => {
    const test = fixture({ openaiOAuth: true })
    const catalog = Model.Info.make({ ...selected, headers: { "ChatGPT-Account-ID": "stale-global-account" } })
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const models = yield* Effect.all(
        [first, second].map((credentialID) => resolver.resolveModel(catalog, undefined, { credentialID })),
        { concurrency: "unbounded" },
      )
      const requests = yield* Effect.all(models.map(authorization))
      expect(requests.map((headers) => [headers.authorization, headers["chatgpt-account-id"]])).toEqual([
        [`Bearer token-${first}`, `account-${first}`],
        [`Bearer token-${second}`, `account-${second}`],
      ])
      expect(catalog.headers?.["ChatGPT-Account-ID"]).toBe("stale-global-account")
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("resolves the selected account without refreshing the broken global active account", () => {
    const test = fixture({ failActive: true })
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const model = yield* resolver.resolveModel(selected, undefined, { credentialID: first })
      expect((yield* authorization(model)).authorization).toBe(`Bearer token-${first}`)
      expect(test.resolved).toEqual([first])
      expect(test.activeLookups()).toBe(0)
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("retains default active-account resolution when no request binding is supplied", () => {
    const test = fixture()
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const model = yield* resolver.resolve(selected)
      if (!model) return yield* Effect.die("Fixture model missing")
      expect((yield* authorization(model)).authorization).toBe(`Bearer token-${second}`)
      expect(test.activeLookups()).toBe(1)
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("keeps two concurrent request bindings isolated", () => {
    const test = fixture()
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const models = yield* Effect.all(
        [first, second].map((credentialID) => resolver.resolveModel(selected, undefined, { credentialID })),
        { concurrency: "unbounded" },
      )
      const headers = yield* Effect.all(models.map(authorization))
      expect(headers.map((entry) => entry.authorization)).toEqual([`Bearer token-${first}`, `Bearer token-${second}`])
      expect(test.activeLookups()).toBe(0)
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("rejects a foreign credential before resolving it instead of falling back", () => {
    const test = fixture()
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const error = yield* resolver
        .resolveModel(selected, undefined, { credentialID: Credential.ID.make("cred_foreign") })
        .pipe(Effect.flip)
      expect(error._tag).toBe("Integration.Authorization")
      if (error._tag === "Integration.Authorization")
        expect(error.cause).toBeInstanceOf(ModelResolver.CredentialUnavailableError)
      expect(test.resolved).toEqual([])
      expect(test.activeLookups()).toBe(0)
    }).pipe(Effect.provide(test.layer))
  })

  it.effect("fails closed when the bound credential disappears during resolution", () => {
    const test = fixture({ disappear: true })
    return Effect.gen(function* () {
      const resolver = yield* ModelResolver.Service
      const error = yield* resolver.resolveModel(selected, undefined, { credentialID: first }).pipe(Effect.flip)
      expect(error._tag).toBe("Integration.Authorization")
      if (error._tag === "Integration.Authorization")
        expect(error.cause).toBeInstanceOf(ModelResolver.CredentialUnavailableError)
      expect(test.resolved).toEqual([first])
      expect(test.activeLookups()).toBe(0)
    }).pipe(Effect.provide(test.layer))
  })
})

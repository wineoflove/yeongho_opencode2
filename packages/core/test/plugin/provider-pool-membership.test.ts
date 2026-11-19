import { expect, spyOn } from "bun:test"
import { Deferred, Effect, Fiber, Layer } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Model } from "@opencode/core/model"
import { ModelResolver } from "@opencode/core/model-resolver"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { OpenAIPlugin } from "@opencode/core/plugin/provider/openai"
import { Provider } from "@opencode/core/provider"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { EffectFlock } from "@opencode/util/effect-flock"
import { testEffect } from "../lib/effect"
import { PluginTestLayer } from "./fixture"

const it = testEffect(
  ModelResolver.layer.pipe(
    Layer.provideMerge(
      Layer.merge(PluginTestLayer, LayerNode.compile(LayerNode.group([AccountPool.node, EffectFlock.node]))),
    ),
  ),
)

for (const change of ["disable", "exclude", "remove", "replace"] as const) {
  it.live(`rejects a prepared pooled request after its account changes: ${change}`, () =>
    Effect.gen(function* () {
      const credentials = yield* Credential.Service
      const providers = yield* Provider.Service
      const resolver = yield* ModelResolver.Service
      const pools = yield* AccountPool.Service
      const providerID = Provider.ID.make("execution-membership-fixture")
      const integrationID = Integration.ID.make(providerID)
      const modelID = Model.ID.make("fixture-model")
      const integrations = yield* Integration.Service
      yield* integrations.transform((editor) =>
        editor.update(integrationID, (draft) => {
          draft.name = "Fixture"
        }),
      )
      const account = yield* credentials.create({
        integrationID,
        value: Credential.Key.make({ type: "key", key: "original" }),
      })
      const other = yield* credentials.create({
        integrationID,
        value: Credential.Key.make({ type: "key", key: "other" }),
      })
      const accounts = [account, other].map((entry) => ({ credentialID: entry.id, enabled: true }))
      yield* providers.transform((editor) => {
        editor.update(providerID, (draft) => {
          draft.activation = "enabled"
          draft.package = "@opencode/ai/providers/openai-compatible"
          draft.settings = { baseURL: "http://127.0.0.1:1/v1", accountPool: { strategy: "round-robin", accounts } }
        })
        editor.models.update(providerID, modelID, (draft) => {
          draft.enabled = true
        })
      })
      const resolved = yield* resolver.resolve(Model.Ref.make({ providerID, id: modelID }), { useAccountPool: true })
      if (!resolved?.accountBinding) return yield* Effect.die("Missing fixture binding")
      expect(resolved.accountBinding.credentialID).toBe(account.id)
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const owner = yield* pools
        .withAccount(
          resolved.accountBinding,
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      const locks = yield* EffectFlock.Service
      const acquire = locks.acquire
      const blocked = yield* Deferred.make<void>()
      const intercepted = spyOn(locks, "acquire").mockImplementation((...args) =>
        acquire(...args).pipe(
          Effect.tapError((error) =>
            error._tag === "LockTimeoutError" ? Deferred.succeed(blocked, undefined) : Effect.void,
          ),
        ),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => intercepted.mockRestore()))
      let executed = 0
      const waiter = yield* pools
        .withAccount(
          resolved.accountBinding,
          Effect.sync(() => {
            executed++
          }),
        )
        .pipe(Effect.result, Effect.forkScoped)
      yield* Deferred.await(blocked)
      switch (change) {
        case "disable":
        case "exclude":
          yield* providers.transform((editor) =>
            editor.update(providerID, (draft) => {
              draft.settings = {
                ...draft.settings,
                accountPool: {
                  strategy: "round-robin",
                  accounts:
                    change === "exclude"
                      ? accounts.filter((entry) => entry.credentialID !== account.id)
                      : accounts.map((entry) => ({ ...entry, enabled: entry.credentialID !== account.id })),
                },
              }
            }),
          )
          break
        case "remove":
          yield* credentials.remove(account.id)
          break
        case "replace":
          yield* credentials.update(account.id, { value: Credential.Key.make({ type: "key", key: "replacement" }) })
          break
      }
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(owner)
      const result = yield* Fiber.join(waiter)
      expect(executed).toBe(0)
      expect(result._tag).toBe("Failure")
      expect((yield* pools.status(integrationID)).lastSelectedCredentialID).toBe(account.id)
      expect(yield* credentials.get(other.id)).toEqual(other)
    }),
  )
}

it.live("allows an unchanged refreshed OAuth credential after storage omits undefined metadata", () =>
  Effect.gen(function* () {
    const credentials = yield* Credential.Service
    const integrations = yield* Integration.Service
    const providers = yield* Provider.Service
    const resolver = yield* ModelResolver.Service
    const pools = yield* AccountPool.Service
    const providerID = Provider.ID.make("refreshed-membership-fixture")
    const integrationID = Integration.ID.make(providerID)
    const modelID = Model.ID.make("fixture-model")
    const methodID = Integration.MethodID.make("fixture-oauth")
    yield* integrations.transform((editor) =>
      editor.method.update({
        integrationID,
        method: { type: "oauth", id: methodID, label: "Fixture" },
        authorize: () => Effect.die("unused login"),
        refresh: (value) =>
          Effect.succeed(
            Credential.OAuth.make({
              ...value,
              access: "renewed",
              expires: Number.MAX_SAFE_INTEGER,
              metadata: undefined,
            }),
          ),
      }),
    )
    const account = yield* credentials.create({
      integrationID,
      value: Credential.OAuth.make({ type: "oauth", methodID, access: "expired", refresh: "fixture", expires: 1 }),
    })
    yield* providers.transform((editor) => {
      editor.update(providerID, (draft) => {
        draft.activation = "enabled"
        draft.package = "@opencode/ai/providers/openai-compatible"
        draft.settings = {
          baseURL: "http://127.0.0.1:1/v1",
          accountPool: { strategy: "round-robin", accounts: [{ credentialID: account.id, enabled: true }] },
        }
      })
      editor.models.update(providerID, modelID, (draft) => {
        draft.enabled = true
      })
    })
    const resolved = yield* resolver.resolve(Model.Ref.make({ providerID, id: modelID }), { useAccountPool: true })
    if (!resolved?.accountBinding) return yield* Effect.die("Missing fixture binding")
    expect(yield* pools.withAccount(resolved.accountBinding, Effect.succeed("sent"))).toBe("sent")
    expect((yield* credentials.get(account.id))?.value).toMatchObject({ access: "renewed" })
  }),
)

it.effect("keeps model and provider pairs from the same immutable catalog generation", () =>
  Effect.gen(function* () {
    const providers = yield* Provider.Service
    const models = yield* Model.Service
    const providerID = Provider.ID.make("snapshot-fixture")
    const modelID = Model.ID.make("fixture-model")
    const publish = (version: number) =>
      providers.transform((catalog) => {
        catalog.update(providerID, (draft) => {
          draft.activation = "enabled"
          draft.settings = { baseURL: `https://generation-${version}.invalid/v1` }
        })
        catalog.models.update(providerID, modelID, (draft) => {
          draft.enabled = true
          draft.limit = { context: version * 1000, output: 100 }
        })
      })
    yield* publish(1)
    const first = yield* models.catalog(providerID, modelID)
    yield* publish(2)
    const second = yield* models.catalog(providerID, modelID)
    expect(first?.model.limit.context).toBe(1000)
    expect(first?.definition.provider.settings?.baseURL).toBe("https://generation-1.invalid/v1")
    expect(second?.model.limit.context).toBe(2000)
    expect(second?.definition.provider.settings?.baseURL).toBe("https://generation-2.invalid/v1")
    yield* providers.transform((catalog) =>
      catalog.update(providerID, (draft) => {
        draft.activation = "disabled"
      }),
    )
    expect(yield* models.catalog(providerID, modelID)).toBeUndefined()
  }),
)

it.effect("rejects conflicting catalog auth observations instead of keeping the last writer", () =>
  Effect.gen(function* () {
    const providers = yield* Provider.Service
    const models = yield* Model.Service
    const providerID = Provider.ID.make("conflict-fixture")
    const modelID = Model.ID.make("fixture")
    yield* providers.transform((catalog) => {
      catalog.update(providerID, (draft) => {
        draft.activation = "enabled"
      })
      catalog.models.update(providerID, modelID, (draft) => {
        draft.enabled = true
      })
      catalog.bindCatalogAuth(providerID, { integrationID: providerID, sourceKey: null, method: null })
      catalog.bindCatalogAuth(providerID, { integrationID: providerID, sourceKey: null, method: "different" })
      catalog.bindCatalogAuth(providerID, { integrationID: providerID, sourceKey: null, method: null })
    })
    expect(yield* models.catalog(providerID, modelID)).toBeUndefined()
    expect((yield* providers.snapshot()).records.get(providerID)?.catalogAuth).toBeNull()
  }),
)

it.effect("rejects catalog auth from a different integration even when both source keys are absent", () =>
  Effect.gen(function* () {
    const providers = yield* Provider.Service
    const models = yield* Model.Service
    const providerID = Provider.ID.make("binding-fixture")
    const modelID = Model.ID.make("fixture")
    yield* providers.transform((catalog) => {
      catalog.update(providerID, (draft) => {
        draft.activation = "enabled"
        draft.integrationID = Integration.ID.make("different-integration")
      })
      catalog.models.update(providerID, modelID, (draft) => {
        draft.enabled = true
      })
      catalog.bindCatalogAuth(providerID, { integrationID: "original-integration", sourceKey: null, method: null })
    })
    expect(yield* models.catalog(providerID, modelID)).toBeUndefined()
  }),
)

it.effect("does not resolve a disabled explicit catalog model with or without pool options", () =>
  Effect.gen(function* () {
    const providers = yield* Provider.Service
    const resolver = yield* ModelResolver.Service
    const providerID = Provider.ID.make("disabled-fixture")
    const modelID = Model.ID.make("fixture")
    yield* providers.transform((catalog) => {
      catalog.update(providerID, (draft) => {
        draft.activation = "enabled"
        draft.package = "@opencode/ai/providers/openai-compatible"
        draft.settings = { baseURL: "http://127.0.0.1:1/v1" }
      })
      catalog.models.update(providerID, modelID, (draft) => {
        draft.enabled = false
      })
    })
    const ref = Model.Ref.make({ providerID, id: modelID })
    expect(yield* resolver.resolve(ref)).toBeUndefined()
    expect(yield* resolver.resolve(ref, { useAccountPool: true })).toBeUndefined()
  }),
)

for (const scenario of ["key-oauth", "oauth-methods", "foreign", "empty"] as const) {
  it.effect(`rejects ${scenario} before refreshing or advancing the pool cursor`, () =>
    Effect.gen(function* () {
      let requests = 0
      const intercepted = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(
          async () => {
            requests++
            return Response.json({ error: "fixture-unexpected-refresh" }, { status: 401 })
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
      const first = yield* credentials.create({
        integrationID,
        value: Credential.OAuth.make({
          type: "oauth",
          methodID: Integration.MethodID.make("chatgpt-headless"),
          access: "expired-A",
          refresh: "fixture-refresh-A",
          expires: 0,
        }),
      })
      const second = yield* credentials.create({
        integrationID: scenario === "foreign" ? Integration.ID.make("other") : integrationID,
        value:
          scenario === "key-oauth"
            ? Credential.Key.make({ type: "key", key: "fixture-key" })
            : Credential.OAuth.make({
                type: "oauth",
                methodID: Integration.MethodID.make(
                  scenario === "foreign" ? "chatgpt-headless" : "chatgpt-token-sharing",
                ),
                access: "expired-B",
                refresh: "fixture-refresh-B",
                expires: 0,
              }),
      })
      yield* OpenAIPlugin.effect(yield* PluginHost.make(yield* Plugin.Service))
      const config = {
        strategy: "round-robin" as const,
        accounts: (scenario === "empty" ? [] : [first, second]).map(({ id }) => ({ credentialID: id, enabled: true })),
      }
      yield* providers.transform((catalog) => {
        catalog.models.update(Provider.ID.openai, Model.ID.make("gpt-6-astra"), (draft) => {
          draft.enabled = true
        })
        catalog.update(Provider.ID.openai, (draft) => {
          draft.activation = "enabled"
          draft.settings = { ...draft.settings, accountPool: config }
        })
      })
      const selected = Model.Info.make({
        id: Model.ID.make("gpt-6-astra"),
        modelID: Model.ID.make("gpt-6-astra"),
        providerID: Provider.ID.openai,
        name: "Fixture",
        package: "@opencode/ai/providers/openai",
        headers: {},
        variants: [],
        capabilities: { tools: true, input: ["text"], output: ["text"] },
        time: { released: 0 },
        cost: [],
        status: "active",
        enabled: true,
        limit: { context: 1000, output: 100 },
      })
      const before = yield* credentials.all()
      const failure = yield* resolver.resolveModel(selected, undefined, { useAccountPool: true }).pipe(Effect.flip)
      expect(failure._tag).toBe("Integration.Authorization")
      expect(requests).toBe(0)
      expect((yield* pool.status(integrationID, config)).lastSelectedCredentialID).toBeNull()
      expect(yield* credentials.all()).toEqual(before)
    }).pipe(Effect.scoped),
  )
}

import { expect, spyOn } from "bun:test"
import { Effect } from "effect"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { Plugin } from "@opencode/core/plugin"
import { PluginHost } from "@opencode/core/plugin/host"
import { AnthropicOAuthPlugin } from "@opencode/core/plugin/provider/anthropic-oauth"
import { CredentialRequestError } from "@opencode/core/credential-request-error"
import { PluginTestLayer } from "./fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(PluginTestLayer)
const integrationID = Integration.ID.make("anthropic")
const methodID = Integration.MethodID.make("anthropic-oauth")

it.effect("does not exchange or save a callback after the user cancels login", () =>
  Effect.gen(function* () {
    const plugin = yield* Plugin.Service
    const host = yield* PluginHost.make(plugin, "test")
    yield* AnthropicOAuthPlugin.effect(host)
    const integrations = yield* Integration.Service
    const credentials = yield* Credential.Service
    const existing = yield* credentials.create({
      integrationID,
      value: Credential.Key.make({ type: "key", key: "existing" }),
    })
    const attempt = yield* integrations.oauth.connect({ integrationID, methodID })
    const state = new URL(attempt.url).searchParams.get("state")
    const mock = spyOn(globalThis, "fetch").mockImplementation(
      Object.assign(async () => Response.json({}), { preconnect: globalThis.fetch.preconnect }),
    )
    yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
    yield* integrations.oauth.cancel({ integrationID, attemptID: attempt.attemptID })
    const error = yield* integrations.oauth
      .complete({ integrationID, attemptID: attempt.attemptID, code: `late-code#${state}` })
      .pipe(Effect.flip)
    expect(error).toBeInstanceOf(Integration.AttemptNotFoundError)
    expect(mock).not.toHaveBeenCalled()
    expect(yield* credentials.list(integrationID)).toEqual([existing])
  }).pipe(Effect.scoped),
)

for (const invalid of [
  { name: "empty refresh token", body: { access_token: "private-fixture", refresh_token: "", expires_in: 3600 } },
  { name: "zero expiry", body: { access_token: "private-fixture", refresh_token: "private-refresh", expires_in: 0 } },
  {
    name: "overflow expiry",
    body: { access_token: "private-fixture", refresh_token: "private-refresh", expires_in: Number.MAX_VALUE },
  },
]) {
  it.effect(`rejects ${invalid.name} without replacing the existing credential`, () =>
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      const host = yield* PluginHost.make(plugin, "test")
      yield* AnthropicOAuthPlugin.effect(host)
      const integrations = yield* Integration.Service
      const credentials = yield* Credential.Service
      const existing = yield* credentials.create({
        integrationID,
        value: Credential.OAuth.make({
          type: "oauth",
          methodID,
          access: "existing-access",
          refresh: "existing-refresh",
          expires: 1,
        }),
      })
      const mock = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(async () => Response.json(invalid.body), { preconnect: globalThis.fetch.preconnect }),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
      const connection = (yield* integrations.get(integrationID))?.connections.find(
        (entry) => entry.type === "credential" && entry.id === existing.id,
      )
      if (!connection) return yield* Effect.die("Missing fixture connection")
      const failure = yield* integrations.connection.resolve(connection).pipe(Effect.flip)
      expect(failure._tag).toBe("Integration.Authorization")
      expect(JSON.stringify(failure.cause)).not.toContain("private-fixture")
      expect(JSON.stringify(failure.cause)).not.toContain("private-refresh")
      expect(yield* credentials.get(existing.id)).toEqual(existing)
    }).pipe(Effect.scoped),
  )
}

it.effect("preserves an existing refresh token when a successful refresh does not rotate it", () =>
  Effect.gen(function* () {
    yield* AnthropicOAuthPlugin.effect(yield* PluginHost.make(yield* Plugin.Service, "test"))
    const integrations = yield* Integration.Service
    const credentials = yield* Credential.Service
    const existing = yield* credentials.create({
      integrationID,
      value: Credential.OAuth.make({
        type: "oauth",
        methodID,
        access: "old-access",
        refresh: "keep-refresh",
        expires: 1,
        metadata: { account: "same-account" },
      }),
    })
    const mock = spyOn(globalThis, "fetch").mockImplementation(
      Object.assign(async () => Response.json({ access_token: "new-access", expires_in: 3600 }), {
        preconnect: globalThis.fetch.preconnect,
      }),
    )
    yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
    const connection = (yield* integrations.get(integrationID))?.connections.find(
      (entry) => entry.type === "credential" && entry.id === existing.id,
    )
    if (!connection) return yield* Effect.die("Missing fixture connection")
    const value = yield* integrations.connection.resolve(connection)
    expect(value).toMatchObject({
      access: "new-access",
      refresh: "keep-refresh",
      metadata: { account: "same-account" },
    })
    expect((yield* credentials.get(existing.id))?.value).toEqual(value)
    expect(mock).toHaveBeenCalledTimes(1)
  }).pipe(Effect.scoped),
)

it.effect("does not borrow an existing credential refresh token for an incomplete new login", () =>
  Effect.gen(function* () {
    yield* AnthropicOAuthPlugin.effect(yield* PluginHost.make(yield* Plugin.Service, "test"))
    const integrations = yield* Integration.Service
    const credentials = yield* Credential.Service
    const existing = yield* credentials.create({
      integrationID,
      value: Credential.OAuth.make({
        type: "oauth",
        methodID,
        access: "existing-access",
        refresh: "existing-refresh",
        expires: Number.MAX_SAFE_INTEGER,
      }),
    })
    const attempt = yield* integrations.oauth.connect({ integrationID, methodID })
    const state = new URL(attempt.url).searchParams.get("state")
    if (!state) return yield* Effect.die("Missing fixture state")
    const mock = spyOn(globalThis, "fetch").mockImplementation(
      Object.assign(async () => Response.json({ access_token: "new-private-access", expires_in: 3600 }), {
        preconnect: globalThis.fetch.preconnect,
      }),
    )
    yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
    const failure = yield* integrations.oauth
      .complete({ integrationID, attemptID: attempt.attemptID, code: `fixture#${state}` })
      .pipe(Effect.flip)
    expect(failure).toBeInstanceOf(Integration.AuthorizationError)
    expect(yield* credentials.list(integrationID)).toEqual([existing])
    expect(mock).toHaveBeenCalledTimes(1)
    expect(JSON.stringify(failure)).not.toContain("new-private-access")
  }).pipe(Effect.scoped),
)

for (const accepted of [true, false]) {
  it.effect(`validates callback state before exchanging and saving a credential: accepted=${accepted}`, () =>
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      const host = yield* PluginHost.make(plugin, "test")
      yield* AnthropicOAuthPlugin.effect(host)
      const integrations = yield* Integration.Service
      const credentials = yield* Credential.Service
      const attempt = yield* integrations.oauth.connect({ integrationID, methodID, label: "Fixture" })
      const url = new URL(attempt.url)
      expect(url.origin).toBe("https://claude.ai")
      expect(url.searchParams.get("code_challenge_method")).toBe("S256")
      expect(url.searchParams.get("scope")).toBe(
        "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload",
      )
      const state = url.searchParams.get("state")
      const expectedChallenge = url.searchParams.get("code_challenge")
      if (!expectedChallenge) return yield* Effect.die("Missing PKCE challenge")
      if (!state) return yield* Effect.die("Missing OAuth state")
      const mock = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(
          async (input: string | URL | Request, init?: RequestInit) => {
            expect(String(input)).toBe("https://platform.claude.com/v1/oauth/token")
            expect(init?.method).toBe("POST")
            expect(new Headers(init?.headers).get("accept")).toBe("application/json, text/plain, */*")
            expect(new Headers(init?.headers).get("content-type")).toBe("application/json")
            expect(new Headers(init?.headers).get("user-agent")).toBe("axios/1.13.6")
            const body = JSON.parse(String(init?.body))
            expect(body.code).toBe("fixture-code")
            expect(body.state).toBe(state)
            expect(body.grant_type).toBe("authorization_code")
            expect(body.code_verifier).toMatch(/^[A-Za-z0-9_-]{86}$/)
            expect(Buffer.from(body.code_verifier, "base64url")).toHaveLength(64)
            const challenge = Buffer.from(
              await crypto.subtle.digest("SHA-256", new TextEncoder().encode(body.code_verifier)),
            ).toString("base64url")
            expect(challenge).toBe(expectedChallenge)
            return Response.json({ access_token: "fixture-access", refresh_token: "fixture-refresh", expires_in: 3600 })
          },
          { preconnect: globalThis.fetch.preconnect },
        ),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
      const operation = integrations.oauth.complete({
        integrationID,
        attemptID: attempt.attemptID,
        code: `fixture-code#${accepted ? state : "wrong-state"}`,
      })
      if (!accepted) {
        expect((yield* operation.pipe(Effect.flip))._tag).toBe("Integration.Authorization")
        expect(mock).not.toHaveBeenCalled()
        expect(yield* credentials.list(integrationID)).toEqual([])
        return
      }
      yield* operation
      expect(mock).toHaveBeenCalledTimes(1)
      expect(yield* credentials.list(integrationID)).toMatchObject([
        {
          label: "Fixture",
          value: {
            type: "oauth",
            methodID,
            access: "fixture-access",
            refresh: "fixture-refresh",
          },
        },
      ])
    }).pipe(Effect.scoped),
  )
}

it.effect("preserves a typed token exchange 429 without retrying or saving a credential", () =>
  Effect.gen(function* () {
    const plugin = yield* Plugin.Service
    const host = yield* PluginHost.make(plugin, "test")
    yield* AnthropicOAuthPlugin.effect(host)
    const integrations = yield* Integration.Service
    const credentials = yield* Credential.Service
    const attempt = yield* integrations.oauth.connect({ integrationID, methodID })
    const state = new URL(attempt.url).searchParams.get("state")
    if (!state) return yield* Effect.die("Missing OAuth state")
    const mock = spyOn(globalThis, "fetch").mockImplementation(
      Object.assign(
        async (input: string | URL | Request) => {
          expect(String(input)).toBe("https://platform.claude.com/v1/oauth/token")
          return Response.json(
            { error: "rate_limit_error", error_description: "private-token-exchange-body" },
            { status: 429, headers: { "Retry-After": "120" } },
          )
        },
        { preconnect: globalThis.fetch.preconnect },
      ),
    )
    yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
    const failure = yield* integrations.oauth
      .complete({ integrationID, attemptID: attempt.attemptID, code: `fixture-code#${state}` })
      .pipe(Effect.flip)
    expect(failure).toBeInstanceOf(Integration.AuthorizationError)
    if (!(failure instanceof Integration.AuthorizationError)) return yield* Effect.die("Missing authorization error")
    expect(failure.cause).toBeInstanceOf(CredentialRequestError)
    expect(failure.cause).toMatchObject({ status: 429, retryAfterMs: 120000 })
    expect(yield* integrations.oauth.status({ integrationID, attemptID: attempt.attemptID })).toMatchObject({
      status: "failed",
      message: "Credential request failed (429); retry-after-ms=120000",
    })
    expect(JSON.stringify(failure)).not.toContain("private-token-exchange-body")
    expect(mock).toHaveBeenCalledTimes(1)
    expect(yield* credentials.list(integrationID)).toEqual([])
  }).pipe(Effect.scoped),
)

for (const status of [200, 403, 400, 429, 503]) {
  it.live(`refreshes only the selected Anthropic credential or preserves it on HTTP ${status}`, () =>
    Effect.gen(function* () {
      const plugin = yield* Plugin.Service
      const host = yield* PluginHost.make(plugin, "test")
      yield* AnthropicOAuthPlugin.effect(host)
      const integrations = yield* Integration.Service
      const credentials = yield* Credential.Service
      const saved = yield* credentials.create({
        integrationID,
        value: Credential.OAuth.make({
          type: "oauth",
          methodID,
          access: "expired",
          refresh: "selected-refresh",
          expires: 1,
          metadata: { account: "selected" },
        }),
      })
      const other = yield* credentials.create({
        integrationID,
        value: Credential.OAuth.make({
          type: "oauth",
          methodID,
          access: "other",
          refresh: "other-refresh",
          expires: Date.now() + 3600000,
        }),
      })
      const mock = spyOn(globalThis, "fetch").mockImplementation(
        Object.assign(
          async (input: string | URL | Request, init?: RequestInit) => {
            expect(String(input)).toBe("https://platform.claude.com/v1/oauth/token")
            expect(init?.method).toBe("POST")
            expect(new Headers(init?.headers).get("accept")).toBe("application/json, text/plain, */*")
            expect(new Headers(init?.headers).get("content-type")).toBe("application/json")
            expect(new Headers(init?.headers).get("user-agent")).toBe("axios/1.13.6")
            expect(JSON.parse(String(init?.body))).toMatchObject({
              grant_type: "refresh_token",
              refresh_token: "selected-refresh",
            })
            return Response.json(
              status === 200
                ? { access_token: "renewed", refresh_token: "rotated", expires_in: 3600 }
                : { error: "invalid_grant", error_description: "private-fixture" },
              { status, headers: status === 429 || status === 503 ? { "Retry-After": "120" } : undefined },
            )
          },
          { preconnect: globalThis.fetch.preconnect },
        ),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => mock.mockRestore()))
      const connection = (yield* integrations.get(integrationID))?.connections.find(
        (entry) => entry.type === "credential" && entry.id === saved.id,
      )
      if (!connection) return yield* Effect.die("Missing fixture connection")
      if (status === 200) {
        const result = yield* integrations.connection.resolve(connection)
        expect(result).toMatchObject({ access: "renewed", refresh: "rotated", metadata: { account: "selected" } })
        expect((yield* credentials.get(saved.id))?.value).toEqual(result)
      } else {
        const failure = yield* integrations.connection.resolve(connection).pipe(Effect.flip)
        expect(failure.cause).toBeInstanceOf(CredentialRequestError)
        expect(failure.cause).toMatchObject({ status })
        if (status === 429 || status === 503) expect(failure.cause).toMatchObject({ retryAfterMs: 120000 })
        expect(JSON.stringify(failure.cause)).not.toContain("private-fixture")
        expect(yield* credentials.get(saved.id)).toEqual(saved)
      }
      expect(mock).toHaveBeenCalledTimes(1)
      expect(yield* credentials.get(other.id)).toEqual(other)
    }).pipe(Effect.scoped),
  )
}

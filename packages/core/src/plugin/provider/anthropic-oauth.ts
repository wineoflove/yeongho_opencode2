import { define } from "@opencode/plugin/effect/plugin"
import { Effect, Option, Schema } from "effect"
import { Credential } from "../../credential.js"
import { Integration } from "../../integration.js"
import { credentialRequestError } from "../../credential-request-error.js"
import type { PluginInternal } from "../internal.js"

const methodID = Integration.MethodID.make("anthropic-oauth")
const clientID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
const redirectURI = "https://platform.claude.com/oauth/code/callback"
const tokenURL = "https://platform.claude.com/v1/oauth/token"
const Tokens = Schema.Struct({
  access_token: Schema.String.check(Schema.isMinLength(1)),
  refresh_token: Schema.optional(Schema.String.check(Schema.isMinLength(1))),
  expires_in: Schema.Number.check(Schema.isGreaterThan(0)),
})

class InvalidCallback extends Schema.TaggedError<InvalidCallback>()("AnthropicOAuth.InvalidCallback", {}) {
  override get message() {
    return "Invalid Anthropic authorization callback or state"
  }
}

class InvalidTokens extends Schema.TaggedError<InvalidTokens>()("AnthropicOAuth.InvalidTokens", {}) {
  override get message() {
    return "Anthropic returned an invalid token response"
  }
}

function parseCallback(input: string) {
  const text = input.trim()
  if (URL.canParse(text)) {
    const url = new URL(text)
    if (url.origin !== new URL(redirectURI).origin || url.pathname !== new URL(redirectURI).pathname) return undefined
    return { code: url.searchParams.get("code"), state: url.searchParams.get("state") }
  }
  const parts = text.split("#")
  if (parts.length === 2) return { code: parts[0], state: parts[1] }
  const params = new URLSearchParams(text)
  return { code: params.get("code"), state: params.get("state") }
}

function exchange(body: Readonly<Record<string, string>>, previousRefresh?: string) {
  return Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch(tokenURL, {
        method: "POST",
        signal,
        headers: {
          "content-type": "application/json",
          accept: "application/json, text/plain, */*",
          "user-agent": "axios/1.13.6",
        },
        body: JSON.stringify(body),
      })
      if (!response.ok) throw await credentialRequestError(response)
      const parsed = Schema.decodeUnknownOption(Tokens)(await response.json().catch(() => undefined))
      if (Option.isNone(parsed)) throw new InvalidTokens()
      const tokens = parsed.value
      const refresh = tokens.refresh_token ?? previousRefresh
      if (!refresh) throw new InvalidTokens()
      const expires = Date.now() + tokens.expires_in * 1000
      if (!Number.isSafeInteger(expires)) throw new InvalidTokens()
      return Credential.OAuth.make({
        type: "oauth",
        methodID,
        access: tokens.access_token,
        refresh,
        expires,
      })
    },
    catch: (cause) => cause,
  }).pipe(Effect.timeout("30 seconds"))
}

export const AnthropicOAuthPlugin = define({
  id: "opencode.provider.anthropic-oauth",
  effect: Effect.fn(function* (ctx) {
    yield* ctx.integration.transform((editor) =>
      editor.method.update({
        integrationID: Integration.ID.make("anthropic"),
        method: { id: methodID, type: "oauth", label: "Anthropic OAuth (compatibility pending)" },
        authorize: () =>
          Effect.gen(function* () {
            const verifier = Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64url")
            const state = crypto.randomUUID().replaceAll("-", "")
            const challenge = yield* Effect.promise(async () =>
              Buffer.from(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier))).toString(
                "base64url",
              ),
            )
            const url = new URL("https://claude.ai/oauth/authorize")
            url.search = new URLSearchParams({
              code: "true",
              client_id: clientID,
              response_type: "code",
              redirect_uri: redirectURI,
              scope:
                "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload",
              code_challenge: challenge,
              code_challenge_method: "S256",
              state,
            }).toString()
            return {
              mode: "code" as const,
              url: url.toString(),
              instructions:
                "Native subscription compatibility is not verified. Paste the callback URL or code#state after authorization.",
              callback: (input: string) =>
                Effect.gen(function* () {
                  const callback = parseCallback(input)
                  if (!callback?.code || callback.state !== state) return yield* new InvalidCallback()
                  return yield* exchange({
                    grant_type: "authorization_code",
                    client_id: clientID,
                    code: callback.code,
                    state,
                    redirect_uri: redirectURI,
                    code_verifier: verifier,
                  })
                }),
            }
          }),
        refresh: (credential) =>
          exchange(
            { grant_type: "refresh_token", client_id: clientID, refresh_token: credential.refresh },
            credential.refresh,
          ).pipe(Effect.map((next) => Credential.OAuth.make({ ...next, metadata: credential.metadata }))),
      }),
    )
  }),
} satisfies PluginInternal.InternalPlugin)

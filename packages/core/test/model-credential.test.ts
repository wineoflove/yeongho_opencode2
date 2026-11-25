import { describe, expect, it } from "bun:test"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { ModelCredential } from "@opencode/core/model-credential"
import { Auth } from "@opencode/ai/route"
import { Effect } from "effect"
import { Headers } from "effect/http"

const openai = Integration.ID.make("openai")
const inherited = { "ChatGPT-Account-Id": "active-account", "x-trace": "preserved" }
const credential = (method: string, accountID?: unknown) =>
  Credential.OAuth.make({
    type: "oauth",
    methodID: Integration.MethodID.make(method),
    access: "fixture-token",
    refresh: "fixture-refresh",
    expires: 1,
    metadata: { accountID },
  })

describe("request-bound Anthropic OAuth protocol", () => {
  it.each([undefined, "", "other-beta, oauth-2025-04-20, second-beta, oauth-2025-04-20"])(
    "merges beta tokens once without mutating input headers: %s",
    async (beta) => {
      const headers = Object.freeze(
        Headers.fromInput({
          "X-API-KEY": "stale-key",
          "Anthropic-Beta": beta,
          "x-trace": "preserved",
          Authorization: "Bearer stale-token",
        }),
      )
      const auth = ModelCredential.auth(
        "@opencode/ai/providers/anthropic",
        credential("anthropic-oauth"),
        Auth.bearer("selected-token"),
      )
      const input = {
        request: {},
        method: "POST",
        url: "https://anthropic.example/v1/messages",
        body: "{}",
        headers,
      } as const
      const result = await Effect.runPromise(auth.apply(input))
      expect(result["anthropic-beta"]).toBe(beta ? "other-beta,oauth-2025-04-20,second-beta" : "oauth-2025-04-20")
      expect(result.authorization).toBe("Bearer selected-token")
      expect(result["x-api-key"]).toBeUndefined()
      expect(result["x-trace"]).toBe("preserved")
      expect(await Effect.runPromise(auth.apply({ ...input, headers: result }))).toEqual(result)
      expect(headers["x-api-key"]).toBe("stale-key")
      if (beta === undefined) expect(headers["anthropic-beta"]).toBeUndefined()
      else expect(headers["anthropic-beta"]).toBe(beta)
      expect(headers.authorization).toBe("Bearer stale-token")
    },
  )
})

describe("request-bound OpenAI account identity", () => {
  it.each(["chatgpt-browser", "chatgpt-headless"])(
    "pairs %s with the selected identity without mutating the catalog",
    (method) => {
      expect(ModelCredential.headers(openai, credential(method, "selected-account"), inherited)).toEqual({
        "chatgpt-account-id": "selected-account",
        "x-trace": "preserved",
      })
      expect(inherited["ChatGPT-Account-Id"]).toBe("active-account")
    },
  )

  it.each([undefined, null, "", " ", 42, "account\r\ninjected"])(
    "rejects missing or invalid account identity %s",
    (accountID) => {
      expect(() => ModelCredential.headers(openai, credential("chatgpt-headless", accountID), inherited)).toThrow(
        ModelCredential.AccountIdentityUnavailableError,
      )
    },
  )

  it("removes a stale account header for token-sharing credentials", () => {
    expect(ModelCredential.headers(openai, credential("chatgpt-token-sharing"), inherited)).toEqual({
      "x-trace": "preserved",
    })
  })

  it("removes a stale account header for API keys", () => {
    expect(ModelCredential.headers(openai, Credential.Key.make({ type: "key", key: "fixture" }), inherited)).toEqual({
      "x-trace": "preserved",
    })
  })

  it("leaves other integration headers unchanged", () => {
    expect(ModelCredential.headers(Integration.ID.make("other"), credential("other"), inherited)).toEqual(inherited)
  })
})

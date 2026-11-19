export * as ModelCredential from "./model-credential.js"

import { Credential } from "./credential.js"
import { Integration } from "./integration.js"
import { Auth } from "@opencode/ai/route"
import { Effect } from "effect"
import { Headers } from "effect/http"

export function auth(
  packageName: string | undefined,
  credential: Credential.Value | undefined,
  source: Auth.Definition,
): Auth.Definition {
  if (
    credential?.type !== "oauth" ||
    credential.methodID !== "anthropic-oauth" ||
    (packageName !== "@opencode/ai/providers/anthropic" &&
      packageName !== "@opencode/ai/providers/anthropic-compatible")
  )
    return source
  return source.andThen(
    Auth.custom((input) => {
      const betas = (input.headers["anthropic-beta"] ?? "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean)
      return Effect.succeed(
        Headers.set(
          Headers.remove(input.headers, "x-api-key"),
          "anthropic-beta",
          [...new Set([...betas, "oauth-2025-04-20"])].join(","),
        ),
      )
    }),
  )
}

export class AccountIdentityUnavailableError extends Error {
  constructor() {
    super("The selected OpenAI account has no usable account identity")
    this.name = "AccountIdentityUnavailableError"
  }
}

/** Keeps a request's OpenAI account header paired with its selected credential, never the active-account snapshot. */
export function headers(
  integrationID: Integration.ID,
  credential: Credential.Value,
  source: Readonly<Record<string, string>> | undefined,
): Record<string, string> | undefined {
  if (integrationID !== Integration.ID.make("openai")) return source && { ...source }
  const result = Object.fromEntries(
    Object.entries(source ?? {}).filter(([name]) => name.toLowerCase() !== "chatgpt-account-id"),
  )
  if (credential.type !== "oauth") return result
  if (credential.methodID !== "chatgpt-browser" && credential.methodID !== "chatgpt-headless") return result
  const accountID = credential.metadata?.accountID
  if (typeof accountID !== "string" || accountID.trim().length === 0 || /[\r\n\0]/.test(accountID))
    throw new AccountIdentityUnavailableError()
  return { ...result, "chatgpt-account-id": accountID }
}

export * as AccountPoolFailure from "./account-pool-failure.js"

import type { AIError } from "@opencode/ai/effect"
import type { Integration } from "@opencode/schema/integration"
import type { AccountPool } from "./account-pool.js"

export function retryAfterMs(error: AIError, integrationID: Integration.ID, now = Date.now()): number | undefined {
  if ("retryAfterMs" in error.reason && error.reason.retryAfterMs !== undefined) return error.reason.retryAfterMs
  const http = error.reason.http
  if (integrationID !== "anthropic" || http?.status !== 429) return undefined
  const delays = ["anthropic-ratelimit-unified-5h-reset", "anthropic-ratelimit-unified-7d-reset"].flatMap((name) => {
    const value = http.headers[name]
    if (!value?.trim()) return []
    const deadline = Number(value) * 1000
    const delay = deadline - now
    return Number.isSafeInteger(deadline) && Number.isSafeInteger(delay) && delay > 0 ? [delay] : []
  })
  return delays.length ? Math.max(...delays) : undefined
}

export function classify(error: AIError): AccountPool.Failure {
  if (error.reason.http?.status === 403 || error.reason._tag === "ContentPolicy") return "access-denied"
  if (error.reason.http?.status === 401 || error.reason._tag === "Authentication") return "authentication"
  if (error.reason.http?.status === 429 || error.reason._tag === "RateLimit" || error.reason._tag === "QuotaExceeded")
    return "rate-limited"
  if ((error.reason.http?.status ?? 0) >= 500 || error.reason._tag === "ProviderInternal") return "service"
  if (error.reason._tag === "Transport" || error.reason._tag === "Timeout") return "transport"
  return "unknown"
}

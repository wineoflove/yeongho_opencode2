import { describe, expect, test } from "bun:test"
import { AIError, ContentPolicyError, HttpContext, RateLimitError, UnknownProviderError } from "@opencode/ai/effect"
import { AccountPoolFailure } from "@opencode/core/account-pool-failure"
import { Integration } from "@opencode/schema/integration"

describe("pool failure categories", () => {
  test.each([
    [401, "authentication"],
    [403, "access-denied"],
    [429, "rate-limited"],
    [503, "service"],
    [400, "unknown"],
  ] as const)("classifies HTTP %i as %s", (status, expected) => {
    const error = new AIError({
      reason: new UnknownProviderError({
        message: "fixture",
        http: new HttpContext({ url: "http://fixture.invalid", status, headers: {} }),
      }),
    })
    expect(AccountPoolFailure.classify(error)).toBe(expected)
  })
  test("treats an explicit policy rejection without HTTP context as access denial", () => {
    expect(AccountPoolFailure.classify(new AIError({ reason: new ContentPolicyError({ message: "fixture" }) }))).toBe(
      "access-denied",
    )
  })
})

describe("pool failure retry delay", () => {
  const now = 1_800_000_000_000
  const integrationID = Integration.ID.make("anthropic")

  test.each([
    ["future 5h", "1800018000", undefined, 18_000_000],
    ["future 7d", undefined, "1800604800", 604_800_000],
    ["latest is 7d", "1800018000", "1800604800", 604_800_000],
    ["latest is 5h", "1800604800", "1800018000", 604_800_000],
    ["past with future", "1799999999", "1800604800", 604_800_000],
    ["invalid with future", "invalid", "1800604800", 604_800_000],
    ["whitespace around number", " 1800018000 ", undefined, 18_000_000],
    ["missing", undefined, undefined, undefined],
    ["blank", "", "  ", undefined],
    ["invalid", "1800018000seconds", "invalid", undefined],
    ["nonfinite", "Infinity", "NaN", undefined],
    ["negative infinity", "-Infinity", undefined, undefined],
    ["past and present", "1799999999", "1800000000", undefined],
    ["unsafe deadline", "9007199254741", undefined, undefined],
    ["overflow", "1e309", undefined, undefined],
  ] as const)("uses only a valid future unified deadline: %s", (_, fiveHour, sevenDay, expected) => {
    const error = new AIError({
      reason: new UnknownProviderError({
        message: "fixture",
        http: new HttpContext({
          url: "http://fixture.invalid",
          status: 429,
          headers: {
            ...(fiveHour === undefined ? {} : { "anthropic-ratelimit-unified-5h-reset": fiveHour }),
            ...(sevenDay === undefined ? {} : { "anthropic-ratelimit-unified-7d-reset": sevenDay }),
            "x-ratelimit-reset-tokens": "1800604800",
          },
        }),
      }),
    })
    expect(AccountPoolFailure.retryAfterMs(error, integrationID, now)).toBe(expected)
  })

  test.each([0, 2000])("preserves explicit retry-after %i ahead of reset headers", (retryAfterMs) => {
    const error = new AIError({
      reason: new RateLimitError({
        message: "fixture",
        retryAfterMs,
        http: new HttpContext({
          url: "http://fixture.invalid",
          status: 429,
          headers: { "anthropic-ratelimit-unified-7d-reset": "1800604800" },
        }),
      }),
    })
    expect(AccountPoolFailure.retryAfterMs(error, integrationID, now)).toBe(retryAfterMs)
  })

  test.each([
    ["openai", 429],
    ["custom-anthropic", 429],
    ["anthropic", 401],
    ["anthropic", 403],
    ["anthropic", 400],
    ["anthropic", 500],
    ["anthropic", 503],
  ] as const)("does not infer reset delays for %s HTTP %i", (integration, status) => {
    const error = new AIError({
      reason: new RateLimitError({
        message: "fixture",
        http: new HttpContext({
          url: "http://fixture.invalid",
          status,
          headers: { "anthropic-ratelimit-unified-7d-reset": "1800604800" },
        }),
      }),
    })
    expect(AccountPoolFailure.retryAfterMs(error, Integration.ID.make(integration), now)).toBeUndefined()
  })

  test("does not infer a reset delay without HTTP context", () => {
    const error = new AIError({ reason: new RateLimitError({ message: "fixture" }) })
    expect(AccountPoolFailure.retryAfterMs(error, integrationID, now)).toBeUndefined()
  })
})

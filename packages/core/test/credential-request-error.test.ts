import { expect, spyOn, test } from "bun:test"
import { credentialRequestError } from "@opencode/core/credential-request-error"

test.each([
  { body: { error: "invalid_grant", error_description: "fixture-private" }, code: "invalid_grant" },
  { body: { error: "invalid_refresh_token" }, code: "invalid_refresh_token" },
  { body: { error: { type: "invalid_grant", message: "fixture-private" } }, code: "invalid_grant" },
  { body: { error: { type: "invalid_refresh_token" } }, code: "invalid_refresh_token" },
  { body: { error: { code: "invalid_refresh_token" } }, code: undefined },
  { body: { error: "invalid_refresh_token_extra" }, code: undefined },
  { body: { error: { type: "INVALID_GRANT" } }, code: undefined },
  { body: { error: "fixture-private", error_description: "invalid_refresh_token" }, code: undefined },
  { body: { error: "invalid_request", error_description: "invalid_grant" }, code: undefined },
  { body: { error: { code: "invalid_grant" } }, code: undefined },
  { body: null, code: undefined },
])("retains only the exact standardized OAuth code from $body", async ({ body, code }) => {
  const error = await credentialRequestError(Response.json(body, { status: 400 }))
  expect(error.status).toBe(400)
  expect(error.code).toBe(code)
  expect(JSON.stringify(error)).not.toContain("fixture-private")
  expect(error.message).toBe("Credential request failed (400)")
})

test.each([401, 403, 429, 500, 503, 599])("does not consume HTTP %s bodies", async (status) => {
  const response = Response.json({ error: "invalid_refresh_token", private: "fixture-private" }, { status })
  const error = await credentialRequestError(response)
  expect(response.bodyUsed).toBe(false)
  expect(error.code).toBeUndefined()
  expect(JSON.stringify(error)).not.toContain("fixture-private")
  expect(error.message).toBe(`Credential request failed (${status})`)
})

test.each([
  { value: "0", expected: 0 },
  { value: "120", expected: 120000 },
  { value: "7200", expected: 7200000 },
  { value: "999999999999999999999999999999999999999999", expected: Number.MAX_SAFE_INTEGER },
  { value: undefined, expected: undefined },
  { value: "", expected: undefined },
  { value: "-1", expected: undefined },
  { value: "1.5", expected: undefined },
  { value: "1e3", expected: undefined },
  { value: "2026-11-09", expected: undefined },
  { value: "fixture-private", expected: undefined },
])("parses Retry-After $value only for transient failures", async ({ value, expected }) => {
  for (const status of [400, 401, 403, 429, 500, 503, 599]) {
    const headers = value === undefined ? undefined : { "Retry-After": value }
    const error = await credentialRequestError(new Response(null, { status, headers }))
    expect(error.retryAfterMs).toBe(status === 429 || status >= 500 ? expected : undefined)
    expect(JSON.stringify(error)).not.toContain("fixture-private")
  }
})

test("uses a deterministic HTTP date delay without a fifteen minute cap", async () => {
  const now = spyOn(Date, "now").mockReturnValue(Date.UTC(2026, 10, 9, 0, 0, 0))
  try {
    for (const [value, expected] of [
      ["Mon, 09 Nov 2026 02:00:00 GMT", 7200000],
      ["Monday, 09-Nov-26 02:00:00 GMT", 7200000],
      ["Mon Nov  9 02:00:00 2026", 7200000],
      ["Sun, 08 Nov 2026 23:00:00 GMT", 0],
      ["Mon, 99 Nov 2026 02:00:00 GMT", undefined],
      ["Mon, 30 Feb 2026 02:00:00 GMT", undefined],
    ] as const) {
      const error = await credentialRequestError(new Response(null, { status: 429, headers: { "Retry-After": value } }))
      expect(error.retryAfterMs).toBe(expected)
    }
  } finally {
    now.mockRestore()
  }
})

test("preserves the HTTP failure for a non-JSON error body", async () => {
  const error = await credentialRequestError(new Response("private HTML", { status: 400 }))
  expect(error.status).toBe(400)
  expect(error.code).toBeUndefined()
  expect(JSON.stringify(error)).not.toContain("private HTML")
})

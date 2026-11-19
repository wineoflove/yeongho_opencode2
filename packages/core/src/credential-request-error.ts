import { Option, Schema } from "effect"

const RejectedCode = Schema.Literals(["invalid_grant", "invalid_refresh_token"])

export class CredentialRequestError extends Schema.TaggedError<CredentialRequestError>()("Credential.Request", {
  status: Schema.Number,
  code: Schema.optional(RejectedCode),
  retryAfterMs: Schema.optional(Schema.Number),
}) {
  override get message() {
    const timing =
      this.retryAfterMs !== undefined &&
      Number.isSafeInteger(this.retryAfterMs) &&
      this.retryAfterMs >= 0 &&
      (this.status === 429 || (this.status >= 500 && this.status < 600))
        ? `; retry-after-ms=${this.retryAfterMs}`
        : ""
    return `Credential request failed (${this.status})${timing}`
  }
}

function retryAfter(value: string | null): number | undefined {
  const text = value?.trim()
  if (!text) return undefined
  if (/^\d+$/.test(text)) return Math.min(Number(text) * 1000, Number.MAX_SAFE_INTEGER)
  const withoutZone = /^[A-Za-z]{3} [A-Za-z]{3} [ \d]\d \d{2}:\d{2}:\d{2} \d{4}$/.test(text)
  const date = Date.parse(withoutZone ? `${text} GMT` : text)
  if (!Number.isFinite(date)) return undefined
  const instant = new Date(date)
  const canonical = instant.toUTCString()
  const weekday = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][instant.getUTCDay()]
  const rfc850 = `${weekday}, ${canonical.slice(5, 7)}-${canonical.slice(8, 11)}-${canonical.slice(14, 16)} ${canonical.slice(17)}`
  const asctime = `${canonical.slice(0, 3)} ${canonical.slice(8, 11)} ${String(instant.getUTCDate()).padStart(2, " ")} ${canonical.slice(17, 25)} ${canonical.slice(12, 16)}`
  if (text !== canonical && text !== rfc850 && text !== asctime) return undefined
  return Math.min(Math.max(0, date - Date.now()), Number.MAX_SAFE_INTEGER)
}

export async function credentialRequestError(response: Response): Promise<CredentialRequestError> {
  if (response.status !== 400)
    return new CredentialRequestError({
      status: response.status,
      ...(response.status === 429 || (response.status >= 500 && response.status < 600)
        ? { retryAfterMs: retryAfter(response.headers.get("Retry-After")) }
        : {}),
    })
  let body: unknown
  try {
    body = await response.json()
  } catch {
    return new CredentialRequestError({ status: response.status })
  }
  const parsed = Schema.decodeUnknownOption(
    Schema.Struct({
      error: Schema.Union([RejectedCode, Schema.Struct({ type: RejectedCode })]),
    }),
  )(body)
  return new CredentialRequestError({
    status: response.status,
    ...(Option.isSome(parsed)
      ? { code: typeof parsed.value.error === "string" ? parsed.value.error : parsed.value.error.type }
      : {}),
  })
}

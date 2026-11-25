import { Effect } from "effect"
import type { AccountPool } from "./account-pool.js"
import type { Credential } from "./credential.js"
import { CredentialRequestError } from "./credential-request-error.js"
import { Integration } from "./integration.js"

function classify(cause: unknown): AccountPool.Failure {
  if (!(cause instanceof CredentialRequestError)) return "unknown"
  if (cause.status === 403) return "access-denied"
  if (
    cause.status === 401 ||
    (cause.status === 400 && (cause.code === "invalid_grant" || cause.code === "invalid_refresh_token"))
  )
    return "authentication"
  if (cause.status === 429) return "rate-limited"
  if (cause.status >= 500 && cause.status < 600) return "service"
  return "unknown"
}

export function observeCredentialResolution(
  operation: Effect.Effect<Credential.Value | undefined, Integration.AuthorizationError>,
  pool: AccountPool.Interface,
  binding: AccountPool.Binding,
) {
  return operation.pipe(
    Effect.tapError((error) =>
      pool
        .reportFailure(
          binding,
          classify(error.cause),
          error.cause instanceof CredentialRequestError ? error.cause.retryAfterMs : undefined,
        )
        .pipe(Effect.mapError((cause) => new Integration.AuthorizationError({ cause }))),
    ),
  )
}

import { Provider } from "@opencode/core/provider"
import { ProviderNotFoundError, ServiceUnavailableError } from "@opencode/protocol/errors"
import { Effect, Schema } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Integration } from "@opencode/core/integration"
import { HttpApiBuilder } from "effect/http-api"
import { Api } from "../api"
import { response } from "../location"

export const ProviderHandler = HttpApiBuilder.group(Api, "server.provider", (handlers) =>
  Effect.gen(function* () {
    return handlers
      .handle(
        "provider.pool",
        Effect.fn(function* (ctx) {
          const providers = yield* Provider.Service
          const pools = yield* AccountPool.Service
          const provider = yield* providers.get(ctx.params.providerID)
          if (!provider)
            return yield* new ProviderNotFoundError({
              providerID: ctx.params.providerID,
              message: "Provider not found",
            })
          const config =
            provider.settings?.accountPool === undefined
              ? undefined
              : yield* Schema.decodeUnknownEffect(AccountPool.Config)(provider.settings.accountPool).pipe(
                  Effect.mapError(
                    () =>
                      new ServiceUnavailableError({
                        service: "account-pool",
                        message: "Invalid account pool configuration",
                      }),
                  ),
                )
          return yield* response(
            pools
              .status(provider.integrationID ?? Integration.ID.make(provider.id), config)
              .pipe(
                Effect.mapError(
                  () =>
                    new ServiceUnavailableError({ service: "account-pool", message: "Account pool state unavailable" }),
                ),
              ),
          )
        }),
      )
      .handle(
        "provider.list",
        Effect.fn(function* () {
          const providers = yield* Provider.Service
          return yield* response(providers.available())
        }),
      )
      .handle(
        "provider.get",
        Effect.fn(function* (ctx) {
          const providers = yield* Provider.Service
          const provider = yield* providers.get(ctx.params.providerID)
          if (!provider)
            return yield* new ProviderNotFoundError({
              providerID: ctx.params.providerID,
              message: `Provider not found: ${ctx.params.providerID}`,
            })
          return yield* response(Effect.succeed(provider))
        }),
      )
  }),
)

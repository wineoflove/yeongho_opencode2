import { Context, Effect, Option } from "effect"

export interface RequestAuth {
  readonly integrationID: string
  readonly method: string | null
}

export class RequestAuthContext extends Context.Service<RequestAuthContext, RequestAuth>()("@opencode/RequestAuth") {}

export const withRequestAuth = <A, E, R>(effect: Effect.Effect<A, E, R>, auth?: RequestAuth) =>
  auth ? effect.pipe(Effect.provideService(RequestAuthContext, auth)) : effect

export const requestUsesOAuth = (methods: readonly string[], fallback: boolean) =>
  Effect.serviceOption(RequestAuthContext).pipe(
    Effect.map((context) => {
      if (Option.isNone(context)) return fallback
      return (
        context.value.integrationID === "openai" &&
        context.value.method !== null &&
        methods.includes(context.value.method)
      )
    }),
  )

export * as SessionGenerate from "./generate.js"

import type { FileSystem } from "../filesystem.js"
import { LLMClient, LLMEvent, Message, AIError } from "@opencode/ai/effect"
import { Duration, Effect, Option } from "effect"
import { Database } from "../database/database.js"
import { Instance } from "../instance/service.js"
import { Plugin } from "../plugin/service.js"
import type { Instructions } from "../instructions/index.js"
import { SessionContext } from "./context.js"
import type { AgentNotFoundError } from "./error.js"
import { SessionHistory } from "./history.js"
import { SessionModelRequest } from "./model-request.js"
import { SessionRunnerRetry } from "./runner/retry.js"
import { toSessionError } from "./to-session-error.js"
import type { SessionRunnerModel } from "./runner/model.js"
import type { SessionSchema } from "./schema.js"
import { AccountPool } from "../account-pool.js"
import { AccountPoolFailure } from "../account-pool-failure.js"
import { Integration } from "../integration.js"
import { bindRequestModel } from "./request-model-binding.js"

export type Error =
  | AgentNotFoundError
  | Instructions.InitializationBlocked
  | SessionRunnerModel.Error
  | AIError
  | FileSystem.DirectoryNotFoundError

/** Generates text from current Session context without mutating the Session. */
export const generate = Effect.fn("SessionGenerate.generate")(function* (input: {
  session: SessionSchema.Info
  prompt: string
}) {
  const instances = yield* Instance.Service
  const database = yield* Database.Service
  const llm = yield* LLMClient.Service

  return yield* Effect.gen(function* () {
    yield* Plugin.awaitActivation
    const context = yield* SessionContext.Service
    const selection = yield* context.select(input.session.id)
    const pools = Option.getOrUndefined(yield* Effect.serviceOption(AccountPool.Service))
    const preview = yield* context.resolveModel(selection.session, { useAccountPool: true, advanceAccountPool: false })
    const history = yield* SessionHistory.preview(
      database.db,
      selection.session.id,
      selection.instructions,
      preview.ref,
    )
    const transcript = SessionModelRequest.baseTranscript({
      agent: selection.agent.info,
      model: preview,
      tools: selection.tools,
      initial: history.initial,
      messages: history.messages,
    })
    const prepared = yield* context.request.generate({
      session: selection.session,
      agent: selection.agent.id,
      model: preview,
      tools: selection.tools,
      system: transcript.system,
      messages: [
        ...transcript.messages,
        ...(history.instructionUpdate ? [Message.system(history.instructionUpdate)] : []),
        Message.user(input.prompt),
      ],
    })
    const decide = yield* SessionRunnerRetry.policy(selection.session.id)
    while (true) {
      const model = yield* context.resolveModel(selection.session, { useAccountPool: true })
      if (model.accountBinding && !pools)
        return yield* new Integration.AuthorizationError({ cause: "Account pool service is unavailable" })
      if (
        Boolean(preview.accountBinding) !== Boolean(model.accountBinding) ||
        preview.requestAuth?.integrationID !== model.requestAuth?.integrationID ||
        preview.requestAuth?.method !== model.requestAuth?.method
      )
        return yield* new Integration.AuthorizationError({
          cause: "Authentication changed during auxiliary generation",
        })
      const request = model.accountBinding ? bindRequestModel(prepared.request, model) : prepared.request
      yield* Effect.logInfo("sending session generation request", {
        sessionID: selection.session.id,
        providerID: model.ref.providerID,
        modelID: model.ref.id,
      })
      const operation = llm.generate(request, prepared.options).pipe(
        Effect.flatMap((response) => {
          if (
            model.accountBinding &&
            pools &&
            (response.finishReason.normalized === "content-filter" ||
              response.events.some(
                (event) =>
                  (LLMEvent.is.stepFinish(event) || LLMEvent.is.finish(event)) &&
                  event.reason.normalized === "content-filter",
              ))
          )
            return pools
              .reportFailure(model.accountBinding, "access-denied")
              .pipe(
                Effect.andThen(
                  Effect.fail(new Integration.AuthorizationError({ cause: "Auxiliary generation was denied" })),
                ),
              )
          return Effect.succeed(response)
        }),
      )
      const result = yield* (
        model.accountBinding && pools
          ? pools.withAccount(
              model.accountBinding,
              operation.pipe(
                Effect.tapError((error) =>
                  model.accountBinding && error instanceof AIError
                    ? pools.reportFailure(
                        model.accountBinding,
                        AccountPoolFailure.classify(error),
                        AccountPoolFailure.retryAfterMs(error, model.accountBinding.integrationID),
                      )
                    : Effect.void,
                ),
              ),
            )
          : operation
      ).pipe(
        Effect.mapError((error) =>
          error instanceof AIError ? error : new Integration.AuthorizationError({ cause: error }),
        ),
        Effect.result,
      )
      if (result._tag === "Failure") {
        const cause = result.failure
        if (!(cause instanceof AIError)) return yield* Effect.fail(cause)
        const decision = yield* decide({
          cause,
          error: toSessionError(cause),
          agent: selection.agent.id,
          model: model.ref,
          hook: prepared.retry,
          retry: SessionRunnerRetry.isRetryable(cause),
        })
        if (!decision.retry) return yield* Effect.fail(cause)
        yield* Effect.sleep(Duration.millis(decision.delay))
        continue
      }
      const response = result.success
      yield* Effect.logInfo("session generation usage diagnostic", { usage: response.usage })
      return response.text
    }
  }).pipe(instances.provide(input.session))
})

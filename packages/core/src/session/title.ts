export * as SessionTitle from "./title.js"

import { isDeepStrictEqual } from "node:util"
import { AIError, LLMClient, LLMEvent, Message, SystemPart } from "@opencode/ai/effect"
import type { Agent } from "@opencode/schema/agent"
import { Context, DateTime, Effect, Layer, Schema, Stream } from "effect"
import { Database } from "../database/database.js"
import { Bus } from "../bus.js"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { isExactRootFallback } from "@opencode/util/session-title-fallback"
import { llmClient } from "../effect/app-node-platform.js"
import { SessionContext } from "./context.js"
import { SessionEvent } from "./event.js"
import { SessionHistory } from "./history.js"
import { SessionRunnerModel } from "./runner/model.js"
import { Model } from "../model.js"
import { AccountPool } from "../account-pool.js"
import { AccountPoolFailure } from "../account-pool-failure.js"
import { Provider } from "../provider.js"
import { bindRequestModel } from "./request-model-binding.js"
import { SessionSchema } from "./schema.js"
import { SessionUsage } from "./usage.js"
import { SessionStore } from "./store.js"

const MAX_LENGTH = 100
const MAX_CONTEXT_LENGTH = 8_000
const MAX_FIRST_MESSAGE_LENGTH = 2_000
const titleChanged = Symbol("Session title changed")
class PoolTitleFailure extends Schema.TaggedError<PoolTitleFailure>()("SessionTitle.PoolFailure", {}) {}

export interface Interface {
  /** Generates an initial title or regenerates one from bounded conversation history. */
  readonly generate: (sessionID: SessionSchema.ID) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionTitle") {}

const truncate = (value: string) => (value.length <= MAX_LENGTH ? value : `${value.slice(0, MAX_LENGTH - 3)}...`)
// Framing the input as text to name keeps models from answering or carrying out the request instead.
const messagePrompt = (text: string) =>
  `Here is the first message of the conversation:\n\n<message>\n${text}\n</message>\n\nReply with only the title.`
const conversationPrompt = (text: string) =>
  `Here is the conversation so far:\n\n<conversation>\n${text}\n</conversation>\n\nReply with only the title.`
export const isUntitled = (session: SessionSchema.Info) =>
  isExactRootFallback({
    title: session.title,
    time: { created: DateTime.toEpochMillis(session.time.created) },
  })

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const llm = yield* LLMClient.Service
    const context = yield* SessionContext.Service
    const store = yield* SessionStore.Service
    const db = (yield* Database.Service).db
    const models = yield* SessionRunnerModel.Service
    const catalog = yield* Model.Service
    const pools = yield* AccountPool.Service
    const providers = yield* Provider.Service

    const attempt = Effect.fn("SessionTitle.attempt")(function* (input: {
      readonly session: SessionSchema.Info
      readonly agent: Agent.Info
      readonly text: string
      readonly model: SessionRunnerModel.Resolved
    }) {
      const configuredPool = (yield* providers.get(input.model.ref.providerID))?.settings?.accountPool !== undefined
      const preview = yield* models
        .resolve({ ...input.session, model: input.model.ref }, catalog.available, {
          useAccountPool: true,
          advanceAccountPool: false,
        })
        .pipe(Effect.catch(() => (configuredPool ? Effect.fail(new PoolTitleFailure()) : Effect.succeed(undefined))))
      if (!preview) return
      const chunks: string[] = []
      let failed = false
      let usage: SessionUsage.Recorded | undefined
      const recordUsage = Effect.suspend(() =>
        usage
          ? bus.publish(SessionEvent.UsageRecorded, {
              sessionID: input.session.id,
              source: "title",
              ...usage,
            })
          : Effect.void,
      )
      const prepared = yield* context.request.title({
        session: input.session,
        agent: input.agent.id,
        model: preview,
        system: input.agent.system ? [SystemPart.make(input.agent.system)] : [],
        messages: [Message.user(input.text)],
      })
      if (prepared.event.result !== undefined) return prepared.event.result
      const model = configuredPool
        ? yield* models
            .resolve({ ...input.session, model: input.model.ref }, catalog.available, { useAccountPool: true })
            .pipe(Effect.mapError(() => new PoolTitleFailure()))
        : preview
      if (
        preview.requestAuth?.integrationID !== model.requestAuth?.integrationID ||
        preview.requestAuth?.method !== model.requestAuth?.method
      )
        return yield* new PoolTitleFailure()
      const request = configuredPool ? bindRequestModel(prepared.request, model) : prepared.request
      const operation = llm.stream(request, prepared.options).pipe(
        Stream.runForEach((event) => {
          if (LLMEvent.is.providerError(event)) failed = true
          if (LLMEvent.is.textDelta(event)) chunks.push(event.text)
          if (LLMEvent.is.stepFinish(event)) {
            const step = SessionUsage.record(event.usage, model.cost)
            usage = usage ? SessionUsage.add(usage, step) : step
          }
          if (
            (LLMEvent.is.stepFinish(event) || LLMEvent.is.finish(event)) &&
            event.reason.normalized === "content-filter"
          ) {
            failed = true
            if (model.accountBinding) return pools.reportFailure(model.accountBinding, "access-denied")
          }
          return Effect.void
        }),
      )
      const guarded = model.accountBinding
        ? pools.withAccount(
            model.accountBinding,
            operation.pipe(
              Effect.tapError((error) =>
                error instanceof AIError && model.accountBinding
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
      yield* guarded.pipe(
        Effect.catch(() =>
          Effect.sync(() => {
            failed = true
          }),
        ),
        Effect.onInterrupt(() => recordUsage.pipe(Effect.asVoid)),
      )
      yield* recordUsage
      if (failed && (configuredPool || model.accountBinding)) return yield* new PoolTitleFailure()
      if (failed) return
      return chunks
        .join("")
        .split("\n")
        .map((line) => line.trim())
        .find((line) => line.length > 0)
    })

    const generate = Effect.fn("SessionTitle.generate")(
      function* (sessionID: SessionSchema.ID) {
        const session = yield* store.get(sessionID)
        if (!session) return
        const firstUser = yield* SessionHistory.firstUserMessage(db, session.id)
        if (!firstUser) return
        const text = !isUntitled(session)
          ? yield* store.context(session.id).pipe(
              Effect.map((messages) => {
                const original = `Original request:\n${firstUser.text.slice(0, MAX_FIRST_MESSAGE_LENGTH)}`
                const recent = messages
                  .flatMap((message) => {
                    if (message.type === "user" && message.id !== firstUser.id) return [`User: ${message.text.trim()}`]
                    if (message.type !== "assistant") return []
                    const text = message.content
                      .flatMap((part) => (part.type === "text" ? [part.text.trim()] : []))
                      .filter(Boolean)
                      .join("\n")
                    return text ? [`Assistant: ${text}`] : []
                  })
                  .join("\n\n")
                if (!recent) return conversationPrompt(original)
                const prefix = `${original}\n\nRecent conversation:\n`
                const budget = MAX_CONTEXT_LENGTH - conversationPrompt("").length - prefix.length
                return conversationPrompt(`${prefix}${recent.slice(-budget)}`)
              }),
              Effect.orElseSucceed(() => messagePrompt(firstUser.text)),
            )
          : messagePrompt(firstUser.text)
        const selection = yield* context.selectTitle(session)
        if (!selection) return
        const title =
          (yield* attempt({ session, agent: selection.agent, text, model: selection.selected })) ??
          (selection.primary && !isDeepStrictEqual(selection.selected.ref, selection.primary.ref)
            ? yield* attempt({ session, agent: selection.agent, text, model: selection.primary })
            : undefined)
        if (!title) return
        const expectedSequence = (yield* Bus.latestSequence(db, sessionID)) + 1
        const current = yield* store.get(sessionID)
        if (!current || current.title !== session.title || current.title === truncate(title)) return
        yield* bus
          .publish(
            SessionEvent.Renamed,
            {
              sessionID: session.id,
              title: truncate(title),
            },
            { commit: (sequence) => (sequence === expectedSequence ? Effect.void : Effect.die(titleChanged)) },
          )
          .pipe(Effect.catchDefect((defect) => (defect === titleChanged ? Effect.void : Effect.die(defect))))
      },
      Effect.catchTag("SessionTitle.PoolFailure", () => Effect.void),
    )
    return Service.of({ generate })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [
    Bus.node,
    llmClient,
    SessionContext.node,
    SessionStore.node,
    Database.node,
    Model.node,
    SessionRunnerModel.node,
    AccountPool.node,
    Provider.node,
  ],
})

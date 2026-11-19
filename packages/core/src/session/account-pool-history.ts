import { AIError, UnsupportedOperationError, type LLMRequest } from "@opencode/ai/effect"
import { Effect } from "effect"
import type { ModelResolver } from "../model-resolver.js"
import { SessionHistory } from "./history.js"
import type { SessionMessage } from "./message.js"

const reject = () =>
  new AIError({
    reason: new UnsupportedOperationError({
      operation: "account-pool native checkpoint replay",
      message:
        "Native checkpoints cannot be used with account pools: checkpoint portability between accounts is unverified",
    }),
  })

export const assertPortableAccountHistory = (
  model: ModelResolver.Resolved,
  messages: ReadonlyArray<SessionMessage.Info>,
): Effect.Effect<void, AIError> =>
  model.accountBinding && messages.some(SessionHistory.isNativeCompaction) ? Effect.fail(reject()) : Effect.void

export const assertPortableAccountRequest = (
  model: ModelResolver.Resolved,
  request: LLMRequest,
): Effect.Effect<void, AIError> =>
  model.accountBinding && request.messages.some((message) => message.content.some((part) => part.type === "compaction"))
    ? Effect.fail(reject())
    : Effect.void

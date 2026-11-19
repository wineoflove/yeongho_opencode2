import { Effect, Schema } from "effect"
import { eq } from "drizzle-orm"
import { Event } from "@opencode/schema/event"
import { SessionMessage } from "@opencode/schema/session-message"
import { Session } from "../session.js"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"
import { SessionInboxTable, SessionMessageTable, SessionTable } from "./sql.js"
import { EventTable } from "../event/sql.js"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { SessionEvent } from "./event.js"

export class ReportConflictError extends Schema.TaggedError<ReportConflictError>()("Session.ReportConflictError", {
  messageID: SessionMessage.ID,
}) {}

const locks = KeyedMutex.makeUnsafe<string>()

export const appendSessionReport = (input: {
  readonly sessionID: Session.ID
  readonly messageID: SessionMessage.ID
  readonly text: string
}) =>
  locks
    .withLock(input.messageID)(
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const bus = yield* Bus.Service
        const { db } = yield* Database.Service
        const session = yield* sessions.get(input.sessionID)
        if (session.revert) return yield* new ReportConflictError({ messageID: input.messageID })
        const eventID = Event.ID.make(input.messageID.replace(/^msg_/, "evt_"))
        const pending = () =>
          db
            .select({ id: SessionInboxTable.id })
            .from(SessionInboxTable)
            .where(eq(SessionInboxTable.id, input.messageID))
            .get()
            .pipe(Effect.orDie)
        if (yield* pending()) return yield* new ReportConflictError({ messageID: input.messageID })
        const stored = yield* db
          .select()
          .from(SessionMessageTable)
          .where(eq(SessionMessageTable.id, input.messageID))
          .get()
          .pipe(Effect.orDie)
        if (stored) {
          const message = yield* sessions.message({ sessionID: input.sessionID, messageID: input.messageID })
          if (
            stored.session_id !== input.sessionID ||
            !message ||
            message.type !== "synthetic" ||
            message.metadata?.source !== "manager-report" ||
            message.text !== input.text
          )
            return yield* new ReportConflictError({ messageID: input.messageID })
          return { messageID: input.messageID, reused: true }
        }
        const eventOwner = yield* db
          .select()
          .from(EventTable)
          .where(eq(EventTable.id, eventID))
          .get()
          .pipe(Effect.orDie)
        if (eventOwner) return yield* new ReportConflictError({ messageID: input.messageID })
        const published = yield* bus
          .publish(
            SessionEvent.Synthetic,
            {
              sessionID: input.sessionID,
              text: input.text,
              metadata: { source: "manager-report" },
            },
            {
              id: eventID,
              commit: () =>
                Effect.gen(function* () {
                  if (yield* pending())
                    return yield* Effect.die(new ReportConflictError({ messageID: input.messageID }))
                  const current = yield* db
                    .select()
                    .from(SessionTable)
                    .where(eq(SessionTable.id, input.sessionID))
                    .get()
                    .pipe(Effect.orDie)
                  if (!current || current.revert)
                    return yield* Effect.die(new ReportConflictError({ messageID: input.messageID }))
                }),
            },
          )
          .pipe(
            Effect.catchDefect((defect) =>
              defect instanceof ReportConflictError ? Effect.fail(defect) : Effect.die(defect),
            ),
            Effect.as(true),
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                const currentSession = yield* db
                  .select()
                  .from(SessionTable)
                  .where(eq(SessionTable.id, input.sessionID))
                  .get()
                  .pipe(Effect.orDie)
                if (!currentSession || currentSession.revert)
                  return yield* new ReportConflictError({ messageID: input.messageID })
                const collided = yield* db
                  .select()
                  .from(EventTable)
                  .where(eq(EventTable.id, eventID))
                  .get()
                  .pipe(Effect.orDie)
                if (!collided) return yield* Effect.failCause(cause)
                const current = yield* sessions.message({ sessionID: input.sessionID, messageID: input.messageID })
                if (
                  collided.aggregate_id === input.sessionID &&
                  current?.type === "synthetic" &&
                  current.metadata?.source === "manager-report" &&
                  current.text === input.text
                )
                  return false
                return yield* new ReportConflictError({ messageID: input.messageID })
              }),
            ),
          )
        return { messageID: input.messageID, reused: !published }
      }),
    )
    .pipe(Effect.uninterruptible)

import { expect } from "bun:test"
import { Effect } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Database } from "@opencode/core/database/database"
import { Bus } from "@opencode/core/bus"
import { Session } from "@opencode/core/session"
import { SessionStore } from "@opencode/core/session/store"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/schema/session-message"
import { Event } from "@opencode/schema/event"
import { appendSessionReport } from "@opencode/core/session/report"
import { SessionMessageTable } from "@opencode/core/session/sql"
import { eq } from "drizzle-orm"
import { Project } from "@opencode/core/project"
import { Location } from "@opencode/core/location"
import { AbsolutePath } from "@opencode/core/schema"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { promptLocationNode } from "./fixture/prompt-location"
import { globalProjectNode } from "./lib/project"
import { testEffect } from "./lib/effect"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, Session.node]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      Project.node.replace(globalProjectNode),
      LocationServiceMap.node.replace(promptLocationNode),
      SessionExecution.node.replace(SessionExecution.noopLayer),
    ],
  ),
)

for (const type of ["user", "synthetic"] as const) {
  for (const foreign of [false, true]) {
    for (const race of [false, true]) {
      it.effect(`report rejects pending ${type} ID foreign=${foreign} race=${race} and leaves it deliverable`, () =>
        Effect.gen(function* () {
          const sessions = yield* Session.Service
          const bus = yield* Bus.Service
          const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })
          const owner = yield* sessions.create({ location })
          const target = foreign ? yield* sessions.create({ location }) : owner
          const messageID = SessionMessage.ID.create()
          const enqueue = bus.publish(SessionEvent.InboxEnqueued, {
            sessionID: owner.id,
            inboxID: messageID,
            item: { type, payload: { text: "preserved pending input" }, delivery: "queue" },
          })
          if (!race) yield* enqueue
          const result = yield* appendSessionReport({ sessionID: target.id, messageID, text: "must conflict" }).pipe(
            Effect.provideService(
              Bus.Service,
              race
                ? {
                    ...bus,
                    publish: (definition, data, options) =>
                      enqueue.pipe(Effect.andThen(bus.publish(definition, data, options))),
                  }
                : bus,
            ),
            Effect.result,
          )
          expect(result).toMatchObject({ _tag: "Failure", failure: { _tag: "Session.ReportConflictError" } })
          expect(yield* sessions.messages({ sessionID: target.id })).toEqual([])
          expect(yield* sessions.inbox(owner.id)).toMatchObject([
            { id: messageID, type, payload: { text: "preserved pending input" } },
          ])
          yield* bus.publish(SessionEvent.InboxDelivered, { sessionID: owner.id, inboxID: messageID })
          expect(yield* sessions.inbox(owner.id)).toEqual([])
          expect(yield* sessions.messages({ sessionID: owner.id })).toMatchObject([
            { id: messageID, type, text: "preserved pending input" },
          ])
        }),
      )
    }
  }
}

it.effect("an event-only ID collision is a report conflict without changing its owning session", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const bus = yield* Bus.Service
    const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })
    const owner = yield* sessions.create({ location })
    const target = yield* sessions.create({ location })
    const messageID = SessionMessage.ID.create()
    const eventID = Event.ID.make(messageID.replace(/^msg_/, "evt_"))
    yield* bus.publish(SessionEvent.Renamed, { sessionID: owner.id, title: "preserved owner title" }, { id: eventID })
    const result = yield* appendSessionReport({ sessionID: target.id, messageID, text: "report" }).pipe(Effect.flip)
    expect(result._tag).toBe("Session.ReportConflictError")
    expect((yield* sessions.get(owner.id)).title).toBe("preserved owner title")
    expect(yield* sessions.messages({ sessionID: target.id })).toEqual([])
  }),
)

it.effect("a competing event writer after the ownership read is converted to a report conflict", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const bus = yield* Bus.Service
    const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })
    const owner = yield* sessions.create({ location })
    const target = yield* sessions.create({ location })
    const messageID = SessionMessage.ID.create()
    const eventID = Event.ID.make(messageID.replace(/^msg_/, "evt_"))
    const raced = appendSessionReport({ sessionID: target.id, messageID, text: "report" }).pipe(
      Effect.provideService(Bus.Service, {
        ...bus,
        publish: (definition, data, options) =>
          bus
            .publish(SessionEvent.Renamed, { sessionID: owner.id, title: "competing event" }, { id: eventID })
            .pipe(Effect.andThen(bus.publish(definition, data, options))),
      }),
    )
    expect((yield* raced.pipe(Effect.flip))._tag).toBe("Session.ReportConflictError")
    expect(yield* sessions.messages({ sessionID: target.id })).toEqual([])
  }),
)

for (const mutation of ["delete", "revert"] as const) {
  it.effect(`report publication refuses a concurrent session ${mutation}`, () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const bus = yield* Bus.Service
      const { db } = yield* Database.Service
      const session = yield* sessions.create({
        location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
      })
      const messageID = SessionMessage.ID.create()
      const race =
        mutation === "delete"
          ? sessions.remove(session.id)
          : bus.publish(SessionEvent.RevertEvent.Staged, {
              sessionID: session.id,
              revert: { messageID },
            })
      const result = yield* appendSessionReport({
        sessionID: session.id,
        messageID,
        text: "must not be published",
      }).pipe(
        Effect.provideService(Bus.Service, {
          ...bus,
          publish: (definition, data, options) => race.pipe(Effect.andThen(bus.publish(definition, data, options))),
        }),
        Effect.flip,
      )
      expect(result._tag).toBe("Session.ReportConflictError")
      expect(
        yield* db
          .select()
          .from(SessionMessageTable)
          .where(eq(SessionMessageTable.id, messageID))
          .get()
          .pipe(Effect.orDie),
      ).toBeUndefined()
    }),
  )
}

it.effect("durable synthetic events appear in transcript without becoming inbox inputs", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const bus = yield* Bus.Service
    const session = yield* sessions.create({
      location: Location.Ref.make({ directory: AbsolutePath.make("/project") }),
    })
    yield* bus.publish(SessionEvent.Synthetic, { sessionID: session.id, text: "synthetic report evidence" })
    const messages = yield* sessions.messages({ sessionID: session.id })
    expect(messages).toHaveLength(1)
    expect(messages[0]).toMatchObject({ type: "synthetic", text: "synthetic report evidence" })
    expect(yield* sessions.inbox(session.id)).toEqual([])
  }),
)

it.effect("report retries publish one message and reject changed content or another session", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })
    const session = yield* sessions.create({ location })
    const other = yield* sessions.create({ location })
    const input = { sessionID: session.id, messageID: SessionMessage.ID.create(), text: "report" }
    const results = yield* Effect.all([appendSessionReport(input), appendSessionReport(input)], {
      concurrency: "unbounded",
    })
    expect(results.map((result) => result.reused).sort()).toEqual([false, true])
    expect(yield* sessions.messages({ sessionID: session.id })).toHaveLength(1)
    expect(yield* sessions.inbox(session.id)).toEqual([])
    const changed = yield* appendSessionReport({ ...input, text: "changed" }).pipe(Effect.flip)
    expect(changed._tag).toBe("Session.ReportConflictError")
    const foreign = yield* appendSessionReport({ ...input, sessionID: other.id }).pipe(Effect.flip)
    expect(foreign._tag).toBe("Session.ReportConflictError")
    expect(yield* sessions.messages({ sessionID: other.id })).toHaveLength(0)
  }),
)

import { expect } from "bun:test"
import { Effect, Schema } from "effect"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"
import { offlineModels } from "../../core/test/fixture/models"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { join } from "node:path"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })
const password = "synthetic-report-password"
const authorization = `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`

it.live("report retries remain idempotent after reopening a file database", () =>
  Effect.gen(function* () {
    const root = yield* tmpdirScoped()
    const options = {
      password,
      app: { version: "test" },
      database: { path: join(root.path, "reports.db") },
      fs: { filewatcher: false },
    }
    const initial = yield* Effect.gen(function* () {
      const handler = yield* ServerFetch.make(options, { overrides: [offlineModels] })
      const response = yield* Effect.promise(() =>
        handler(
          new Request("http://opencode.local/api/session", {
            method: "POST",
            headers: { authorization, "content-type": "application/json" },
            body: JSON.stringify({ title: "restart report" }),
          }),
        ),
      )
      const session = Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => response.json())).data
      const messageID = SessionMessage.ID.create()
      const url = `http://opencode.local/api/experimental/session/${session.id}/report`
      const body = { messageID, text: "persistent synthetic report" }
      const published = yield* Effect.promise(() =>
        handler(
          new Request(url, {
            method: "POST",
            headers: { authorization, "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        ),
      )
      expect(published.status).toBe(200)
      return { url, body, sessionID: session.id }
    }).pipe(Effect.scoped)
    const reopened = yield* ServerFetch.make(options, { overrides: [offlineModels] })
    const repeated = yield* Effect.promise(() =>
      reopened(
        new Request(initial.url, {
          method: "POST",
          headers: { authorization, "content-type": "application/json" },
          body: JSON.stringify(initial.body),
        }),
      ),
    )
    expect(repeated.status).toBe(200)
    expect(yield* Effect.promise(() => repeated.json())).toEqual({
      data: { messageID: initial.body.messageID, reused: true },
    })
    const removed = yield* Effect.promise(() =>
      reopened(
        new Request(`http://opencode.local/api/session/${initial.sessionID}`, {
          method: "DELETE",
          headers: { authorization },
        }),
      ),
    )
    expect(removed.status).toBe(204)
  }).pipe(Effect.scoped),
)

it.live("authenticated report delivery is idempotent, refuses conflicts, and does not enter the inbox", () =>
  Effect.gen(function* () {
    const handler = yield* ServerFetch.make(
      {
        password,
        app: { version: "test" },
        database: { path: ":memory:" },
        fs: { filewatcher: false },
      },
      { overrides: [offlineModels] },
    )
    const request = (path: string, body?: unknown, auth = true) =>
      Effect.promise(() =>
        handler(
          new Request(`http://opencode.local${path}`, {
            method: body === undefined ? "GET" : "POST",
            headers: { "content-type": "application/json", ...(auth ? { authorization } : {}) },
            body: body === undefined ? undefined : JSON.stringify(body),
          }),
        ),
      )
    const created = yield* request("/api/session", { title: "report HTTP fixture" })
    expect(created.status).toBe(200)
    const session = Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => created.json())).data
    const messageID = SessionMessage.ID.create()
    const route = `/api/experimental/session/${session.id}/report`
    const body = { messageID, text: "synthetic report" }
    expect((yield* request(route, body, false)).status).toBe(401)
    const response = yield* request(route, body)
    expect(response.status).toBe(200)
    expect(yield* Effect.promise(() => response.json())).toEqual({ data: { messageID, reused: false } })
    const repeated = yield* request(route, body)
    expect(repeated.status).toBe(200)
    expect(yield* Effect.promise(() => repeated.json())).toEqual({ data: { messageID, reused: true } })
    expect((yield* request(route, { ...body, text: "conflicting" })).status).toBe(409)
    const message = yield* request(`/api/session/${session.id}/message/${messageID}`)
    expect(message.status).toBe(200)
    expect(yield* Effect.promise(() => message.json())).toMatchObject({ data: { type: "synthetic", text: body.text } })
    const inbox = yield* request(`/api/session/${session.id}/inbox`)
    expect(inbox.status).toBe(200)
    expect(yield* Effect.promise(() => inbox.json())).toEqual({ data: [] })
    const active = yield* request("/api/session/active")
    expect(yield* Effect.promise(() => active.json())).toEqual({ data: {} })
    const removed = yield* Effect.promise(() =>
      handler(
        new Request(`http://opencode.local/api/session/${session.id}`, {
          method: "DELETE",
          headers: { authorization },
        }),
      ),
    )
    expect(removed.status).toBe(204)
  }).pipe(Effect.scoped),
)

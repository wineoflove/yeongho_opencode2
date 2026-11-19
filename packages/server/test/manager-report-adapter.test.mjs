import { expect } from "bun:test"
import { Effect } from "effect"
import { it } from "../../core/test/lib/effect"
import { offlineModels } from "../../core/test/fixture/models"
import { ServerFetch } from "../src/fetch"
import { openCodeApi } from "/Users/yeonghokim/apps/ocm-v2-staging/port/scripts/learning-review/opencode.mjs"
import { publishExactReport } from "/Users/yeonghokim/apps/ocm-v2-staging/port/scripts/learning-review/report-delivery.mjs"

it.live("Manager noReply adapter publishes through the native report route without scheduling a prompt", () =>
  Effect.gen(function* () {
    const password = "synthetic-native-secret"
    const authorization = `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`
    const handler = yield* ServerFetch.make(
      {
        password,
        app: { version: "test" },
        database: { path: ":memory:" },
        fs: { filewatcher: false },
      },
      { overrides: [offlineModels] },
    )
    yield* Effect.promise(async () => {
      const created = await handler(
        new Request("http://opencode.local/api/session", {
          method: "POST",
          headers: { authorization, "content-type": "application/json" },
          body: "{}",
        }),
      )
      expect(created.status).toBe(200)
      const { data: session } = await created.json()
      let reports = 0
      let prompts = 0
      const transport = async (input, init) => {
        const request = new Request(input, init)
        expect(request.headers.get("authorization")).toBe("Bearer synthetic-manager-token")
        const url = new URL(request.url)
        expect(url.pathname.startsWith("/api/opencode-proxy/api/")).toBe(true)
        url.pathname = url.pathname.slice("/api/opencode-proxy".length)
        if (url.pathname.endsWith("/report")) reports++
        if (url.pathname.endsWith("/prompt")) prompts++
        const headers = new Headers(request.headers)
        headers.set("authorization", authorization)
        return handler(
          new Request(url, {
            method: request.method,
            headers,
            body: request.method === "GET" ? undefined : await request.text(),
          }),
        )
      }
      const api = openCodeApi(
        { OCM_INTERNAL_API_URL: "http://opencode.local/api/internal", OCM_INTERNAL_TOKEN: "synthetic-manager-token" },
        transport,
      )
      try {
        const config = { delivery: "noReply", feedbackSession: session.id }
        await publishExactReport(config, "msg_managerReportFixture", "합성 보고서", api)
        await publishExactReport(config, "msg_managerReportFixture", "합성 보고서", api)
        expect(reports).toBe(1)
        expect(prompts).toBe(0)
        const saved = await api.session.message.get({ sessionID: session.id, messageID: "msg_managerReportFixture" })
        expect(saved).toMatchObject({ type: "synthetic", text: "합성 보고서", metadata: { source: "manager-report" } })
        const inbox = await handler(
          new Request(`http://opencode.local/api/session/${session.id}/inbox`, { headers: { authorization } }),
        )
        expect(await inbox.json()).toEqual({ data: [] })
        const active = await handler(
          new Request("http://opencode.local/api/session/active", { headers: { authorization } }),
        )
        expect(await active.json()).toEqual({ data: {} })
      } finally {
        const removed = await handler(
          new Request(`http://opencode.local/api/session/${session.id}`, {
            method: "DELETE",
            headers: { authorization },
          }),
        )
        expect(removed.status).toBe(204)
      }
    })
  }).pipe(Effect.scoped),
)

import { expect, test } from "bun:test"
import { ai, Message } from "@opencode/ai/effect"
import { LLMClient, RequestExecutor, WebSocketTransport } from "@opencode/ai/route"
import { configure } from "@opencode/ai/providers/openai"
import { Credential } from "@opencode/core/credential"
import { Integration } from "@opencode/core/integration"
import { ModelCredential } from "@opencode/core/model-credential"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { WebSocketConstructor } from "@opencode/core/effect/websocket-constructor"
import { Session } from "@opencode/schema/session"
import { Effect, Layer, Schema } from "effect"
import { Socket } from "effect/socket"
import { FetchHttpClient } from "effect/http"
import { makeWebSocketServer } from "./lib/websocket-server"

const selectedModel = (baseURL: string, account: string) => {
  const credential = Credential.OAuth.make({
    type: "oauth",
    methodID: Integration.MethodID.make("chatgpt-headless"),
    access: `fixture-token-${account}`,
    refresh: `fixture-refresh-${account}`,
    expires: 1,
    metadata: { accountID: account },
  })
  return configure({
    baseURL,
    apiKey: credential.access,
    headers: ModelCredential.headers(Integration.ID.make("openai"), credential, {
      "ChatGPT-Account-Id": "global-stale",
    }),
  }).responses("gpt-fixture")
}

test("real Responses sockets change account affinity without reusing the other account checkpoint", async () => {
  const requests: Array<{ readonly previousResponseID: unknown; readonly connectionID: number }> = []
  await Effect.runPromise(
    Effect.gen(function* () {
      const constructor = yield* Socket.WebSocketConstructor
      const server = yield* makeWebSocketServer({
        message: (socket, message) => {
          const input = Schema.decodeUnknownSync(
            Schema.Struct({ previous_response_id: Schema.optional(Schema.String) }),
          )(JSON.parse(message.toString()))
          requests.push({ previousResponseID: input.previous_response_id, connectionID: socket.data.id })
          const id = `message-${requests.length}`
          const item = {
            type: "message",
            id,
            role: "assistant",
            status: "completed",
            content: [{ type: "output_text", text: "Fixture reply", annotations: [], logprobs: [] }],
          }
          socket.send(JSON.stringify({ type: "response.created", response: { id: `response-${requests.length}` } }))
          socket.send(JSON.stringify({ type: "response.output_item.added", item: { type: "message", id } }))
          socket.send(JSON.stringify({ type: "response.output_text.delta", item_id: id, delta: "Fixture reply" }))
          socket.send(JSON.stringify({ type: "response.output_text.done", item_id: id, text: "Fixture reply" }))
          socket.send(JSON.stringify({ type: "response.output_item.done", item }))
          socket.send(
            JSON.stringify({
              type: "response.completed",
              response: { id: `response-${requests.length}`, output: [item] },
            }),
          )
        },
      })
      const transportLayer = SessionModelTransport.makeLayer({
        open: (input) =>
          WebSocketTransport.open(input).pipe(Effect.provideService(Socket.WebSocketConstructor, constructor)),
      })
      yield* Effect.gen(function* () {
        const transport = yield* SessionModelTransport.Service
        const executor = transport.bind(Session.ID.make("ses_account_transport"))
        const client = LLMClient.layer.pipe(
          Layer.provide(
            Layer.succeed(
              RequestExecutor.Service,
              RequestExecutor.Service.of({ execute: () => Effect.die("Unexpected HTTP fallback") }),
            ),
          ),
        )
        const baseURL = server.url.replace(/^ws/, "http").replace(/responses$/, "")
        const first = yield* LLMClient.Service.use((llm) =>
          llm.generate(ai.request({ model: selectedModel(baseURL, "A"), prompt: "First" }), { webSocket: executor }),
        ).pipe(Effect.provide(client))
        const second = yield* LLMClient.Service.use((llm) =>
          llm.generate(
            ai.request({
              model: selectedModel(baseURL, "B"),
              messages: [Message.user("First"), first.message, Message.user("Second")],
            }),
            { webSocket: executor },
          ),
        ).pipe(Effect.provide(client))
        yield* LLMClient.Service.use((llm) =>
          llm.generate(
            ai.request({
              model: selectedModel(baseURL, "A"),
              messages: [
                Message.user("First"),
                first.message,
                Message.user("Second"),
                second.message,
                Message.user("Third"),
              ],
            }),
            { webSocket: executor },
          ),
        ).pipe(Effect.provide(client))
        expect(server.state.opens).toBe(3)
        expect(server.state.headers.map((headers) => [headers.authorization, headers["chatgpt-account-id"]])).toEqual([
          ["Bearer fixture-token-A", "A"],
          ["Bearer fixture-token-B", "B"],
          ["Bearer fixture-token-A", "A"],
        ])
        expect(requests.map((entry) => entry.connectionID)).toEqual([0, 1, 2])
        expect(requests.map((entry) => entry.previousResponseID)).toEqual([undefined, undefined, undefined])
      }).pipe(Effect.provide(transportLayer))
    }).pipe(Effect.scoped, Effect.provide(WebSocketConstructor.layer)),
  )
})

test("a rejected WebSocket upgrade uses the same selected account for HTTP fallback", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const constructor = yield* Socket.WebSocketConstructor
      const server = yield* makeWebSocketServer({
        upgrade: () => false,
        http: async (request) => {
          await request.text()
          const item = {
            type: "message",
            id: "msg_fallback",
            status: "completed",
            role: "assistant",
            content: [{ type: "output_text", text: "Fallback reply", annotations: [], logprobs: [] }],
          }
          const events = [
            { type: "response.created", response: { id: "resp_fallback" } },
            { type: "response.output_item.added", item: { type: "message", id: "msg_fallback" } },
            { type: "response.output_text.delta", item_id: "msg_fallback", delta: "Fallback reply" },
            { type: "response.output_text.done", item_id: "msg_fallback", text: "Fallback reply" },
            { type: "response.output_item.done", item },
            { type: "response.completed", response: { id: "resp_fallback", output: [item] } },
          ]
          return new Response(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(""), {
            headers: { "content-type": "text/event-stream" },
          })
        },
      })
      const transportLayer = SessionModelTransport.makeLayer({
        open: (input) =>
          WebSocketTransport.open(input).pipe(Effect.provideService(Socket.WebSocketConstructor, constructor)),
      })
      yield* Effect.gen(function* () {
        const transport = yield* SessionModelTransport.Service
        const executor = transport.bind(Session.ID.make("ses_account_fallback"))
        const model = selectedModel(server.url.replace(/^ws/, "http").replace(/responses$/, ""), "B")
        const client = LLMClient.layer.pipe(
          Layer.provide(RequestExecutor.layer.pipe(Layer.provide(FetchHttpClient.layer))),
        )
        const result = yield* LLMClient.Service.use((llm) =>
          llm.generate(ai.request({ model, prompt: "Fallback" }), { webSocket: executor }),
        ).pipe(Effect.provide(client))
        expect(result.text).toBe("Fallback reply")
        expect(server.state.headers).toHaveLength(2)
        expect(server.state.headers.map((headers) => [headers.authorization, headers["chatgpt-account-id"]])).toEqual([
          ["Bearer fixture-token-B", "B"],
          ["Bearer fixture-token-B", "B"],
        ])
        expect(server.state.headers[0]?.upgrade).toBe("websocket")
        expect(server.state.headers[1]?.upgrade).toBeUndefined()
      }).pipe(Effect.provide(transportLayer))
    }).pipe(Effect.scoped, Effect.provide(WebSocketConstructor.layer)),
  )
})

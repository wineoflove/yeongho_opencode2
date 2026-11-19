import { expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { ConfigNormalize } from "@opencode/core/config/normalize"
import { ConfigAgentPlugin } from "@opencode/core/config/plugin/agent"
import { Document, Info } from "@opencode/schema/config"
import { Model } from "@opencode/schema/model"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { testEffect } from "../lib/effect"
import { agentHost, host } from "../plugin/host"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Agent.node, Bus.node, FSUtil.node, Global.node])))
const decode = Schema.decodeUnknownSync(Info)

test("normalizes an unbound legacy variant without selecting a model", () => {
  const result = ConfigNormalize.normalize({ agent: { assistant: { variant: "high" } } })
  expect(result.type).toBe("normalized")
  if (result.type !== "normalized") throw new Error("NORMALIZATION_FAILED")
  expect(decode(result.encoded).agents?.assistant).toEqual({ variant: "high" })
  expect(result.diagnostics).toEqual([])
})

for (const item of [
  { name: "unbound", later: {}, expected: undefined },
  {
    name: "project model",
    later: { agents: { assistant: { model: "openai/gpt-6-astra" } } },
    expected: "openai/gpt-6-astra#high",
  },
  {
    name: "explicit variant",
    later: { agents: { assistant: { model: "openai/gpt-6-astra#low" } } },
    expected: "openai/gpt-6-astra#low",
  },
  {
    name: "later native default",
    later: { agents: { assistant: { model: "openai/gpt-6-astra", variant: "low" } } },
    expected: "openai/gpt-6-astra#low",
  },
  {
    name: "embedded variant over native default",
    later: { agents: { assistant: { model: "openai/gpt-6-astra#low", variant: "high" } } },
    expected: "openai/gpt-6-astra#low",
  },
  {
    name: "disabled then recreated agent",
    later: [{ agents: { assistant: { disabled: true } } }, { agents: { assistant: { model: "openai/gpt-6-astra" } } }],
    expected: "openai/gpt-6-astra",
  },
]) {
  it.effect(`keeps the standalone default through ${item.name} merge`, () =>
    Effect.gen(function* () {
      const agents = yield* Agent.Service
      const initial = ConfigNormalize.normalize({ agent: { assistant: { variant: "high" } } })
      if (initial.type !== "normalized") throw new Error("NORMALIZATION_FAILED")
      const documents = [
        new Document({ type: "document", info: decode(initial.encoded) }),
        ...(Array.isArray(item.later) ? item.later : [item.later]).map(
          (value) => new Document({ type: "document", info: decode(value) }),
        ),
      ]
      yield* ConfigAgentPlugin.Plugin.effect(host({ agent: agentHost(agents) })).pipe(
        Effect.provide(Config.testLayer(documents)),
      )
      expect((yield* agents.get(Agent.ID.make("assistant")))?.model).toEqual(
        item.expected === undefined ? undefined : Model.Ref.parse(item.expected),
      )
    }),
  )
}

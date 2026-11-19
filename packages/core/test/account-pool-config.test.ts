import { expect, test } from "bun:test"
import { Schema } from "effect"
import { AccountPool } from "@opencode/core/account-pool"
import { Credential } from "@opencode/core/credential"

const parse = Schema.decodeUnknownSync(AccountPool.Config)

test.each([21])("rejects a pool with %s configured accounts", (count) => {
  expect(() =>
    parse({
      strategy: "round-robin",
      accounts: Array.from({ length: count }, (_, id) => ({ credentialID: `cred_${id}`, enabled: true })),
    }),
  ).toThrow()
})

test("rejects duplicate IDs even when only one copy is enabled", () => {
  expect(() =>
    parse({
      strategy: "round-robin",
      accounts: [
        { credentialID: "cred_A", enabled: true },
        { credentialID: "cred_A", enabled: false },
      ],
    }),
  ).toThrow()
})

test.each([0, 1, 20])("preserves configured order and disabled state for %s unique accounts", (count) => {
  const config = {
    strategy: "round-robin" as const,
    accounts: Array.from({ length: count }, (_, id) => ({
      credentialID: Credential.ID.make(`cred_${id}`),
      enabled: false,
    })),
  }
  expect(parse(config)).toEqual(config)
})

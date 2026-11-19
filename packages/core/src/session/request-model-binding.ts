import { HttpOptions, LanguageModel, LLMRequest } from "@opencode/ai/effect"
import type { ModelResolver } from "../model-resolver.js"

export function bindRequestModel(request: LLMRequest, selected: ModelResolver.Resolved) {
  return LLMRequest.update(request, {
    model: LanguageModel.update(selected.model, {
      route: selected.model.route.with({ endpoint: request.model.route.endpoint }),
    }),
    http: new HttpOptions({
      ...request.http,
      headers: Object.fromEntries(
        Object.entries(request.http?.headers ?? {}).filter(
          ([name]) => !["authorization", "x-api-key", "api-key", "chatgpt-account-id"].includes(name.toLowerCase()),
        ),
      ),
    }),
  })
}

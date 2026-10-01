import { describe, expect, test } from "bun:test"
import {
  Deferred,
  Effect,
  Exit,
  Fiber,
  Random,
  Redacted,
} from "effect"
import { TestClock } from "effect/testing"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  makeOfficialTurbopufferClient,
  type TurbopufferClientConfig,
  type TurbopufferClientService,
} from "../src/storage/turbopuffer/client.js"
import type { TurbopufferTransportFailed } from "../src/storage/turbopuffer/errors.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"

const profile = defineEmbeddingProfile({
  id: "test:client-boundary",
  version: "v1",
  dimensions: 3,
})

const partition = makeTurbopufferWorkspacePartition({
  workspace: "client-boundary-test",
  deploymentId: "test-deployment",
  endpoint: { _tag: "Region", region: "gcp-us-central1" },
  embeddingProfile: profile,
  schemaGeneration: 1,
})

interface RecordedRequest {
  readonly url: string
  readonly method: string
  readonly authorization: string | null
  readonly acceptEncoding: string | null
}

interface ProviderResponseFixture {
  readonly error?: {
    readonly message: string
  }
}

const jsonResponse = (
  body: ProviderResponseFixture,
  status = 200,
  headers?: HeadersInit,
): Response => {
  const responseHeaders = new Headers(headers)

  if (!responseHeaders.has("content-type")) {
    responseHeaders.set("content-type", "application/json")
  }

  return new Response(JSON.stringify(body), {
    status,
    headers: responseHeaders,
  })
}

const requestUrl = (input: string | URL | Request): string =>
  input instanceof Request ? input.url : String(input)

const withEnvironmentValue = <A>(
  name:
    | "TURBOPUFFER_BASE_URL"
    | "TURBOPUFFER_REGION"
    | "TURBOPUFFER_CUSTOM_HEADERS",
  value: string,
  run: () => A,
): A => {
  const previous = process.env[name]
  process.env[name] = value

  try {
    return run()
  } finally {
    if (previous === undefined) {
      delete process.env[name]
    } else {
      process.env[name] = previous
    }
  }
}

const recordingFetch = (
  requests: Array<RecordedRequest>,
  respond: (
    attempt: number,
    request: RecordedRequest,
    init: RequestInit | undefined,
  ) => Response | Promise<Response> = () => jsonResponse({}),
): NonNullable<TurbopufferClientConfig["fetch"]> =>
  async (input, init) => {
    const request = {
      url: requestUrl(input),
      method: init?.method ?? "GET",
      authorization: new Headers(init?.headers).get("authorization"),
      acceptEncoding: new Headers(init?.headers).get("accept-encoding"),
    }

    requests.push(request)

    return respond(requests.length, request, init)
  }

const clientConfig = (
  fetch: NonNullable<TurbopufferClientConfig["fetch"]>,
  input?: Partial<
    Pick<TurbopufferClientConfig, "retries" | "timeoutMilliseconds">
  >,
): TurbopufferClientConfig => ({
  apiKey: Redacted.make("sensitive-client-test-key"),
  partition,
  retries: input?.retries ?? 0,
  timeoutMilliseconds: input?.timeoutMilliseconds,
  fetch,
})

const captureFailure = async (
  effect: Effect.Effect<unknown, TurbopufferTransportFailed>,
): Promise<TurbopufferTransportFailed> => {
  const outcome = await Effect.runPromise(
    effect.pipe(
      Effect.map(() => ({ _tag: "Success" as const })),
      Effect.catch((error) =>
        Effect.succeed({ _tag: "Failure" as const, error })),
    ),
  )

  if (outcome._tag === "Success") {
    throw new Error("Expected Turbopuffer request to fail")
  }

  return outcome.error
}

const expectProviderDelay = async (
  headers: HeadersInit,
  expectedMilliseconds: number,
): Promise<void> => {
  const requests: Array<RecordedRequest> = []
  await Effect.runPromise(
    Effect.gen(function* () {
      const firstAttempt = yield* Deferred.make<void>()

      const client = makeOfficialTurbopufferClient(
        clientConfig(
          recordingFetch(requests, (attempt) => {
            if (attempt !== 1) return jsonResponse({})
            Deferred.doneUnsafe(firstAttempt, Exit.succeed(undefined))

            return jsonResponse(
              { error: { message: "retry later" } },
              429,
              headers,
            )
          }),
          { retries: 1 },
        ),
      )

      const fiber = yield* client.query({ rank_by: ["id", "asc"] }).pipe(
        Effect.forkChild,
      )

      yield* Deferred.await(firstAttempt)
      yield* TestClock.adjust(expectedMilliseconds - 1)
      expect(requests).toHaveLength(1)

      yield* TestClock.adjust(1)
      yield* Fiber.join(fiber)
    }).pipe(
      Random.withSeed("turbopuffer-retry-delay"),
      Effect.provide(TestClock.layer()),
    ),
  )
  expect(requests).toHaveLength(2)
}

type NamespaceOmitted<Request> =
  "namespace" extends keyof Request ? false : true

describe("Turbopuffer client boundary", () => {
  test("uses a redacted canonical API key and never includes rejected input in diagnostics", async () => {
    const requests: Array<RecordedRequest> = []
    const apiKey = Redacted.make("sensitive-client-test-key")

    const config: TurbopufferClientConfig = {
      apiKey,
      partition,
      retries: 0,
      fetch: recordingFetch(requests),
    }

    const client = makeOfficialTurbopufferClient(config)

    await Effect.runPromise(client.query({ rank_by: ["id", "asc"] }))

    expect(requests).toHaveLength(1)
    expect(requests[0]?.authorization).toBe(
      "Bearer sensitive-client-test-key",
    )
    expect(String(config.apiKey)).toBe("<redacted>")
    expect(JSON.stringify(config)).not.toContain("sensitive-client-test-key")

    for (const rawValue of ["", "   ", " sensitive-client-test-key "]) {
      let cause: unknown

      try {
        makeOfficialTurbopufferClient({
          ...config,
          apiKey: Redacted.make(rawValue),
        })
      } catch (error: unknown) {
        cause = error
      }

      expect(cause).toMatchObject({
        _tag: "InvalidTurbopufferConfiguration",
        field: "api_key",
        reason: "invalid_value",
      })
      expect(String(cause)).not.toContain("sensitive-client-test-key")
      expect(JSON.stringify(cause)).not.toContain("sensitive-client-test-key")
    }
  })

  test("asks for gzipped responses without compressing requests", async () => {
    const requests: Array<RecordedRequest> = []
    const client = makeOfficialTurbopufferClient(clientConfig(recordingFetch(requests)))

    const plain: Array<RecordedRequest> = []

    const uncompressed = makeOfficialTurbopufferClient({
      ...clientConfig(recordingFetch(plain)),
      compressResponses: false,
    })

    await Effect.runPromise(client.query({ rank_by: ["id", "asc"] }))
    await Effect.runPromise(uncompressed.query({ rank_by: ["id", "asc"] }))

    expect(requests[0]?.acceptEncoding).toBe("gzip")
    expect(plain[0]?.acceptEncoding).toBe("identity")
  })

  test("keeps the configured API key authoritative over ambient custom headers", async () => {
    const requests: Array<RecordedRequest> = []

    const client = withEnvironmentValue(
      "TURBOPUFFER_CUSTOM_HEADERS",
      "Authorization: Bearer ambient-client-test-key",
      () => makeOfficialTurbopufferClient({
        apiKey: Redacted.make("configured-client-test-key"),
        partition,
        retries: 0,
        fetch: recordingFetch(requests),
      }),
    )

    await Effect.runPromise(client.query({ rank_by: ["id", "asc"] }))

    expect(requests).toHaveLength(1)
    expect(requests[0]?.authorization).toBe(
      "Bearer configured-client-test-key",
    )
  })

  test("derives a custom base URL solely from the canonical partition", async () => {
    const requests: Array<RecordedRequest> = []

    const customPartition = makeTurbopufferWorkspacePartition({
      workspace: "custom-client-boundary-test",
      deploymentId: "test-custom-deployment",
      endpoint: {
        _tag: "Custom",
        baseURL: "https://gateway.example.test/turbopuffer/",
      },
      embeddingProfile: profile,
      schemaGeneration: 1,
    })

    const client = withEnvironmentValue(
      "TURBOPUFFER_REGION",
      "ambient-region",
      () => makeOfficialTurbopufferClient({
        apiKey: Redacted.make("sensitive-client-test-key"),
        partition: customPartition,
        retries: 0,
        fetch: recordingFetch(requests),
      }),
    )

    await Effect.runPromise(client.query({ rank_by: ["id", "asc"] }))

    expect(requests.map((request) => request.url)).toEqual([
      `https://gateway.example.test/turbopuffer/v2/namespaces/${customPartition.namespace}/query`,
    ])
  })

  test("derives a regional base URL solely from the canonical partition", async () => {
    const requests: Array<RecordedRequest> = []

    const client = withEnvironmentValue(
      "TURBOPUFFER_BASE_URL",
      "https://ambient.example.test/{region}",
      () => makeOfficialTurbopufferClient(
        clientConfig(recordingFetch(requests)),
      ),
    )

    await Effect.runPromise(client.query({ rank_by: ["id", "asc"] }))

    expect(requests.map((request) => request.url)).toEqual([
      `https://gcp-us-central1.turbopuffer.com/v2/namespaces/${partition.namespace}/query`,
    ])
  })

  test("omits namespace from service inputs and overwrites wider-object overrides", async () => {
    const requestTypesOmitNamespace: readonly [
      NamespaceOmitted<Parameters<TurbopufferClientService["write"]>[0]>,
      NamespaceOmitted<Parameters<TurbopufferClientService["query"]>[0]>,
      NamespaceOmitted<
        Parameters<TurbopufferClientService["multiQuery"]>[0]
      >,
      NamespaceOmitted<
        Parameters<TurbopufferClientService["updateSchema"]>[0]
      >,
    ] = [
      true,
      true,
      true,
      true,
    ]

    expect(requestTypesOmitNamespace).toEqual([true, true, true, true])

    const requests: Array<RecordedRequest> = []

    const client = makeOfficialTurbopufferClient(
      clientConfig(recordingFetch(requests)),
    )

    const override = "other-workspace"

    const writeRequest: Parameters<
      TurbopufferClientService["write"]
    >[0] & { readonly namespace: string } = {
      deletes: [],
      namespace: override,
    }

    const queryRequest: Parameters<
      TurbopufferClientService["query"]
    >[0] & { readonly namespace: string } = {
      rank_by: ["id", "asc"],
      namespace: override,
    }

    const multiQueryRequest: Parameters<
      TurbopufferClientService["multiQuery"]
    >[0] & { readonly namespace: string } = {
      queries: [{ rank_by: ["id", "asc"] }],
      namespace: override,
    }

    const updateSchemaRequest: Parameters<
      TurbopufferClientService["updateSchema"]
    >[0] & { readonly namespace: string } = {
      schema: {},
      namespace: override,
    }

    await Effect.runPromise(client.write(writeRequest))
    await Effect.runPromise(client.query(queryRequest))
    await Effect.runPromise(client.multiQuery(multiQueryRequest))
    await Effect.runPromise(client.updateSchema(updateSchemaRequest))

    const namespace = partition.namespace
    expect(requests.map((request) => request.url)).toEqual([
      `https://gcp-us-central1.turbopuffer.com/v2/namespaces/${namespace}`,
      `https://gcp-us-central1.turbopuffer.com/v2/namespaces/${namespace}/query`,
      `https://gcp-us-central1.turbopuffer.com/v2/namespaces/${namespace}/query?stainless_overload=multiQuery`,
      `https://gcp-us-central1.turbopuffer.com/v1/namespaces/${namespace}/schema`,
    ])
  })

  test("disables the SDK write retry override per request", async () => {
    const requests: Array<RecordedRequest> = []

    const client = makeOfficialTurbopufferClient(
      clientConfig(recordingFetch(
        requests,
        () => jsonResponse(
          { error: { message: "temporary failure" } },
          500,
          { "retry-after-ms": "1" },
        ),
      )),
    )

    const failure = await captureFailure(client.write({ deletes: ["row-1"] }))

    expect(requests).toHaveLength(1)
    expect(failure.reason).toBe("unavailable")
  })

  test("classifies lock conflicts and retries them through Effect", async () => {
    const requests: Array<RecordedRequest> = []

    const client = makeOfficialTurbopufferClient(
      clientConfig(
        recordingFetch(
          requests,
          () => jsonResponse({ error: { message: "lock timeout" } }, 409),
        ),
        { retries: 1 },
      ),
    )

    const failure = await captureFailure(
      client.query({ rank_by: ["id", "asc"] }),
    )

    expect(requests).toHaveLength(2)
    expect(failure.reason).toBe("conflict")
  })

  test("keeps the aggregate write outcome unknown after an ambiguous attempt", async () => {
    const requests: Array<RecordedRequest> = []

    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const firstAttempt = yield* Deferred.make<void>()

        const client = makeOfficialTurbopufferClient(
          clientConfig(
            recordingFetch(requests, (attempt) => {
              if (attempt === 1) {
                Deferred.doneUnsafe(firstAttempt, Exit.succeed(undefined))

                return jsonResponse(
                  { error: { message: "ambiguous provider failure" } },
                  500,
                )
              }

              return jsonResponse(
                { error: { message: "definite final rejection" } },
                400,
              )
            }),
            { retries: 1 },
          ),
        )

        const fiber = yield* client.write({ deletes: ["row-1"] }).pipe(
          Effect.result,
          Effect.forkChild,
        )

        yield* Deferred.await(firstAttempt)
        yield* TestClock.adjust("1 second")

        return yield* Fiber.join(fiber)
      }).pipe(
        Random.withSeed("turbopuffer-aggregate-write-outcome"),
        Effect.provide(TestClock.layer()),
      ),
    )

    expect(requests).toHaveLength(2)
    expect(outcome).toMatchObject({
      failure: { reason: "rejected", requestOutcome: "unknown" },
    })
  })

  test("obeys explicit provider retry decisions", async () => {
    const deniedRequests: Array<RecordedRequest> = []

    const deniedClient = makeOfficialTurbopufferClient(
      clientConfig(
        recordingFetch(
          deniedRequests,
          () => jsonResponse(
            { error: { message: "do not retry" } },
            500,
            { "x-should-retry": "false" },
          ),
        ),
        { retries: 2 },
      ),
    )

    const deniedFailure = await captureFailure(
      deniedClient.query({ rank_by: ["id", "asc"] }),
    )

    expect(deniedRequests).toHaveLength(1)
    expect(deniedFailure.providerRetryDirective).toBe("do_not_retry")
    expect(deniedFailure.requestOutcome).toBe("unknown")

    const allowedRequests: Array<RecordedRequest> = []
    await Effect.runPromise(
      Effect.gen(function* () {
        const firstAttempt = yield* Deferred.make<void>()

        const allowedClient = makeOfficialTurbopufferClient(
          clientConfig(
            recordingFetch(allowedRequests, (attempt) => {
              if (attempt !== 1) return jsonResponse({})
              Deferred.doneUnsafe(firstAttempt, Exit.succeed(undefined))

              return jsonResponse(
                { error: { message: "retry this rejection" } },
                400,
                { "x-should-retry": "true" },
              )
            }),
            { retries: 1 },
          ),
        )

        const fiber = yield* allowedClient.query({
          rank_by: ["id", "asc"],
        }).pipe(Effect.forkChild)

        yield* Deferred.await(firstAttempt)
        yield* TestClock.adjust("1 second")
        yield* Fiber.join(fiber)
      }).pipe(
        Random.withSeed("turbopuffer-retry-override"),
        Effect.provide(TestClock.layer()),
      ),
    )

    expect(allowedRequests).toHaveLength(2)
  })

  test("honors every provider retry delay format and caps excessive delays", async () => {
    await expectProviderDelay({ "retry-after-ms": "5000" }, 5_000)
    await expectProviderDelay({ "retry-after": "5" }, 5_000)
    await expectProviderDelay(
      { "retry-after": new Date(5_000).toUTCString() },
      5_000,
    )
    await expectProviderDelay(
      { "retry-after-ms": String(60 * 60 * 1_000) },
      30 * 60 * 1_000,
    )
  })

  test("interrupts a provider-directed retry wait without another request", async () => {
    const requests: Array<RecordedRequest> = []

    const interrupted = await Effect.runPromise(
      Effect.gen(function* () {
        const firstAttempt = yield* Deferred.make<void>()

        const client = makeOfficialTurbopufferClient(
          clientConfig(
            recordingFetch(requests, () => {
              Deferred.doneUnsafe(firstAttempt, Exit.succeed(undefined))

              return jsonResponse(
                { error: { message: "retry later" } },
                429,
                { "retry-after-ms": "5000" },
              )
            }),
            { retries: 1 },
          ),
        )

        const fiber = yield* client.query({ rank_by: ["id", "asc"] }).pipe(
          Effect.forkChild,
        )

        yield* Deferred.await(firstAttempt)
        yield* Fiber.interrupt(fiber)
        const exit = yield* Fiber.await(fiber)
        yield* TestClock.adjust("5 seconds")

        return Exit.hasInterrupts(exit)
      }).pipe(
        Random.withSeed("turbopuffer-retry-cancellation"),
        Effect.provide(TestClock.layer()),
      ),
    )

    expect(interrupted).toBe(true)
    expect(requests).toHaveLength(1)
  })

  test("keeps authentication and permission failures distinct from rejections", async () => {
    const cases = [
      { status: 401, reason: "authentication_failed" as const },
      { status: 403, reason: "permission_denied" as const },
    ]

    for (const fixture of cases) {
      const requests: Array<RecordedRequest> = []

      const client = makeOfficialTurbopufferClient(
        clientConfig(
          recordingFetch(
            requests,
            () => jsonResponse({ error: { message: "denied" } }, fixture.status),
          ),
          { retries: 2 },
        ),
      )

      const failure = await captureFailure(
        client.query({ rank_by: ["id", "asc"] }),
      )

      expect(requests).toHaveLength(1)
      expect(failure.reason).toBe(fixture.reason)
      expect(failure.requestOutcome).toBe("definitely_not_applied")
      expect(failure.reason).not.toBe("rejected")
    }
  })

  test("classifies the SDK timeout subclass before the base API error", async () => {
    const requests: Array<RecordedRequest> = []

    const fetch = recordingFetch(requests, (_attempt, _request, init) =>
      new Promise<Response>((_resolve, reject) => {
        const signal = init?.signal

        if (signal === undefined || signal === null) {
          reject(new Error("Expected the SDK to provide a timeout signal"))

          return
        }

        const rejectOnAbort = () =>
          reject(new DOMException("The operation was aborted", "AbortError"))

        if (signal.aborted) {
          rejectOnAbort()

          return
        }

        signal.addEventListener("abort", rejectOnAbort, { once: true })
      }))

    const client = makeOfficialTurbopufferClient(
      clientConfig(fetch, { timeoutMilliseconds: 1 }),
    )

    const failure = await captureFailure(
      client.query({ rank_by: ["id", "asc"] }),
    )

    expect(requests).toHaveLength(1)
    expect(failure.reason).toBe("timed_out")
  })
})

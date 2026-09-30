interface Settlement<Result> {
  readonly resolve: (result: Result) => void
  readonly reject: (cause: Error) => void
}

interface PendingBatch<Request, Result> {
  readonly requests: Array<Request>
  readonly settlements: Array<Settlement<Result>>
  readonly timer: ReturnType<typeof setTimeout>
}

/** Requests that arrive together, grouped by a key and answered by one batched call. */
export interface RequestCoalescer<Request, Result> {
  readonly submit: (key: string, request: Request) => Promise<Result>
}

/**
 * Collect requests that share a key for a short window, then answer them with
 * one call. A batch flushes when its window ends or when it is full; results
 * return in request order, and a failed or short batch fails every request in
 * it. State belongs to one instance, so an instance must not outlive the
 * scope its promises may settle in, such as one Workers request.
 */
export const makeRequestCoalescer = <Request, Result>(options: {
  readonly windowMilliseconds: number
  readonly maximumBatch: number
  readonly run: (key: string, requests: ReadonlyArray<Request>) => Promise<ReadonlyArray<Result>>
}): RequestCoalescer<Request, Result> => {
  const pending = new Map<string, PendingBatch<Request, Result>>()

  const flush = (key: string): void => {
    const batch = pending.get(key)
    if (batch === undefined) return
    pending.delete(key)
    clearTimeout(batch.timer)

    options.run(key, batch.requests).then(
      (results) => {
        if (results.length !== batch.requests.length) {
          const failure = new Error("A coalesced batch returned a different number of results than requests")
          for (const { reject } of batch.settlements) reject(failure)
          return
        }

        results.forEach((result, index) => batch.settlements[index]?.resolve(result))
      },
      (cause: Error) => {
        for (const { reject } of batch.settlements) reject(cause)
      },
    )
  }

  return {
    submit: (key, request) =>
      new Promise<Result>((resolve, reject) => {
        const existing = pending.get(key)
        const batch = existing ?? {
          requests: [],
          settlements: [],
          timer: setTimeout(() => flush(key), options.windowMilliseconds),
        }

        if (existing === undefined) pending.set(key, batch)
        batch.requests.push(request)
        batch.settlements.push({ resolve, reject })

        if (batch.requests.length >= options.maximumBatch) flush(key)
      }),
  }
}

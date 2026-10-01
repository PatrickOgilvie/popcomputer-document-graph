import { Effect } from "effect"
import { d1ProjectionPublicationCoordinator } from "../src/storage/d1/projection-publication.js"
import { describePublicationCoordinatorConformance } from "./support/projection-publication-coordinator-suite.js"
import { makeDatabase } from "./support/sqlite-d1.js"

describePublicationCoordinatorConformance("D1", async () => {
  const database = await makeDatabase()

  return {
    run: (effect, options = {}) => Effect.runPromise(effect.pipe(
      Effect.provide(d1ProjectionPublicationCoordinator({
        database,
        indexGeneration: options.indexGeneration ?? "schema-v1",
        publicationLeaseMilliseconds: 1_000,
        retainedPublicationHistory: options.retainedPublicationHistory,
      })),
    )),
    close: async () => database.close(),
  }
})

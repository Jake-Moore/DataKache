package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.api.ordering.OperationTime
import com.mongodb.kotlin.client.coroutine.ClientSession

/**
 * The cluster time of the write this session just performed, or `null` if the driver did not
 * report one.
 *
 * The driver tracks it already, so reading it costs nothing, unlike asking the deployment for the
 * current cluster time which is a round trip. It is only absent when a command reply carries no
 * `$clusterTime`, which every deployment that supports change streams -- a replica set or a sharded
 * cluster, already a hard requirement for this cache to function at all -- populates on every reply
 * since MongoDB 3.6. So `null` here means an unsupported deployment, not a normal occurrence.
 *
 * There is no safe sentinel to substitute. Treating it as the oldest possible time would let the
 * write's cache update be silently refused by state that is actually stale, since it compares as
 * older than everything; treating it as the newest possible time would let it silently win over
 * state that is genuinely newer, since it compares as newer than everything, and unlike the first
 * case that failure does not repair itself. Callers should skip the cache update rather than guess:
 * the write already reached MongoDB, so the change stream will eventually deliver an event for it
 * that carries a real cluster time, and the cache catches up then instead of being wrong now.
 *
 * Read through [ClientSession.wrapped] rather than off the Kotlin session directly, because
 * `ClientSession.getOperationTime()` is declared to return a non-null `BsonTimestamp` while the
 * driver core it delegates to returns a field that starts null and is only assigned once a reply
 * carries an operation time. The Kotlin wrapper therefore compiles to an
 * `Intrinsics.checkNotNullExpressionValue` and throws on exactly the case this function exists to
 * report, which would fail an already-committed write instead of degrading to the change stream.
 * The wrapped reactive session declares the same getter unannotated, but its package carries the
 * driver's `@NonNullApi`, so Kotlin infers non-null and calls the safe call below unnecessary. That
 * inference is JSR-305 and applied at WARN strictness, not enforced: the compiler emits an `ifnull`
 * branch and no `Intrinsics` check, so the null really does come back. Suppressed rather than
 * removed, because removing it would restore the throw this exists to avoid.
 */
@Suppress("UNNECESSARY_SAFE_CALL")
internal fun ClientSession.operationTimeOrNull(): OperationTime? =
    this.wrapped.operationTime?.let { OperationTime(it.value) }

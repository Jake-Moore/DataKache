package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.api.ordering.OperationTime
import com.mongodb.kotlin.client.coroutine.ClientSession

/**
 * The cluster time of the operation this session last performed.
 *
 * The driver tracks it already, so reading it costs nothing, unlike asking the deployment for the
 * current cluster time which is a round trip. Falls back to [OperationTime.UNKNOWN] rather than
 * throwing, so a driver that does not report one degrades to the previous behaviour of applying
 * unconditionally instead of failing a write.
 */
internal fun ClientSession.operationTimeOrUnknown(): OperationTime =
    this.operationTime?.let { OperationTime(it.value) } ?: OperationTime.UNKNOWN

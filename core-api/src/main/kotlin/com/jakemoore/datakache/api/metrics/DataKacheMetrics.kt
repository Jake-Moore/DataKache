package com.jakemoore.datakache.api.metrics

import com.jakemoore.datakache.api.DataKacheClient
import org.jetbrains.annotations.ApiStatus
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

/**
 * Primary API class for viewing and receiving DataKache metrics.
 */
@Suppress("unused")
object DataKacheMetrics {
    private val receiverMap = ConcurrentHashMap<String, MetricsReceiver>()

    /**
     * READ-ONLY access to the registered [MetricsReceiver]s.
     */
    internal val receivers: Collection<MetricsReceiver>
        get() = Collections.unmodifiableCollection(receiverMap.values)

    /**
     * Register a custom [MetricsReceiver] for your [DataKacheClient].
     * - Uses the [DataKacheClient.name] as the receiver ID.
     *
     * NOTE: Your client can only have one receiver registered at a time.
     *
     * If you wish to register additional receivers, register them under custom ids with:
     * - [registerReceiverByID]
     *
     * Throws an [IllegalArgumentException] if a receiver with the same ID is already registered.
     */
    @Throws(IllegalArgumentException::class)
    fun registerReceiver(client: DataKacheClient, receiver: MetricsReceiver) {
        return registerReceiverByID(client.name, receiver)
    }

    /**
     * Register a custom [MetricsReceiver] with a specific ID.
     *
     * This allows you to register multiple receivers under different IDs.
     *
     * You can also register a receiver using your [DataKacheClient] using:
     * - [registerReceiver]
     *
     * Throws an [IllegalArgumentException] if a receiver with the same ID is already registered.
     */
    @Throws(IllegalArgumentException::class)
    fun registerReceiverByID(receiverID: String, receiver: MetricsReceiver) {
        require(!receiverMap.containsKey(receiverID)) {
            "A MetricsReceiver with ID '$receiverID' is already registered."
        }
        receiverMap[receiverID] = receiver
    }

    /**
     * Unregister a [MetricsReceiver] by its ID.
     *
     * @return true if the receiver was successfully removed, false if no receiver with that ID was found.
     */
    fun unregisterReceiverByID(receiverID: String): Boolean {
        return receiverMap.remove(receiverID) != null
    }

    /**
     * Unregister a [MetricsReceiver] by its [DataKacheClient.name].
     *
     * @return true if the receiver was successfully removed, false if no receiver with that name was found.
     */
    fun unregisterReceiver(client: DataKacheClient): Boolean {
        return unregisterReceiverByID(client.name)
    }

    /**
     * READ-ONLY access to the registered [MetricsReceiver]s.
     *
     * FOR INTERNAL USE ONLY.
     */
    @ApiStatus.Internal
    fun getReceiversInternal(): Collection<MetricsReceiver> {
        return receivers
    }
}

package com.jakemoore.datakache.core.connections.mongo

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.jakemoore.datakache.util.RollingAverage
import com.mongodb.connection.ServerDescription
import com.mongodb.event.ClusterClosedEvent
import com.mongodb.event.ClusterDescriptionChangedEvent
import com.mongodb.event.ClusterListener
import com.mongodb.event.ServerHeartbeatSucceededEvent
import com.mongodb.event.ServerMonitorListener
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Listens to MongoDB cluster events and server heartbeats to monitor connection status and ping times.
 */
internal class MongoListener(private val service: MongoDatabaseService) : ClusterListener, ServerMonitorListener {
    internal val listenerID: UUID = UUID.randomUUID()

    /**
     * Listen to cluster status which is used for status updates and connection pings.
     */
    override fun clusterDescriptionChanged(event: ClusterDescriptionChangedEvent) {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event

        val oldState = event.previousDescription.hasWritableServer()
        val newState = event.newDescription.hasWritableServer()

        // Mark the service as connected if we have a writable server now
        if (!oldState && newState) {
            // Update connected state
            service.mongoConnected = true

            // Inform console about the connection status
            if (!service.mongoFirstConnect) {
                service.mongoFirstConnect = true
                service.info("&aMongoDB initial connection succeeded.")
                service.info("&aPlayers may now join the server.")
            } else {
                service.debug("&aMongoDB connection reestablished.")
            }
        }

        // Mark the service as disconnected if we had a writable server before but not now
        if (oldState && !newState) {
            // Update connected state
            service.mongoConnected = false

            // Inform console about the disconnection status
            service.warn("&cMongoDB connection lost.")
        }

        // Update the ping value for servers in the cluster
        if (event.newDescription.serverDescriptions.isNotEmpty()) {
            updateClusterServerPings(event.newDescription.serverDescriptions)
        }
    }

    /**
     * Listen for cluster closed events, which indicates that the MongoDB cluster is no longer available.
     */
    override fun clusterClosed(event: ClusterClosedEvent) {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event

        // Update connected state
        service.mongoConnected = false

        // Inform console about the cluster closure
        if (service.keepMongoConnected) {
            // ISSUE! Mongo is supposed to be connected, but cluster was closed!
            service.warn("&cMongoDB cluster closed.")
        } else {
            // NORMAL - Mongo does not need to be connected, cluster was closed intentionally
            service.info("&aMongoDB cluster closed.")
        }
    }

    /**
     * Listen to heartbeats in order to update the ping time of the MongoDB connection.
     */
    override fun serverHeartbeatSucceeded(event: ServerHeartbeatSucceededEvent) {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event

        val client = service.mongoClient
        if (client != null) {
            // Attempt to use the current client's cluster description to make sure the ping is accurate
            updateClusterServerPings(client.getClusterDescription().serverDescriptions)
        }
    }

    // Map <server address, ping rolling average>
    private val pingAverages: Cache<String, RollingAverage> = CacheBuilder
        .newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build()

    private fun updateClusterServerPings(servers: List<ServerDescription>) {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event

        for (server in servers) {
            val rolling = pingAverages.asMap().computeIfAbsent(server.address.toString()) {
                RollingAverage(30)
            }
            rolling.add(server.roundTripTimeNanos)
        }
        recalculateAveragePing()
    }

    private fun recalculateAveragePing() {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event
        if (!service.mongoConnected) return

        // Update service serverPingMap with rolling averages
        var count = 0
        var sumNanos = 0L
        for ((address, rolling) in pingAverages.asMap()) {
            val average = rolling.average()
            // Only update if the rolling average is valid
            service.serverPingMap.put(address, average)

            // Keep track of local average nanos
            sumNanos += average
            count++
        }

        // Set average ping nanos
        this.service.averagePingNanos = if (count > 0) {
            sumNanos / count
        } else {
            -1L // No servers available, set to -1 to indicate no ping
        }

        val ms = this.service.averagePingNanos / 1_000_000
        System.err.println(
            "[recalculateAveragePing] Average Ping: ${service.averagePingNanos} nanos ($ms ms) " +
                "from ${pingAverages.size()} servers."
        )
    }
}

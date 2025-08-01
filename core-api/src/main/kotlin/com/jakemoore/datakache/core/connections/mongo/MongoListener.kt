package com.jakemoore.datakache.core.connections.mongo

import com.mongodb.connection.ServerDescription
import com.mongodb.event.ClusterClosedEvent
import com.mongodb.event.ClusterDescriptionChangedEvent
import com.mongodb.event.ClusterListener
import com.mongodb.event.ServerHeartbeatSucceededEvent
import com.mongodb.event.ServerMonitorListener
import java.util.UUID

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

    private fun updateClusterServerPings(servers: List<ServerDescription>) {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event

        for (server in servers) {
            service.serverPingMap.put(server.address.toString(), server.roundTripTimeNanos)
        }
        recalculateAveragePing()
    }

    private fun recalculateAveragePing() {
        if (service.activeListener != listenerID) return // This listener is not the active one, ignore the event

        if (!service.mongoConnected) return

        var pingSumNanos = 0L
        for (ping in service.serverPingMap.asMap().values) {
            pingSumNanos += ping
        }

        // Set average ping nanos
        this.service.averagePingNanos = if (service.serverPingMap.size() > 0) {
            pingSumNanos / service.serverPingMap.size()
        } else {
            -1L // No servers available, set to -1 to indicate no ping
        }
    }
}

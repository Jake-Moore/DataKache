package com.jakemoore.datakache.core.connections.mongo.changestream

import com.mongodb.client.model.changestream.ChangeStreamDocument

/**
 * Something the change stream's producer hands to its ordered buffer.
 *
 * A reposition travels through the buffer rather than being announced directly, because it has to
 * take effect **in stream order**. The producer notices a reposition the moment it reconnects, while
 * the consumer may still be draining events from the connection before it. Announcing it directly
 * would apply it to those events too, which is the opposite of what it means.
 */
internal sealed interface StreamItem<D : Any> {
    /** A change event, to be applied to the cache. */
    class Event<D : Any>(val change: ChangeStreamDocument<D>) : StreamItem<D>

    /**
     * The stream reconnected somewhere it may not have reached before, so everything after this
     * point may be older than what came before it.
     */
    class Reposition<D : Any> : StreamItem<D>
}

package com.jakemoore.datakache.test.unit

import com.jakemoore.datakache.core.connections.mongo.changestream.ConnectionSequence
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe

/**
 * The cache stops forgetting removed keys' positions when the change stream reconnects, because a
 * reconnection can resume from an earlier point and replay history. Getting the answer to "is this a
 * reconnection" wrong makes that protection silently inert, which is what happened when it was
 * derived from the stream's state machine, so the derivation is a unit of its own and tested here.
 */
@Suppress("unused")
class TestConnectionSequence :
    DescribeSpec({
        describe("Connection Sequence") {

            it("should report the first connection as not a reconnection") {
                ConnectionSequence().observeConnection().shouldBe(false)
            }

            it("should report every connection after the first as a reconnection") {
                val sequence = ConnectionSequence()

                sequence.observeConnection().shouldBe(false)
                sequence.observeConnection().shouldBe(true)
                sequence.observeConnection().shouldBe(true)
            }

            it("should track each stream separately") {
                val first = ConnectionSequence()
                val second = ConnectionSequence()

                first.observeConnection().shouldBe(false)
                first.observeConnection().shouldBe(true)

                // A stream that has never connected is on its first connection regardless.
                second.observeConnection().shouldBe(false)
            }
        }
    })

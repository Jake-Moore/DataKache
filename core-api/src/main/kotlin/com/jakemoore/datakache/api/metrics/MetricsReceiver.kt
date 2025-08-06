package com.jakemoore.datakache.api.metrics

import com.jakemoore.datakache.api.metrics.receiver.ChangeStreamReceiver
import com.jakemoore.datakache.api.metrics.receiver.DatabaseReceiver
import com.jakemoore.datakache.api.metrics.receiver.DocCacheReceiver
import com.jakemoore.datakache.api.metrics.receiver.PlayerDocCacheReceiver
import com.jakemoore.datakache.api.metrics.receiver.UniqueIndexReceiver

/**
 * Primary metrics receiver interface, compiling all sub-metrics interfaces.
 *
 * If we wish for a more robust and flexible metrics class, see [MetricsReceiverPartial].
 */
interface MetricsReceiver :
    DatabaseReceiver,
    DocCacheReceiver,
    PlayerDocCacheReceiver,
    ChangeStreamReceiver,
    UniqueIndexReceiver

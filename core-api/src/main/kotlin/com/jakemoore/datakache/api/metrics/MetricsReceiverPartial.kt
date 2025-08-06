package com.jakemoore.datakache.api.metrics

/**
 * Secondary, more flexible, metrics receiver interface.
 *
 * For the primary metrics receiver interface, see [MetricsReceiver].
 */
open class MetricsReceiverPartial : MetricsReceiver {
    override fun onDatabaseInsert() {}

    override fun onDatabaseUpdate() {}

    override fun onDatabaseRead() {}

    override fun onDatabaseDelete() {}

    override fun onDatabaseReadAll() {}

    override fun onDatabaseSize() {}

    override fun onDatabaseHasKey() {}

    override fun onDatabaseClear() {}

    override fun onDatabaseReadKeys() {}

    override fun onDatabaseReplace() {}

    override fun onDatabaseInsertFail() {}

    override fun onDatabaseUpdateFail() {}

    override fun onDatabaseUpdateDocNotFoundFail() {}

    override fun onDatabaseReadFail() {}

    override fun onDatabaseDeleteFail() {}

    override fun onDatabaseReadAllFail() {}

    override fun onDatabaseSizeFail() {}

    override fun onDatabaseHasKeyFail() {}

    override fun onDatabaseClearFail() {}

    override fun onDatabaseReadKeysFail() {}

    override fun onDatabaseReplaceFail() {}

    override fun onDatabaseUpdateTransactionLimitReached() {}

    override fun onDatabaseUpdateTransactionAttemptStart() {}

    override fun onDatabaseUpdateTransactionAttemptTime(milliseconds: Long) {}

    override fun onDatabaseUpdateTransactionAttemptsRequired(attempts: Int) {}

    override fun onDatabaseUpdateTransactionSuccess(milliseconds: Long) {}

    override fun onGenericDocCreate() {}

    override fun onDocRead() {}

    override fun onDocDelete() {}

    override fun onDocUpdate() {}

    override fun onDocRejectableUpdate() {}

    override fun onGenericDocCreateFail() {}

    override fun onGenericDocCreateDuplicateFail() {}

    override fun onDocReadFail() {}

    override fun onDocDeleteFail() {}

    override fun onDocUpdateFail() {}

    override fun onDocUpdateNotFoundFail() {}

    override fun onDocRejectableUpdateFail() {}

    override fun onDocRejectableUpdateNotFoundFail() {}

    override fun onPlayerDocClear() {}

    override fun onPlayerDocCreate() {}

    override fun onPlayerDocClearFail() {}

    override fun onPlayerDocCreateFail() {}
}

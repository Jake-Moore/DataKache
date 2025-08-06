package com.jakemoore.datakache.api.metrics.receiver

/**
 * Metrics receiver for ALL DocCache operations and failures.
 */
interface DocCacheReceiver {
    // CRUD Operations
    fun onGenericDocCreate()
    fun onDocRead()
    fun onDocDelete()
    fun onDocUpdate()
    fun onDocRejectableUpdate()

    // Fail States
    fun onGenericDocCreateFail()
    fun onGenericDocCreateDuplicateFail()
    fun onDocReadFail()
    fun onDocDeleteFail()
    fun onDocUpdateFail()
    fun onDocUpdateNotFoundFail()
    fun onDocRejectableUpdateFail()
    fun onDocRejectableUpdateNotFoundFail()
}

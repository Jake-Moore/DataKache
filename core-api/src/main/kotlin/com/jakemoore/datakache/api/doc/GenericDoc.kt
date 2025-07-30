package com.jakemoore.datakache.api.doc

abstract class GenericDoc<D : GenericDoc<D>> : Doc<String, D> {
    abstract override val key: String
    abstract override val version: Long
}

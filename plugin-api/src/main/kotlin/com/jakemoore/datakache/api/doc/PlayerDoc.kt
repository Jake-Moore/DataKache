package com.jakemoore.datakache.api.doc

import com.jakemoore.datakache.core.serialization.java.UUIDSerializer
import kotlinx.serialization.Serializable
import java.util.UUID

@Suppress("unused")
abstract class PlayerDoc<D : PlayerDoc<D>> : Doc<UUID, D> {
    @Serializable(with = UUIDSerializer::class)
    abstract override val key: UUID
    abstract override val version: Long
    abstract val username: String?
}

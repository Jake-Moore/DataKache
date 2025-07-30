package com.jakemoore.datakache.api.doc

import com.jakemoore.datakache.core.serialization.java.UUIDSerializer
import kotlinx.serialization.Serializable
import java.util.UUID

abstract class PlayerDoc<D : PlayerDoc<D>> : Doc<UUID, D> {
    @Serializable(with = UUIDSerializer::class)
    abstract override val id: UUID
    abstract override val version: Long
    abstract val username: String?
}

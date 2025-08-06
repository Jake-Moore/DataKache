package com.jakemoore.datakache.api.doc

import com.jakemoore.datakache.api.cache.DocCache
import com.jakemoore.datakache.api.cache.PlayerDocCache
import com.jakemoore.datakache.api.serialization.java.UUIDSerializer
import com.jakemoore.datakache.util.PlayerUtil
import kotlinx.serialization.Serializable
import org.bukkit.Bukkit
import org.bukkit.entity.Player
import java.util.Objects
import java.util.UUID

@Suppress("unused")
abstract class PlayerDoc<D : PlayerDoc<D>> : Doc<UUID, D> {
    @Serializable(with = UUIDSerializer::class)
    abstract override val key: UUID
    abstract override val version: Long
    abstract val username: String?

    // ------------------------------------------------------------ //
    //                          API Methods                         //
    // ------------------------------------------------------------ //
    private lateinit var docCache: PlayerDocCache<D>
    override fun getDocCache(): DocCache<UUID, D> {
        return docCache
    }

    // ------------------------------------------------------------ //
    //                       Data Class Helpers                     //
    // ------------------------------------------------------------ //
    /**
     * Produces a new instance of this document with its username updated.
     *
     * Top-level data classes should override this by invoking their generated
     * `copy(username = username)` method.
     *
     * @param username The new username for the copied player document.
     * @return A new instance of the document with the updated username.
     */
    abstract fun copyHelper(username: String?): D

    // ------------------------------------------------------------ //
    //                          PlayerDoc API                       //
    // ------------------------------------------------------------ //
    val uniqueId: UUID
        get() = this.key

    @kotlinx.serialization.Transient @Transient
    private var _player: Player? = null

    /**
     * Get the Bukkit [Player] who owns this document.
     */
    fun getPlayer(): Player? {
        if (PlayerUtil.isFullyValidPlayer(this._player)) return this._player

        val tempPlayer = Bukkit.getPlayer(this.uniqueId)
        this._player = if (!PlayerUtil.isFullyValidPlayer(tempPlayer)) {
            null
        } else {
            tempPlayer
        }
        return this._player
    }

    /**
     * Checks if the [Player] behind this [PlayerDoc] is online AND valid.
     *
     * See [isOnline] for a more lenient check.
     */
    val isTrulyOnline: Boolean
        get() = PlayerUtil.isFullyValidPlayer(Bukkit.getPlayer(this.uniqueId))

    /**
     * Checks if the [Player] behind this [PlayerDoc] is online. (not necessarily valid)
     *
     * See [isTrulyOnline] for a more strict check.
     */
    val isOnline: Boolean
        get() = Bukkit.getPlayer(this.uniqueId)?.isOnline ?: false

    // ------------------------------------------------------------ //
    //                      Internal API Methods                    //
    // ------------------------------------------------------------ //
    override fun initializeInternal(cache: DocCache<UUID, D>) {
        // Allow this function to be called many times
        if (!::docCache.isInitialized || docCache !== cache) {
            require(cache is PlayerDocCache<D>) {
                "Cache must be a PlayerDocCache"
            }
            docCache = cache
        }
    }

    override fun hashCode(): Int {
        return Objects.hashCode(this.key)
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is PlayerDoc<*>) return false
        return this.key == other.key
    }
}

package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.DataKache

class MongoConfig private constructor(val uri: String) {
    companion object {

        private var config: MongoConfig? = null

        fun get(): MongoConfig {
            config?.let { return it }
            return loadConfig().also {
                config = it
            }
        }

        private fun loadConfig(): MongoConfig {
            // pull settings from central DataKache config
            return MongoConfig(
                uri = DataKache.config.mongoURI,
            )
        }
    }
}

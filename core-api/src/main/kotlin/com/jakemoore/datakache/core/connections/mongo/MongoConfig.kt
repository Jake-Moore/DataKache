package com.jakemoore.datakache.core.connections.mongo

import com.jakemoore.datakache.DataKache

class MongoConfig private constructor(val uri: String) {
    companion object {

        fun get(): MongoConfig {
            // pull settings from central DataKache config
            return MongoConfig(
                uri = DataKache.config.mongoURI,
            )
        }
    }
}

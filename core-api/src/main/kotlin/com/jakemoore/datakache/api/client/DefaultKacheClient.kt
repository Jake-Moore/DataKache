@file:Suppress("unused")

package com.jakemoore.datakache.api.client

import com.jakemoore.datakache.api.DataKacheClient

class DefaultKacheClient(
    override val name: String
) : DataKacheClient

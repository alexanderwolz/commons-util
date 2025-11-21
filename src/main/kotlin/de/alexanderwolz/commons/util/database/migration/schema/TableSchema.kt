package de.alexanderwolz.commons.util.database.migration.schema

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

data class TableSchema(
    val columns: List<ColumnSchema>,
    val indexes: List<IndexSchema>,
    val foreignKeys: List<ForeignKeySchema>
) {
    fun toJson(): String {
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this)
    }

    companion object {
        private val mapper = jacksonObjectMapper().apply {
            setSerializationInclusion(JsonInclude.Include.NON_NULL)
        }

        fun fromJson(json: String): TableSchema {
            return mapper.readValue(json, TableSchema::class.java)
        }
    }
}
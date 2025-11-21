package de.alexanderwolz.commons.util.database.migration.schema

data class ColumnSchema(
    val name: String,
    val type: String,
    val nullable: Boolean,
    val unique: Boolean = false,
    val isPrimaryKey: Boolean = false,
    val defaultValue: String? = null
)
package de.alexanderwolz.commons.util.database

data class ForeignKeySchema(
    val columnName: String,
    val referencedTable: String,
    val referencedColumn: String,
    val onDelete: String
)
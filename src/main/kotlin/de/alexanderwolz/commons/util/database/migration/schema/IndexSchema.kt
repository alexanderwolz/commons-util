package de.alexanderwolz.commons.util.database.migration.schema

data class IndexSchema(
    val name: String,
    val columns: List<String>,
    val unique: Boolean = false
)
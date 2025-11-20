package de.alexanderwolz.commons.util.database

data class IndexSchema(
    val name: String,
    val columns: List<String>,
    val unique: Boolean = false
)
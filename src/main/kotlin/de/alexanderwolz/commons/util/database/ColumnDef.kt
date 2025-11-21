package de.alexanderwolz.commons.util.database

data class ColumnDef(
    val name: String,
    val type: String,
    val constraints: List<String>
) {
    fun toSql(maxName: Int, maxType: Int): String {
        return buildString {
            append("    ")
            append(name.padEnd(maxName))
            append("    ")
            append(type.padEnd(maxType))
            if (constraints.isNotEmpty()) {
                append(" ")
                append(constraints.joinToString(" "))
            }
        }
    }
}
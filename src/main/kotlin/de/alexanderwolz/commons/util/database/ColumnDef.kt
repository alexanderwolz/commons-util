package de.alexanderwolz.commons.util.database

data class ColumnDef(
    val name: String,
    val type: String,
    val constraints: List<String>
) {
    fun toSql(maxName: Int, maxType: Int): String {
        val n = name.padEnd(maxName)
        val t = type.padEnd(maxType)
        val c = if (constraints.isNotEmpty()) " " + constraints.joinToString(" ") else ""
        return "    $n $t$c".trimEnd()
    }
}
package de.alexanderwolz.commons.util.database.migration

import de.alexanderwolz.commons.util.database.provider.SchemaProvider
import jakarta.persistence.Column
import jakarta.persistence.Table
import jakarta.persistence.Transient
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.Modifier

object DatabaseMigrationUtils {

    fun getTableName(entity: Class<*>): String {
        val tableAnn = entity.getAnnotation(Table::class.java)
        return tableAnn?.name?.ifBlank { null } ?: toSnakeCase(entity.simpleName)
    }

    fun toSnakeCase(camelCase: String): String {
        return camelCase.replace(Regex("([a-z])([A-Z])"), "$1_$2").lowercase()
    }

    fun allPersistentFields(clazz: Class<*>): List<Field> {
        val fields = mutableListOf<Field>()
        var current: Class<*>? = clazz
        while (current != null && current != Any::class.java) {
            current.declaredFields.forEach { field ->
                if (!Modifier.isStatic(field.modifiers) && !Modifier.isTransient(field.modifiers) && field.getAnnotation(
                        Transient::class.java
                    ) == null
                ) {
                    fields.add(field)
                }
            }
            current = current.superclass
        }
        return fields
    }

    fun validateUniqueTableNames(entities: List<Class<*>>) {
        val tableNames = entities.map { DatabaseMigrationUtils.getTableName(it) }
        val duplicates = tableNames.groupingBy { it }.eachCount().filter { it.value > 1 }
        if (duplicates.isNotEmpty()) {
            throw IllegalStateException("Duplicate table names detected: ${duplicates.keys}")
        }
    }

    fun groupByPartition(
        entities: List<Class<*>>,
        provider: SchemaProvider,
        outDir: File
    ): Map<String, List<Class<*>>> {
        val result = mutableMapOf<String, MutableList<Class<*>>>()
        for (e in entities) {
            val folder = provider.getFolderFor(e, outDir)?.trim().orEmpty()
            val effective = folder.ifBlank { "default" }
            result.computeIfAbsent(effective) { mutableListOf() }.add(e)
        }
        return result
    }

    fun extractDefaultValue(colAnn: Column?): String? {
        if (colAnn == null) return null
        val def = colAnn.columnDefinition
        if (def.isBlank()) return null

        val defaultRegex = """DEFAULT\s+(.+)""".toRegex(RegexOption.IGNORE_CASE)
        return defaultRegex.find(def)?.groupValues?.get(1)?.trim()
    }

    fun formatCreateTableSql(sql: String): String {
        val lines = sql.trim().lines()

        val startIdx = lines.indexOfFirst { it.contains("(") }
        val endIdx = lines.indexOfLast { it.trim() == ");" }

        if (startIdx == -1 || endIdx == -1) return sql.trim() + "\n"

        val header = lines.subList(0, startIdx + 1)
        val body = lines.subList(startIdx + 1, endIdx)
        val footer = lines.subList(endIdx, lines.size)

        val cleanBody = body.map { it.trim().removeSuffix(",") }.filter { it.isNotBlank() }
        val aligned = alignSqlColumns(cleanBody).toMutableList()

        aligned.forEachIndexed { i, _ ->
            aligned[i] = if (i == aligned.lastIndex) aligned[i] else aligned[i] + ","
        }

        return (header + aligned + footer).joinToString("\n").trim() + "\n"
    }

    private fun alignSqlColumns(lines: List<String>): List<String> {
        data class Col(val name: String, val type: String, val rest: String?)

        val parsed = lines.map { line ->
            val parts = line.trim().split(Regex("\\s+"), limit = 3)
            Col(
                name = parts.getOrNull(0) ?: "", type = parts.getOrNull(1) ?: "", rest = parts.getOrNull(2)
            )
        }

        val maxName = parsed.maxOf { it.name.length }
        val maxType = parsed.maxOf { it.type.length }

        return parsed.map { col ->
            buildString {
                append("    ")
                append(col.name.padEnd(maxName))
                append("    ")
                append(col.type.padEnd(maxType))
                if (col.rest != null) append(" ${col.rest}")
            }
        }
    }

    inline fun <reified T : Annotation> findAnnotation(field: Field, entityClass: Class<*>): T? {
        field.getAnnotation(T::class.java)?.let { return it }
        val getterName = "get${field.name.replaceFirstChar { it.uppercase() }}"
        try {
            val getter = entityClass.getMethod(getterName)
            return getter.getAnnotation(T::class.java)
        } catch (_: NoSuchMethodException) {
        }
        return null
    }


}
package de.alexanderwolz.commons.util.database.migration

import de.alexanderwolz.commons.util.database.migration.schema.ColumnSchema
import de.alexanderwolz.commons.util.database.migration.schema.ForeignKeySchema
import de.alexanderwolz.commons.util.database.migration.schema.IndexSchema
import de.alexanderwolz.commons.util.database.migration.schema.TableSchema

class MigrationGenerator {

    fun compareSchemasAndGenerateMigration(
        tableName: String, entityClass: Class<*>, oldSchema: TableSchema, newSchema: TableSchema
    ): String {
        val normalizedOld = normalizeSchema(oldSchema)
        val normalizedNew = normalizeSchema(newSchema)

        val changes = mutableListOf<String>()

        changes.add("-- Migration for table: $tableName")
        changes.add("-- Entity: ${entityClass.simpleName}")
        changes.add("")

        val columnChanges = compareColumns(tableName, normalizedOld.columns, normalizedNew.columns)
        if (columnChanges.isNotEmpty()) {
            changes.add("-- Column changes")
            changes.addAll(columnChanges)
            changes.add("")
        }

        val indexChanges = compareIndexes(tableName, normalizedOld.indexes, normalizedNew.indexes)
        if (indexChanges.isNotEmpty()) {
            changes.add("-- Index changes")
            changes.addAll(indexChanges)
            changes.add("")
        }

        val fkChanges = compareForeignKeys(tableName, normalizedOld.foreignKeys, normalizedNew.foreignKeys)
        if (fkChanges.isNotEmpty()) {
            changes.add("-- Foreign key changes")
            changes.addAll(fkChanges)
            changes.add("")
        }

        return if (changes.size > 3) changes.joinToString("\n") else ""
    }

    private fun normalizeSchema(schema: TableSchema): TableSchema {
        fun normalizeType(type: String): String = type.trim().replace(Regex("\\s+"), " ").uppercase()

        fun normalizeDefault(default: String?): String? {
            val trimmed = default?.trim() ?: return null
            if (trimmed.equals("now()", ignoreCase = true)) return "NOW()"
            return trimmed
        }

        fun normalizeColumn(c: ColumnSchema): ColumnSchema = c.copy(
            name = c.name.trim(), type = normalizeType(c.type), defaultValue = normalizeDefault(c.defaultValue)
        )

        fun normalizeIndex(i: IndexSchema): IndexSchema = i.copy(
            name = i.name.trim(), columns = i.columns.map { it.trim() })

        fun normalizeFk(f: ForeignKeySchema): ForeignKeySchema = f.copy(
            columnName = f.columnName.trim(),
            referencedTable = f.referencedTable.trim(),
            referencedColumn = f.referencedColumn.trim(),
            onDelete = f.onDelete.trim().replace(Regex("\\s+"), " ").uppercase()
        )

        val cols = schema.columns.map(::normalizeColumn).sortedBy { it.name }
        val idxs = schema.indexes.map(::normalizeIndex)
            .sortedWith(compareBy<IndexSchema> { it.columns.size }.thenBy { it.columns.joinToString(",") }
                .thenBy { it.name })
        val fks = schema.foreignKeys.map(::normalizeFk).sortedBy { it.columnName }

        return TableSchema(
            columns = cols, indexes = idxs, foreignKeys = fks
        )
    }

    private fun compareColumns(
        tableName: String, oldCols: List<ColumnSchema>, newCols: List<ColumnSchema>
    ): List<String> {
        val changes = mutableListOf<String>()
        val oldMap = oldCols.associateBy { it.name }
        val newMap = newCols.associateBy { it.name }

        newCols.forEach { newCol ->
            if (newCol.name !in oldMap) {
                changes.add(buildString {
                    append("ALTER TABLE $tableName ADD COLUMN ${newCol.name} ${newCol.type}")
                    if (!newCol.nullable) append(" NOT NULL")
                    if (newCol.unique) append(" UNIQUE")
                    if (newCol.defaultValue != null) append(" DEFAULT ${newCol.defaultValue}")
                    append(";")
                })
            }
        }

        newCols.forEach { newCol ->
            val oldCol = oldMap[newCol.name]
            if (oldCol != null && oldCol != newCol) {
                if (oldCol.type != newCol.type) {
                    changes.add("ALTER TABLE $tableName ALTER COLUMN ${newCol.name} TYPE ${newCol.type};")
                }
                if (oldCol.nullable != newCol.nullable) {
                    val constraint = if (newCol.nullable) "DROP NOT NULL" else "SET NOT NULL"
                    changes.add("ALTER TABLE $tableName ALTER COLUMN ${newCol.name} $constraint;")
                }
                if (oldCol.unique != newCol.unique) {
                    if (newCol.unique) {
                        changes.add("ALTER TABLE $tableName ADD CONSTRAINT uq_${tableName}_${newCol.name} UNIQUE (${newCol.name});")
                    } else {
                        changes.add("ALTER TABLE $tableName DROP CONSTRAINT IF EXISTS uq_${tableName}_${newCol.name};")
                    }
                }
            }
        }

        oldCols.forEach { oldCol ->
            if (oldCol.name !in newMap && !oldCol.isPrimaryKey) {
                changes.add("-- WARNING: Column '${oldCol.name}' was removed from entity")
                changes.add("-- Consider: ALTER TABLE $tableName DROP COLUMN ${oldCol.name};")
            }
        }

        return changes
    }

    private fun compareIndexes(
        tableName: String, oldIndexes: List<IndexSchema>, newIndexes: List<IndexSchema>
    ): List<String> {
        val changes = mutableListOf<String>()
        val oldMap = oldIndexes.associateBy { it.columns.sorted() }
        val newMap = newIndexes.associateBy { it.columns.sorted() }

        newIndexes.forEach { idx ->
            if (idx.columns.sorted() !in oldMap) {
                val uniqueStr = if (idx.unique) "UNIQUE " else ""
                val name = idx.name.ifBlank { "idx_${tableName}_${idx.columns.joinToString("_")}" }
                changes.add("CREATE ${uniqueStr}INDEX $name ON $tableName (${idx.columns.joinToString(", ")});")
            }
        }

        oldIndexes.forEach { idx ->
            if (idx.columns.sorted() !in newMap) {
                val name = idx.name.ifBlank { "idx_${tableName}_${idx.columns.joinToString("_")}" }
                changes.add("DROP INDEX IF EXISTS $name;")
            }
        }

        return changes
    }

    private fun compareForeignKeys(
        tableName: String, oldFKs: List<ForeignKeySchema>, newFKs: List<ForeignKeySchema>
    ): List<String> {
        val changes = mutableListOf<String>()
        val oldMap = oldFKs.associateBy { it.columnName }
        val newMap = newFKs.associateBy { it.columnName }

        newFKs.forEach { fk ->
            if (fk.columnName !in oldMap) {
                val fkName = "fk_${tableName}_${fk.columnName}"
                changes.add(buildString {
                    appendLine("ALTER TABLE $tableName")
                    appendLine("    ADD CONSTRAINT $fkName")
                    appendLine("    FOREIGN KEY (${fk.columnName})")
                    appendLine("    REFERENCES ${fk.referencedTable}(${fk.referencedColumn})")
                    append("    ON DELETE ${fk.onDelete};")
                })
            }
        }

        newFKs.forEach { newFk ->
            val oldFk = oldMap[newFk.columnName]
            if (oldFk != null && oldFk != newFk) {
                val fkName = "fk_${tableName}_${newFk.columnName}"
                changes.add("ALTER TABLE $tableName DROP CONSTRAINT IF EXISTS $fkName;")
                changes.add(buildString {
                    appendLine("ALTER TABLE $tableName")
                    appendLine("    ADD CONSTRAINT $fkName")
                    appendLine("    FOREIGN KEY (${newFk.columnName})")
                    appendLine("    REFERENCES ${newFk.referencedTable}(${newFk.referencedColumn})")
                    append("    ON DELETE ${newFk.onDelete};")
                })
            }
        }

        oldFKs.forEach { fk ->
            if (fk.columnName !in newMap) {
                val fkName = "fk_${tableName}_${fk.columnName}"
                changes.add("ALTER TABLE $tableName DROP CONSTRAINT IF EXISTS $fkName;")
            }
        }

        return changes
    }
}

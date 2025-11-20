package de.alexanderwolz.commons.util.database

class MigrationGenerator {

    fun compareSchemasAndGenerateMigration(
        tableName: String,
        entityClass: Class<*>,
        oldSchema: TableSchema,
        newSchema: TableSchema
    ): String {
        val changes = mutableListOf<String>()

        changes.add("-- Migration for table: $tableName")
        changes.add("-- Entity: ${entityClass.simpleName}")
        changes.add("")

        val columnChanges = compareColumns(tableName, oldSchema.columns, newSchema.columns)
        if (columnChanges.isNotEmpty()) {
            changes.add("-- Column changes")
            changes.addAll(columnChanges)
            changes.add("")
        }

        val indexChanges = compareIndexes(tableName, oldSchema.indexes, newSchema.indexes)
        if (indexChanges.isNotEmpty()) {
            changes.add("-- Index changes")
            changes.addAll(indexChanges)
            changes.add("")
        }

        val fkChanges = compareForeignKeys(tableName, oldSchema.foreignKeys, newSchema.foreignKeys)
        if (fkChanges.isNotEmpty()) {
            changes.add("-- Foreign key changes")
            changes.addAll(fkChanges)
            changes.add("")
        }

        return if (changes.size > 3) changes.joinToString("\n") else ""
    }

    private fun compareColumns(
        tableName: String,
        oldCols: List<ColumnSchema>,
        newCols: List<ColumnSchema>
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
                    if (newCol.defaultValue != null) append(" ${newCol.defaultValue}")
                    append(";")
                })
            }
        }

        newCols.forEach { newCol ->
            val oldCol = oldMap[newCol.name]
            if (oldCol != null && oldCol != newCol) {
                // Typ geändert
                if (oldCol.type != newCol.type) {
                    changes.add("ALTER TABLE $tableName ALTER COLUMN ${newCol.name} TYPE ${newCol.type};")
                }
                // Nullable geändert
                if (oldCol.nullable != newCol.nullable) {
                    val constraint = if (newCol.nullable) "DROP NOT NULL" else "SET NOT NULL"
                    changes.add("ALTER TABLE $tableName ALTER COLUMN ${newCol.name} $constraint;")
                }
                // Unique constraint geändert
                if (oldCol.unique != newCol.unique) {
                    if (newCol.unique) {
                        changes.add("ALTER TABLE $tableName ADD CONSTRAINT uq_${tableName}_${newCol.name} UNIQUE (${newCol.name});")
                    } else {
                        changes.add("ALTER TABLE $tableName DROP CONSTRAINT IF EXISTS uq_${tableName}_${newCol.name};")
                    }
                }
            }
        }

        // Gelöschte Spalten (Optional: mit Warnung)
        oldCols.forEach { oldCol ->
            if (oldCol.name !in newMap && !oldCol.isPrimaryKey) {
                changes.add("-- WARNING: Column '${oldCol.name}' was removed from entity")
                changes.add("-- Consider: ALTER TABLE $tableName DROP COLUMN ${oldCol.name};")
            }
        }

        return changes
    }

    private fun compareIndexes(
        tableName: String,
        oldIndexes: List<IndexSchema>,
        newIndexes: List<IndexSchema>
    ): List<String> {
        val changes = mutableListOf<String>()
        val oldMap = oldIndexes.associateBy { it.columns.sorted() }
        val newMap = newIndexes.associateBy { it.columns.sorted() }

        // Neue Indexes
        newIndexes.forEach { idx ->
            if (idx.columns.sorted() !in oldMap) {
                val uniqueStr = if (idx.unique) "UNIQUE " else ""
                val name = idx.name.ifBlank { "idx_${tableName}_${idx.columns.joinToString("_")}" }
                changes.add("CREATE ${uniqueStr}INDEX $name ON $tableName (${idx.columns.joinToString(", ")});")
            }
        }

        // Gelöschte Indexes
        oldIndexes.forEach { idx ->
            if (idx.columns.sorted() !in newMap) {
                val name = idx.name.ifBlank { "idx_${tableName}_${idx.columns.joinToString("_")}" }
                changes.add("DROP INDEX IF EXISTS $name;")
            }
        }

        return changes
    }

    private fun compareForeignKeys(
        tableName: String,
        oldFKs: List<ForeignKeySchema>,
        newFKs: List<ForeignKeySchema>
    ): List<String> {
        val changes = mutableListOf<String>()
        val oldMap = oldFKs.associateBy { it.columnName }
        val newMap = newFKs.associateBy { it.columnName }

        // Neue Foreign Keys
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

        // Geänderte Foreign Keys
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

        // Gelöschte Foreign Keys
        oldFKs.forEach { fk ->
            if (fk.columnName !in newMap) {
                val fkName = "fk_${tableName}_${fk.columnName}"
                changes.add("ALTER TABLE $tableName DROP CONSTRAINT IF EXISTS $fkName;")
            }
        }

        return changes
    }
}
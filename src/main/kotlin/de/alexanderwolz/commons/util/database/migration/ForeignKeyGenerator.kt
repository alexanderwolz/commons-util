package de.alexanderwolz.commons.util.database.migration

import jakarta.persistence.JoinColumn
import jakarta.persistence.ManyToOne
import jakarta.persistence.OneToOne

class ForeignKeyGenerator {

    fun generateAllForeignKeys(entities: List<Class<*>>): List<String> {
        val out = mutableListOf<String>()

        entities.forEach { e ->
            val table = DatabaseMigrationUtils.getTableName(e)

            DatabaseMigrationUtils.allPersistentFields(e).forEach { f ->
                val rel = DatabaseMigrationUtils.findAnnotation<ManyToOne>(f, e)
                    ?: DatabaseMigrationUtils.findAnnotation<OneToOne>(f, e)
                if (rel == null) return@forEach

                val join = DatabaseMigrationUtils.findAnnotation<JoinColumn>(f, e)
                val colName = join?.name?.ifBlank { null } ?: "${DatabaseMigrationUtils.toSnakeCase(f.name)}_id"
                val refTable = DatabaseMigrationUtils.getTableName(f.type)
                val fkName = join?.foreignKey?.name?.ifBlank { null } ?: "fk_${table}_${colName}"
                val nullable = join?.nullable ?: true
                val deleteRule = if (nullable) "SET NULL" else "CASCADE"

                out.add(
                    buildString {
                        appendLine("-- Foreign key: $table.$colName -> $refTable.id")
                        appendLine("ALTER TABLE $table")
                        appendLine("    ADD CONSTRAINT $fkName")
                        appendLine("    FOREIGN KEY ($colName)")
                        appendLine("    REFERENCES $refTable(id)")
                        appendLine("    ON DELETE $deleteRule;")
                    })
            }
        }

        return out
    }
}
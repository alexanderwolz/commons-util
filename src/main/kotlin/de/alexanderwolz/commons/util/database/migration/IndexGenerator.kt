package de.alexanderwolz.commons.util.database.migration

import jakarta.persistence.*

class IndexGenerator {

    fun generateAllIndexes(entities: List<Class<*>>): List<String> {
        val out = mutableListOf<String>()

        entities.forEach { e ->
            val table = DatabaseMigrationUtils.getTableName(e)
            val tableAnn = e.getAnnotation(Table::class.java)

            // ------ custom indexes via @Table(indexes = …) ------
            tableAnn?.indexes?.forEach { idx ->
                val name = idx.name.ifBlank {
                    "idx_${table}_${idx.columnList.replace(",", "_").replace(" ", "")}"
                }
                val unique = if (idx.unique) "UNIQUE " else ""
                out.add(
                    buildString {
                        appendLine("-- Index on $table(${idx.columnList})")
                        append("CREATE ")
                        append(unique)
                        append("INDEX $name ON $table (${idx.columnList});")
                    })
            }

            // ------ Foreign-key indexes + heuristics ------
            DatabaseMigrationUtils.allPersistentFields(e).forEach { f ->

                // FK index
                if (DatabaseMigrationUtils.findAnnotation<ManyToOne>(
                        f,
                        e
                    ) != null || DatabaseMigrationUtils.findAnnotation<OneToOne>(f, e) != null
                ) {
                    val join = DatabaseMigrationUtils.findAnnotation<JoinColumn>(f, e)
                    val col = join?.name?.ifBlank { null } ?: "${DatabaseMigrationUtils.toSnakeCase(f.name)}_id"

                    // check if custom index already exists
                    val custom = tableAnn?.indexes?.any { idx ->
                        idx.columnList.split(",").any { it.trim() == col }
                    } ?: false

                    if (!custom) {
                        out.add(
                            buildString {
                                appendLine("-- Index on foreign key: $table.$col")
                                appendLine("CREATE INDEX idx_${table}_${col} ON $table ($col);")
                            })
                    }
                    return@forEach
                }

                // heuristic indexes for common lookup fields
                val colAnn = DatabaseMigrationUtils.findAnnotation<Column>(f, e)
                val name = colAnn?.name?.ifBlank { null } ?: DatabaseMigrationUtils.toSnakeCase(f.name)
                if (name in listOf("email", "username", "subject", "code")) {
                    out.add(
                        buildString {
                            appendLine("-- Lookup index on $table.$name")
                            appendLine("CREATE INDEX idx_${table}_${name} ON $table ($name);")
                        })
                }
            }
        }

        return out.distinct()
    }
}
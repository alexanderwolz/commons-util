package de.alexanderwolz.commons.util.database

import java.io.File

class SchemaStateTracker(baseDir: File) {

    private val stateDir = File(baseDir, ".schema-state")

    init {
        stateDir.mkdirs()
    }

    fun getExistingTables(): Set<String> {
        return stateDir.walkTopDown()
            .filter { it.isFile && it.extension == "json" }
            .map { it.nameWithoutExtension }
            .toSet()
    }

    fun saveTableSchema(schema: String, tableName: String, tableSchema: TableSchema) {
        val schemaDir = File(stateDir, schema.lowercase())
        schemaDir.mkdirs()
        val file = File(schemaDir, "$tableName.json")
        file.writeText(tableSchema.toJson())
    }

    fun loadTableSchema(schema: String, tableName: String): TableSchema? {
        val file = File(File(stateDir, schema.lowercase()), "$tableName.json")
        return if (file.exists()) {
            TableSchema.fromJson(file.readText())
        } else null
    }
}
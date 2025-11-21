package de.alexanderwolz.commons.util.database.migration

import de.alexanderwolz.commons.log.Logger
import de.alexanderwolz.commons.util.database.migration.schema.ColumnSchema
import de.alexanderwolz.commons.util.database.migration.schema.ForeignKeySchema
import de.alexanderwolz.commons.util.database.migration.schema.IndexSchema
import de.alexanderwolz.commons.util.database.migration.schema.TableSchema
import java.io.File

class SqlFileSchemaExtractor(private val baseDir: File) {

    private val logger = Logger(javaClass)

    fun getExistingTables(schemaFolder: String? = null): Set<String> {
        val dir = schemaFolder?.takeIf { it.isNotBlank() }
            ?.let { File(baseDir, it.lowercase()) }
            ?: baseDir

        if (!dir.exists()) return emptySet()

        return dir.walkTopDown()
            .filter { it.isFile && it.extension == "sql" }
            .flatMap { extractTablesFromFile(it).asSequence() }
            .toSet()
    }

    private fun extractTablesFromFile(file: File): List<String> {
        val content = file.readText()

        val results = mutableSetOf<String>()

        Regex("""CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)""", RegexOption.IGNORE_CASE)
            .findAll(content)
            .forEach { results.add(it.groupValues[1]) }

        Regex("""ALTER\s+TABLE\s+(\w+)""", RegexOption.IGNORE_CASE)
            .findAll(content)
            .forEach { results.add(it.groupValues[1]) }

        Regex("""CREATE\s+(?:UNIQUE\s+)?INDEX\s+\w+\s+ON\s+(\w+)""", RegexOption.IGNORE_CASE)
            .findAll(content)
            .forEach { results.add(it.groupValues[1]) }

        return results.toList()
    }

    fun loadTableSchema(schema: String, tableName: String): TableSchema? {
        val schemaDir = File(baseDir, schema.lowercase())
        if (!schemaDir.exists()) return null

        val createFile = findLatestCreateTableFile(schemaDir, tableName)
        if (createFile == null) {
            logger.debug { "No CREATE TABLE file found for $schema.$tableName" }
            return null
        }

        return try {
            parseTableSchema(createFile, tableName)
        } catch (e: Exception) {
            logger.warn { "Failed to parse schema from ${createFile.name}: ${e.message}" }
            null
        }
    }

    private fun findLatestCreateTableFile(schemaDir: File, tableName: String): File? {
        return schemaDir.listFiles()
            ?.filter { it.isFile && it.extension == "sql" }
            ?.filter { file ->
                val content = file.readText()
                content.contains("CREATE TABLE", ignoreCase = true) &&
                        content.contains(tableName, ignoreCase = true)
            }
            ?.maxByOrNull { it.name }
    }

    private fun extractTableNameFromFile(file: File): String? {
        val content = file.readText()
        val regex = """CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)""".toRegex(RegexOption.IGNORE_CASE)
        return regex.find(content)?.groupValues?.get(1)
    }

    private fun parseTableSchema(file: File, tableName: String): TableSchema {
        val content = file.readText()

        val columns = parseColumns(content, tableName)
        val indexes = parseIndexes(file.parentFile, tableName)
        val foreignKeys = parseForeignKeys(file.parentFile, tableName)

        return TableSchema(
            columns = columns,
            indexes = indexes,
            foreignKeys = foreignKeys
        )
    }

    private fun parseColumns(createTableSql: String, tableName: String): List<ColumnSchema> {
        val columns = mutableListOf<ColumnSchema>()

        val tableContentRegex = """CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?$tableName\s*\((.*?)\);""".toRegex(
            setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)
        )
        val tableContent = tableContentRegex.find(createTableSql)?.groupValues?.get(1) ?: return columns

        val lines = tableContent.lines()
            .map { it.trim() }
            .filter { it.isNotEmpty() && !it.startsWith("--") }

        for (line in lines) {
            if (line.startsWith("PRIMARY KEY", ignoreCase = true)) continue
            if (line.startsWith("FOREIGN KEY", ignoreCase = true)) continue
            if (line.startsWith("CONSTRAINT", ignoreCase = true)) continue

            parseColumnLine(line)?.let { columns.add(it) }
        }

        val pkRegex = """PRIMARY\s+KEY\s*\(([^)]+)\)""".toRegex(RegexOption.IGNORE_CASE)
        val pkCols = pkRegex.find(createTableSql)?.groupValues?.get(1)
            ?.split(",")?.map { it.trim() }?.toSet() ?: emptySet()

        val updatedColumns = columns.map { col ->
            if (col.name in pkCols) {
                col.copy(
                    isPrimaryKey = true,
                    nullable = false
                )
            } else col
        }

        return updatedColumns
    }

    private fun parseColumnLine(line: String): ColumnSchema? {
        val clean = line.trimEnd(',').trim()
        if (clean.isEmpty()) return null

        val parts = clean.split(Regex("\\s+"), limit = 3)
        if (parts.size < 2) return null

        val columnName = parts[0]
        val columnType = parts[1]
        val rest = parts.getOrNull(2) ?: ""

        val isPrimaryKey = rest.contains("PRIMARY KEY", ignoreCase = true)
        val nullable = if (isPrimaryKey) false else !rest.contains("NOT NULL", ignoreCase = true)
        val unique = rest.contains("UNIQUE", ignoreCase = true)
        val default = extractDefaultValue(rest)

        return ColumnSchema(
            name = columnName,
            type = columnType,
            nullable = nullable,
            unique = unique,
            isPrimaryKey = isPrimaryKey,
            defaultValue = default
        )
    }

    private fun extractDefaultValue(text: String): String? {
        val regex = """DEFAULT\s+(.+?)(?:\s*,|\s*$)""".toRegex(RegexOption.IGNORE_CASE)
        return regex.find(text)?.groupValues?.get(1)?.trim()
    }

    private fun parseIndexes(schemaDir: File, tableName: String): List<IndexSchema> {
        val indexes = mutableListOf<IndexSchema>()

        schemaDir.listFiles()
            ?.filter { it.isFile && it.extension == "sql" }
            ?.forEach { file ->
                val content = file.readText()
                indexes.addAll(parseIndexStatementsFromContent(content, tableName))
            }

        return indexes
    }

    private fun parseIndexStatementsFromContent(content: String, tableName: String): List<IndexSchema> {
        val indexes = mutableListOf<IndexSchema>()

        val indexRegex = """CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+$tableName\s*\(([^)]+)\)""".toRegex(
            RegexOption.IGNORE_CASE
        )

        indexRegex.findAll(content).forEach { match ->
            val unique = match.groupValues[1].isNotBlank()
            val indexName = match.groupValues[2]
            val columnList = match.groupValues[3]
                .split(",")
                .map { it.trim() }

            indexes.add(
                IndexSchema(
                    name = indexName,
                    columns = columnList,
                    unique = unique
                )
            )
        }

        return indexes
    }

    private fun parseForeignKeys(schemaDir: File, tableName: String): List<ForeignKeySchema> {
        val foreignKeys = mutableListOf<ForeignKeySchema>()

        schemaDir.listFiles()
            ?.filter { it.isFile && it.extension == "sql" }
            ?.forEach { file ->
                val content = file.readText()
                foreignKeys.addAll(parseForeignKeyStatementsFromContent(content, tableName))
            }

        return foreignKeys
    }

    private fun parseForeignKeyStatementsFromContent(content: String, tableName: String): List<ForeignKeySchema> {
        val foreignKeys = mutableListOf<ForeignKeySchema>()

        val fkRegex =
            """ALTER\s+TABLE\s+$tableName\s+ADD\s+CONSTRAINT\s+\w+\s+FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)""".toRegex(
                setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)
            )

        fkRegex.findAll(content).forEach { match ->
            val columnName = match.groupValues[1]
            val referencedTable = match.groupValues[2]
            val referencedColumn = match.groupValues[3]
            val onDelete = match.groupValues[4]

            foreignKeys.add(
                ForeignKeySchema(
                    columnName = columnName,
                    referencedTable = referencedTable,
                    referencedColumn = referencedColumn,
                    onDelete = onDelete
                )
            )
        }

        return foreignKeys
    }
}
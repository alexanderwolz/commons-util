package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.log.Logger
import jakarta.persistence.*
import java.io.File
import java.lang.Boolean
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.time.LocalDateTime
import kotlin.Annotation
import kotlin.Any
import kotlin.Exception
import kotlin.Int
import kotlin.String
import kotlin.let
import kotlin.takeIf

class SchemaGenerator(
    private val basePackage: String,
    private val outDir: File,
    private val databaseType: DatabaseType = DatabaseType.POSTGRES,
    private val uuidType: UUIDType = UUIDType.UUID_V7
) {

    enum class DatabaseType { POSTGRES, MARIADB }
    enum class UUIDType { UUID_V4, UUID_V7 }

    private val logger = Logger(javaClass)

    fun generate() {
        logger.info { "Generating SQL migrations from classes within '$basePackage'" }
        logger.info { "Database Type: $databaseType, UUID Type: $uuidType" }
        val entities = findEntities()
        separateBySchema(entities).forEach { (schema, list) ->
            generateFiles(list, schema)
        }
        logger.info { "done" }
    }

    private fun findEntities(): List<Class<*>> {
        val entities = mutableListOf<Class<*>>()
        val path = basePackage.replace('.', '/')
        val classLoader = Thread.currentThread().contextClassLoader
        try {
            val resources = classLoader.getResources(path).toList()
            resources.forEach { url ->
                val dir = File(url.file.replace("%20", " "))
                if (dir.exists() && dir.isDirectory) {
                    scanDirectory(dir, entities)
                }
            }
        } catch (e: Exception) {
            logger.error(e)
        }
        return entities
    }

    private fun scanDirectory(directory: File, entities: MutableList<Class<*>>) {
        directory.walkTopDown().forEach { file ->
            if (file.name.endsWith(".class") && !file.name.contains("$")) {
                try {
                    val relativePath = file.absolutePath
                        .substringAfter(basePackage.replace('.', File.separatorChar))
                        .removePrefix(File.separator)
                        .removeSuffix(".class")
                        .replace(File.separatorChar, '.')
                    val className = "$basePackage.$relativePath"
                    val clazz = Class.forName(className)
                    if (clazz.isAnnotationPresent(Entity::class.java)) {
                        entities.add(clazz)
                        logger.info { "Found entity: ${clazz.name}" }
                    }
                } catch (_: Exception) {
                    // ignore
                }
            }
        }
    }

    private fun separateBySchema(entities: List<Class<*>>): Map<String, List<Class<*>>> {
        val map = HashMap<String, MutableList<Class<*>>>()
        entities.forEach { entity ->
            val schema = entity.getAnnotation(Table::class.java)?.schema ?: ""
            val key = schema.takeIf { it.isNotBlank() }?.lowercase() ?: entity.packageName.split(".").last()
            map.computeIfAbsent(key) { mutableListOf() }.add(entity)
        }
        return map
    }

    private fun generateFiles(entities: List<Class<*>>, type: String) {
        if (entities.isEmpty()) return
        val targetDir = File(outDir, type.lowercase())
        targetDir.mkdirs()
        targetDir.listFiles { it.extension == "sql" }?.forEach { it.delete() }

        // Generate setup file for PostgreSQL with UUID extension
        if (databaseType == DatabaseType.POSTGRES && uuidType == UUIDType.UUID_V7) {
            val setupFile = File(targetDir, "V0__setup_uuid_extension.sql")
            setupFile.writeText(generateUuidExtensionSetup())
            logger.info { "Created: ${setupFile.parentFile.name}/${setupFile.name}" }
        }

        entities.forEachIndexed { i, entity ->
            val table = getTableName(entity)
            val file = File(targetDir, "V${i + 1}__create_${table}_table.sql")
            try {
                file.writeText(generateCreateTableSql(entity, table))
                logger.info { "Created: ${file.parentFile.name}/${file.name}" }
            } catch (e: Exception) {
                logger.error(e)
            }
        }

        val fks = generateAllForeignKeys(entities)
        if (fks.isNotEmpty()) {
            File(targetDir, "V${entities.size + 1}__add_foreign_keys.sql")
                .writeText(
                    "-- Foreign Keys generated ${LocalDateTime.now()}\n" +
                            "-- Database: $databaseType\n\n" +
                            fks.joinToString("\n\n")
                )
        }

        val indexes = generateAllIndexes(entities)
        if (indexes.isNotEmpty()) {
            File(targetDir, "V${entities.size + 2}__add_indexes.sql")
                .writeText(
                    "-- Indexes generated ${LocalDateTime.now()}\n" +
                            "-- Database: $databaseType\n\n" +
                            indexes.joinToString("\n\n")
                )
        }
    }

    private fun getTableName(clazz: Class<*>): String =
        clazz.getAnnotation(Table::class.java)?.name?.ifBlank { toSnakeCase(clazz.simpleName) }
            ?: toSnakeCase(clazz.simpleName)

    private fun toSnakeCase(s: String): String =
        s.replace(Regex("([a-z])([A-Z])"), "$1_$2").lowercase()

    private fun generateUuidExtensionSetup(): String {
        return buildString {
            appendLine("-- Setup UUID Extension for PostgreSQL")
            appendLine("-- Generated: ${LocalDateTime.now()}")
            appendLine()
            when (uuidType) {
                UUIDType.UUID_V7 -> {
                    appendLine("-- Enable uuid-ossp extension for UUID v7 support")
                    appendLine("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
                    appendLine()
                    appendLine("-- Create UUID v7 generation function")
                    appendLine("CREATE OR REPLACE FUNCTION uuid_generate_v7()")
                    appendLine("RETURNS UUID AS \$\$")
                    appendLine("DECLARE")
                    appendLine("    unix_ts_ms BIGINT;")
                    appendLine("    uuid_bytes BYTEA;")
                    appendLine("BEGIN")
                    appendLine("    unix_ts_ms = (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;")
                    appendLine("    uuid_bytes = OVERLAY(gen_random_bytes(16) PLACING")
                    appendLine("        substring(int8send(unix_ts_ms) FROM 3) FROM 1 FOR 6);")
                    appendLine("    uuid_bytes = SET_BYTE(uuid_bytes, 6, (GET_BYTE(uuid_bytes, 6) & 15) | 112);")
                    appendLine("    uuid_bytes = SET_BYTE(uuid_bytes, 8, (GET_BYTE(uuid_bytes, 8) & 63) | 128);")
                    appendLine("    RETURN ENCODE(uuid_bytes, 'hex')::UUID;")
                    appendLine("END;")
                    appendLine("\$\$ LANGUAGE plpgsql VOLATILE;")
                }

                UUIDType.UUID_V4 -> {
                    appendLine("-- Enable uuid-ossp extension for UUID v4 support")
                    appendLine("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
                }
            }
        }
    }

    private fun allPersistentFields(type: Class<*>): List<Field> {
        val ordered = LinkedHashMap<String, Field>()
        var c: Class<*>? = type
        while (c != null && c != Any::class.java) {
            c.declaredFields.filter {
                !it.isSynthetic && !Modifier.isStatic(it.modifiers) &&
                        it.getAnnotation(Transient::class.java) == null
            }.forEach { f -> ordered.putIfAbsent(f.name, f) }
            c = c.superclass
        }
        return ordered.values.toList()
    }

    private inline fun <reified A : Annotation> Field.findAnnotationOnFieldOrGetter(owner: Class<*>): A? {
        this.getAnnotation(A::class.java)?.let { return it }
        val prefix =
            if (this.type == Boolean.TYPE || this.type == Boolean::class.java) "is" else "get"
        val getterName = prefix + this.name.replaceFirstChar { it.uppercaseChar() }
        return try {
            val m = owner.getMethod(getterName)
            m.getAnnotation(A::class.java)
        } catch (_: NoSuchMethodException) {
            null
        }
    }

    private fun getIdType(entity: Class<*>): String {
        val idField = allPersistentFields(entity).firstOrNull { field ->
            field.findAnnotationOnFieldOrGetter<Id>(entity) != null
        } ?: return when (databaseType) {
            DatabaseType.POSTGRES -> "BIGINT"
            DatabaseType.MARIADB -> "BIGINT"
        }

        val column = idField.findAnnotationOnFieldOrGetter<Column>(entity)
        val gen = idField.findAnnotationOnFieldOrGetter<GeneratedValue>(entity)

        if (gen != null) {
            return when (gen.strategy) {
                GenerationType.UUID -> when (databaseType) {
                    DatabaseType.POSTGRES -> "UUID"
                    DatabaseType.MARIADB -> "CHAR(36)"
                }

                GenerationType.IDENTITY -> when (databaseType) {
                    DatabaseType.POSTGRES -> "BIGINT"
                    DatabaseType.MARIADB -> "BIGINT"
                }

                else -> sqlType(idField, column)
            }
        }

        // Fallback based on field type
        if (idField.type.simpleName == "UUID") {
            return when (databaseType) {
                DatabaseType.POSTGRES -> "UUID"
                DatabaseType.MARIADB -> "CHAR(36)"
            }
        }

        return sqlType(idField, column)
    }

    private fun generateCreateTableSql(entity: Class<*>, tableName: String): String {
        val cols = mutableListOf<String>()
        val maxColNameLength = 40

        allPersistentFields(entity).forEach { field ->
            val column = field.findAnnotationOnFieldOrGetter<Column>(entity)
            when {
                field.findAnnotationOnFieldOrGetter<Id>(entity) != null -> {
                    val name = column?.name?.takeIf { it.isNotBlank() } ?: toSnakeCase(field.name)
                    val gen = field.findAnnotationOnFieldOrGetter<GeneratedValue>(entity)
                    val def = if (gen != null) {
                        when (gen.strategy) {
                            GenerationType.UUID -> {
                                val type = when (databaseType) {
                                    DatabaseType.POSTGRES -> "UUID"
                                    DatabaseType.MARIADB -> "CHAR(36)"
                                }
                                val default = getUuidDefault()
                                formatColumn(name, type, "PRIMARY KEY $default", maxColNameLength)
                            }

                            GenerationType.IDENTITY -> {
                                when (databaseType) {
                                    DatabaseType.POSTGRES -> formatColumn(
                                        name,
                                        "BIGSERIAL",
                                        "PRIMARY KEY",
                                        maxColNameLength
                                    )

                                    DatabaseType.MARIADB -> formatColumn(
                                        name,
                                        "BIGINT",
                                        "AUTO_INCREMENT PRIMARY KEY",
                                        maxColNameLength
                                    )
                                }
                            }

                            else -> formatColumn(name, sqlType(field, column), "PRIMARY KEY", maxColNameLength)
                        }
                    } else {
                        // Fallback by type
                        if (field.type.simpleName == "UUID") {
                            val type = when (databaseType) {
                                DatabaseType.POSTGRES -> "UUID"
                                DatabaseType.MARIADB -> "CHAR(36)"
                            }
                            val default = getUuidDefault()
                            formatColumn(name, type, "PRIMARY KEY $default", maxColNameLength)
                        } else {
                            formatColumn(name, sqlType(field, column), "PRIMARY KEY", maxColNameLength)
                        }
                    }
                    cols.add(def)
                }

                field.findAnnotationOnFieldOrGetter<ManyToOne>(entity) != null ||
                        field.findAnnotationOnFieldOrGetter<OneToOne>(entity) != null -> {
                    val joinCol = field.findAnnotationOnFieldOrGetter<JoinColumn>(entity)
                    val colName = joinCol?.name?.takeIf { it.isNotBlank() } ?: "${toSnakeCase(field.name)}_id"
                    val nullable = joinCol?.nullable ?: true
                    val fkType = getIdType(field.type)
                    val constraints = if (!nullable) "NOT NULL" else ""
                    cols.add(formatColumn(colName, fkType, constraints, maxColNameLength))
                }

                field.findAnnotationOnFieldOrGetter<OneToMany>(entity) != null ||
                        field.findAnnotationOnFieldOrGetter<ManyToMany>(entity) != null -> {
                    // skip
                }

                field.findAnnotationOnFieldOrGetter<Embedded>(entity) != null -> {
                    cols.addAll(resolveEmbeddedColumns(field, maxColNameLength))
                }

                else -> {
                    val column = field.findAnnotationOnFieldOrGetter<Column>(entity)
                    val name = column?.name?.takeIf { it.isNotBlank() } ?: toSnakeCase(field.name)
                    val nullable = column?.nullable ?: true
                    val unique = column?.unique ?: false
                    val type = sqlType(field, column)

                    val constraints = buildList {
                        if (!nullable) add("NOT NULL")
                        if (unique) add("UNIQUE")
                        if (name == "created_at" || name == "updated_at") {
                            add(
                                when (databaseType) {
                                    DatabaseType.POSTGRES -> "DEFAULT CURRENT_TIMESTAMP"
                                    DatabaseType.MARIADB -> "DEFAULT CURRENT_TIMESTAMP"
                                }
                            )
                        }
                    }.joinToString(" ")

                    cols.add(formatColumn(name, type, constraints, maxColNameLength))
                }
            }
        }

        return buildString {
            appendLine("-- create_${tableName}_table")
            appendLine("-- Entity: ${entity.simpleName}")
            appendLine("-- Database: $databaseType")
            appendLine("-- Generated: ${LocalDateTime.now()}")
            appendLine()
            appendLine("CREATE TABLE $tableName (")
            cols.forEachIndexed { index, col ->
                append(col)
                if (index < cols.size - 1) appendLine(",")
                else appendLine()
            }
            append(");")
        }
    }

    private fun getUuidDefault(): String {
        return when (databaseType) {
            DatabaseType.POSTGRES -> when (uuidType) {
                UUIDType.UUID_V7 -> "DEFAULT uuid_generate_v7()"
                UUIDType.UUID_V4 -> "DEFAULT uuid_generate_v4()"
            }

            DatabaseType.MARIADB -> when (uuidType) {
                UUIDType.UUID_V7 -> "DEFAULT (UUID())"  // MariaDB hat kein natives UUID v7
                UUIDType.UUID_V4 -> "DEFAULT (UUID())"
            }
        }
    }

    private fun formatColumn(name: String, type: String, constraints: String, maxLength: Int): String {
        val paddedName = name.padEnd(maxLength)
        val paddedType = type.padEnd(20)
        return if (constraints.isNotBlank()) {
            "    $paddedName $paddedType $constraints"
        } else {
            "    $paddedName $paddedType"
        }.trimEnd()
    }

    private fun resolveEmbeddedColumns(field: Field, maxLength: Int, prefix: String = ""): List<String> {
        val cols = mutableListOf<String>()
        val type = field.type
        if (!type.isAnnotationPresent(Embeddable::class.java)) return emptyList()

        val overrides = mutableMapOf<String, Column>()
        field.getAnnotationsByType(AttributeOverride::class.java)
            .forEach { overrides[it.name] = it.column }
        field.getAnnotation(AttributeOverrides::class.java)?.value?.forEach {
            overrides[it.name] = it.column
        }

        type.declaredFields.filter {
            !it.isSynthetic && !Modifier.isStatic(it.modifiers)
        }.forEach { field ->
            val overrideCol = overrides[field.name]
            val columnAnn = overrideCol ?: field.getAnnotation(Column::class.java)
            val name = when {
                overrideCol != null && overrideCol.name.isNotBlank() -> overrideCol.name
                columnAnn != null && columnAnn.name.isNotBlank() -> columnAnn.name
                else -> prefix + toSnakeCase(field.name)
            }
            val nullable = columnAnn?.nullable ?: true
            val sqlType = sqlType(field, columnAnn)
            val constraints = if (!nullable) "NOT NULL" else ""
            cols.add(formatColumn(name, sqlType, constraints, maxLength))
        }

        return cols
    }

    private fun generateAllForeignKeys(entities: List<Class<*>>): List<String> {
        val out = mutableListOf<String>()
        entities.forEach { e ->
            val table = getTableName(e)
            allPersistentFields(e).forEach { f ->
                val rel = f.findAnnotationOnFieldOrGetter<ManyToOne>(e)
                    ?: f.findAnnotationOnFieldOrGetter<OneToOne>(e)
                if (rel != null) {
                    val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(e)
                    val col = join?.name?.takeIf { it.isNotBlank() } ?: "${toSnakeCase(f.name)}_id"
                    val ref = f.type
                    val refTable = getTableName(ref)
                    val fkName = join?.foreignKey?.name?.takeIf { it.isNotBlank() }
                        ?: "fk_${table}_${col}"
                    val onDelete = if (rel is OneToOne) "CASCADE" else "SET NULL"

                    out.add(buildString {
                        appendLine("-- Foreign key: $table.$col -> $refTable.id")
                        appendLine("ALTER TABLE $table")
                        appendLine("    ADD CONSTRAINT $fkName")
                        appendLine("        FOREIGN KEY ($col)")
                        appendLine("        REFERENCES $refTable(id)")
                        append("        ON DELETE $onDelete;")
                    })
                }
            }
        }
        return out
    }

    private fun generateAllIndexes(entities: List<Class<*>>): List<String> {
        val out = mutableListOf<String>()
        entities.forEach { e ->
            val table = getTableName(e)

            val tableAnn = e.getAnnotation(Table::class.java)
            tableAnn?.indexes?.forEach { idx ->
                val unique = if (idx.unique) "UNIQUE " else ""
                val name = idx.name.ifBlank { "idx_${table}_${idx.columnList.replace(",", "_").replace(" ", "")}" }
                out.add("-- Index on $table(${idx.columnList})\nCREATE ${unique}INDEX $name ON $table (${idx.columnList});")
            }

            allPersistentFields(e).forEach { f ->
                if (f.findAnnotationOnFieldOrGetter<ManyToOne>(e) != null ||
                    f.findAnnotationOnFieldOrGetter<OneToOne>(e) != null
                ) {
                    val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(e)
                    val col = join?.name?.takeIf { it.isNotBlank() } ?: "${toSnakeCase(f.name)}_id"
                    // Skip if already covered by @Table indexes
                    val alreadyIndexed = tableAnn?.indexes?.any {
                        it.columnList.split(",").any { c -> c.trim() == col }
                    } ?: false
                    if (!alreadyIndexed) {
                        out.add("-- Index on foreign key $table.$col\nCREATE INDEX idx_${table}_${col} ON $table ($col);")
                    }
                } else {
                    val col = f.findAnnotationOnFieldOrGetter<Column>(e)
                    val colName = col?.name?.takeIf { it.isNotBlank() } ?: toSnakeCase(f.name)
                    if (colName in listOf("email", "username", "subject", "code")) {
                        out.add("-- Index on common lookup field\nCREATE INDEX idx_${table}_${colName} ON $table ($colName);")
                    }
                }
            }
        }
        return out.distinct()
    }

    private fun sqlType(field: Field, column: Column?): String {
        return when (databaseType) {
            DatabaseType.POSTGRES -> sqlTypePostgres(field, column)
            DatabaseType.MARIADB -> sqlTypeMariaDb(field, column)
        }
    }

    private fun sqlTypePostgres(field: Field, column: Column?): String {
        column?.columnDefinition?.takeIf { it.isNotBlank() }?.let {
            return it.uppercase()
        }

        val length = column?.length ?: 255

        return when (field.type.simpleName) {
            "String" -> "VARCHAR($length)"

            "LocalDateTime", "Instant" -> "TIMESTAMP"
            "LocalDate" -> "DATE"
            "LocalTime" -> "TIME"
            "ZonedDateTime", "OffsetDateTime" -> "TIMESTAMP WITH TIME ZONE"
            "Duration" -> "BIGINT"
            "Period" -> "VARCHAR(50)"

            "Boolean", "boolean" -> "BOOLEAN"

            "Byte", "byte" -> "SMALLINT"
            "Short", "short" -> "SMALLINT"
            "Integer", "int" -> "INTEGER"
            "Long", "long" -> "BIGINT"

            "Float", "float" -> "REAL"
            "Double", "double" -> "DOUBLE PRECISION"
            "BigDecimal" -> {
                val precision = column?.precision ?: 19
                val scale = column?.scale ?: 2
                "DECIMAL($precision,$scale)"
            }

            "UUID" -> "UUID"
            "byte[]", "Byte[]" -> "BYTEA"
            "URL" -> "VARCHAR(2048)"
            "URI" -> "VARCHAR(2048)"
            "JsonNode" -> "JSONB"

            else -> if (field.type.isEnum) "VARCHAR(50)" else "VARCHAR($length)"
        }
    }

    private fun sqlTypeMariaDb(field: Field, column: Column?): String {
        column?.columnDefinition?.takeIf { it.isNotBlank() }?.let {
            return it.uppercase()
        }

        val length = column?.length ?: 255

        return when (field.type.simpleName) {
            "String" -> "VARCHAR($length)"

            "LocalDateTime", "Instant" -> "DATETIME"
            "LocalDate" -> "DATE"
            "LocalTime" -> "TIME"
            "ZonedDateTime", "OffsetDateTime" -> "DATETIME"
            "Duration" -> "BIGINT"
            "Period" -> "VARCHAR(50)"

            "Boolean", "boolean" -> "BOOLEAN"

            "Byte", "byte" -> "TINYINT"
            "Short", "short" -> "SMALLINT"
            "Integer", "int" -> "INT"
            "Long", "long" -> "BIGINT"

            "Float", "float" -> "FLOAT"
            "Double", "double" -> "DOUBLE"
            "BigDecimal" -> {
                val precision = column?.precision ?: 19
                val scale = column?.scale ?: 2
                "DECIMAL($precision,$scale)"
            }

            "UUID" -> "CHAR(36)"
            "byte[]", "Byte[]" -> "BLOB"
            "URL" -> "VARCHAR(2048)"
            "URI" -> "VARCHAR(2048)"
            "JsonNode" -> "JSON"

            else -> if (field.type.isEnum) "VARCHAR(50)" else "VARCHAR($length)"
        }
    }

}
package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.log.Logger
import de.alexanderwolz.commons.util.database.provider.DefaultPackageProvider
import de.alexanderwolz.commons.util.database.provider.PackageProvider
import jakarta.persistence.*
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SchemaGenerator(
    private val basePackage: String,
    private val outDir: File,
    private val databaseType: DatabaseType = DatabaseType.POSTGRES,
    private val uuidType: UUIDType = UUIDType.UUID_V7,
    private val provider: PackageProvider = DefaultPackageProvider(),
    private val clearTargetFolders: Boolean = false
) {

    private val logger = Logger(javaClass)
    private val entityScanner = EntityScanner(basePackage)

    enum class DatabaseType { POSTGRES, MARIADB }
    enum class UUIDType { UUID_V4, UUID_V7 }

    private var executionTimestamp = ""


    fun generate() {
        try {
            executionTimestamp = buildTimestamp() //ensure the same timestamp for all files

            logger.info { "Generating SQL migrations from classes within '$basePackage'" }
            logger.info { "Database Type: $databaseType, UUID Type: $uuidType" }

            val rawEntities = entityScanner.findEntities()

            validateUniqueTableNames(rawEntities)
            val entities = rawEntities.sortedBy { stableSortKey(getTableName(it)) }

            generateSetupFiles(entities)

            groupByPartition(entities).forEach { (schema, entitiesInPartition) ->
                val sorted = entitiesInPartition.sortedBy { stableSortKey(getTableName(it)) }
                generateFiles(sorted, schema)
            }

            logger.info { "done" }
        } finally {
            executionTimestamp = ""
        }
    }

    private fun buildTimestamp(): String {
        return DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
    }

    private fun validateUniqueTableNames(entities: List<Class<*>>) {
        val map = mutableMapOf<String, MutableList<Class<*>>>()

        for (e in entities) {
            val table = getTableName(e)
            map.computeIfAbsent(table) { mutableListOf() }.add(e)
        }

        val duplicates = map.filter { it.value.size > 1 }
        if (duplicates.isNotEmpty()) {
            val msg = duplicates.entries.joinToString("\n") { (table, classes) ->
                "Table '$table' is mapped by: ${classes.joinToString(", ") { it.name }}"
            }
            throw IllegalStateException("Duplicate table names detected:\n$msg")
        }
    }

    private fun stableSortKey(name: String): String {
        val md = MessageDigest.getInstance("SHA-1")
        val hash = md.digest(name.toByteArray())
        return hash.joinToString("") { "%02x".format(it) }.take(8)
    }

    internal fun groupByPartition(entities: List<Class<*>>): Map<String, List<Class<*>>> {
        val result = mutableMapOf<String, MutableList<Class<*>>>()
        for (e in entities) {
            val folder = provider.getFolderFor(e, outDir) ?: ""
            result.computeIfAbsent(folder) { mutableListOf() }.add(e)
        }
        return result
    }

    private fun generateSetupFiles(entities: List<Class<*>>) {
        val setupFolder = provider.getSetupFolder(outDir) ?: ""

        if (databaseType == DatabaseType.POSTGRES && uuidType == UUIDType.UUID_V7) {
            val usesUuid = entities.any { it.idField() != null }
            if (usesUuid) {
                val target = prepareTargetDirectory(setupFolder)
                val sql = formatPlainSql(TableSqlGenerator().generateUuidExtensionSetup())
                writeMigrationFile(
                    targetDir = target,
                    sortNumber = "0001",
                    baseName = "setup_uuid_extension",
                    content = sql
                )
            }
        }
    }

    private fun generateFiles(entities: List<Class<*>>, schema: String) {
        if (entities.isEmpty()) return

        val target = prepareTargetDirectory(schema)
        val tableGen = TableSqlGenerator()
        val fkGen = ForeignKeyGenerator()
        val idxGen = IndexGenerator()

        entities.forEachIndexed { index, entity ->
            val table = getTableName(entity)
            val sql = formatCreateTableSql(tableGen.generateCreateTableSql(entity, table))
            val sortNumber = (1000 + index).toString().padStart(4, '0')
            writeMigrationFile(
                targetDir = target,
                sortNumber = sortNumber,
                baseName = "create_${table}_table",
                content = sql
            )
        }

        val fkList = fkGen.generateAllForeignKeys(entities)
        if (fkList.isNotEmpty()) {
            val sql = "-- Foreign Keys\n" + fkList.joinToString("\n\n")
            writeMigrationFile(
                targetDir = target,
                sortNumber = "5000",
                baseName = "add_foreign_keys",
                content = formatPlainSql(sql)
            )
        }

        val idxList = idxGen.generateAllIndexes(entities)
        if (idxList.isNotEmpty()) {
            val sql = "-- Indexes\n" + idxList.joinToString("\n\n")
            writeMigrationFile(
                targetDir = target,
                sortNumber = "9000",
                baseName = "add_indexes",
                content = formatPlainSql(sql)
            )
        }
    }

    private fun formatCreateTableSql(sql: String): String {
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
                name = parts.getOrNull(0) ?: "",
                type = parts.getOrNull(1) ?: "",
                rest = parts.getOrNull(2)
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

    private fun formatPlainSql(sql: String): String =
        sql.trim().replace(Regex("\\n{3,}"), "\n\n").trim() + "\n"

    private fun prepareTargetDirectory(schemaKey: String): File {
        return File(outDir, schemaKey.lowercase()).apply {
            mkdirs()
            if (clearTargetFolders) {
                logger.info { "Clearing target folder ${this.absolutePath}" }
                listFiles()?.forEach { it.deleteRecursively() }
            }
        }
    }

    private fun getTableName(clazz: Class<*>): String =
        clazz.getAnnotation(Table::class.java)?.name?.ifBlank { toSnakeCase(clazz.simpleName) }
            ?: toSnakeCase(clazz.simpleName)

    private fun toSnakeCase(s: String): String =
        s.replace(Regex("([a-z0-9])([A-Z])"), "$1_$2")
            .replace(Regex("([A-Z]+)([A-Z][a-z])"), "$1_$2")
            .lowercase()

    private fun Class<*>.idField(): Field? = allPersistentFields(this).firstOrNull {
        it.findAnnotationOnFieldOrGetter<Id>(this) != null
    }

    private fun allPersistentFields(type: Class<*>): List<Field> {
        val map = LinkedHashMap<String, Field>()
        var c: Class<*>? = type
        while (c != null && c != Any::class.java) {
            c.declaredFields
                .filter {
                    !Modifier.isStatic(it.modifiers)
                            && !it.isSynthetic
                            && it.getAnnotation(Transient::class.java) == null
                }
                .forEach { f -> map.putIfAbsent(f.name, f) }
            c = c.superclass
        }
        return map.values.toList()
    }

    private inline fun <reified A : Annotation> Field.findAnnotationOnFieldOrGetter(owner: Class<*>): A? {
        getAnnotation(A::class.java)?.let { return it }

        val prefix =
            if (this.type == java.lang.Boolean.TYPE || this.type == java.lang.Boolean::class.java) "is" else "get"
        val getter = prefix + this.name.replaceFirstChar { it.uppercase() }

        return try {
            owner.getMethod(getter).getAnnotation(A::class.java)
        } catch (_: Exception) {
            null
        }
    }

    private fun sqlType(field: Field, col: Column?): String = when (databaseType) {
        DatabaseType.POSTGRES -> sqlTypePg(field, col)
        DatabaseType.MARIADB -> sqlTypeMaria(field, col)
    }

    private fun sqlTypePg(field: Field, col: Column?): String {
        col?.columnDefinition?.takeIf { it.isNotBlank() }?.let { return it }

        val length = col?.length ?: 255

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
                val p = col?.precision ?: 19
                val s = col?.scale ?: 2
                "DECIMAL($p,$s)"
            }

            "UUID" -> "UUID"
            "URL", "URI" -> "VARCHAR(2048)"
            "JsonNode" -> "JSONB"
            else -> if (field.type.isEnum) "VARCHAR(50)" else "VARCHAR($length)"
        }
    }

    private fun sqlTypeMaria(field: Field, col: Column?): String {
        col?.columnDefinition?.takeIf { it.isNotBlank() }?.let { return it }
        val length = col?.length ?: 255

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
                val p = col?.precision ?: 19
                val s = col?.scale ?: 2
                "DECIMAL($p,$s)"
            }

            "UUID" -> "CHAR(36)"
            "URL", "URI" -> "VARCHAR(2048)"
            "JsonNode" -> "JSON"
            else -> if (field.type.isEnum) "VARCHAR(50)" else "VARCHAR($length)"
        }
    }

    private fun uuidDefaultSql(): String = when (databaseType) {
        DatabaseType.POSTGRES -> when (uuidType) {
            UUIDType.UUID_V7 -> "DEFAULT public.uuid_generate_v7()"
            UUIDType.UUID_V4 -> "DEFAULT public.uuid_generate_v4()"
        }

        DatabaseType.MARIADB -> "DEFAULT (UUID())"
    }

    private fun timestampDefault(col: String): String? =
        if (col == "created_at" || col == "updated_at") "DEFAULT CURRENT_TIMESTAMP" else null

    private inner class TableSqlGenerator {

        fun generateCreateTableSql(entity: Class<*>, tableName: String): String {
            val cols = mutableListOf<ColumnDef>()

            allPersistentFields(entity).forEach { f ->
                val colAnn = f.findAnnotationOnFieldOrGetter<Column>(entity)

                // ----- ID FIELD -----
                if (f.findAnnotationOnFieldOrGetter<Id>(entity) != null) {
                    val colName = colAnn?.name?.ifBlank { null } ?: toSnakeCase(f.name)
                    val gen = f.findAnnotationOnFieldOrGetter<GeneratedValue>(entity)

                    val type = when (gen?.strategy) {
                        GenerationType.UUID ->
                            if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

                        GenerationType.IDENTITY ->
                            if (databaseType == DatabaseType.POSTGRES) "BIGSERIAL" else "BIGINT"

                        else -> sqlType(f, colAnn)
                    }

                    val constraints = mutableListOf("PRIMARY KEY")
                    if (gen?.strategy == GenerationType.UUID) constraints.add(uuidDefaultSql())
                    if (gen?.strategy == GenerationType.IDENTITY && databaseType == DatabaseType.MARIADB)
                        constraints.add("AUTO_INCREMENT")

                    cols.add(ColumnDef(colName, type, constraints))
                    return@forEach
                }

                // ----- RELATIONS -----
                val relMany = f.findAnnotationOnFieldOrGetter<ManyToOne>(entity)
                val relOne = f.findAnnotationOnFieldOrGetter<OneToOne>(entity)
                if (relMany != null || relOne != null) {
                    val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(entity)
                    val name = join?.name?.ifBlank { null } ?: "${toSnakeCase(f.name)}_id"
                    val nullable = join?.nullable ?: true
                    val refType = getIdType(f.type)

                    val cons = if (!nullable) listOf("NOT NULL") else emptyList()
                    cols.add(ColumnDef(name, refType, cons))
                    return@forEach
                }

                // Skip OneToMany or ManyToMany
                if (
                    f.findAnnotationOnFieldOrGetter<OneToMany>(entity) != null ||
                    f.findAnnotationOnFieldOrGetter<ManyToMany>(entity) != null
                ) return@forEach

                // ----- EMBEDDED -----
                if (f.findAnnotationOnFieldOrGetter<Embedded>(entity) != null) {
                    cols.addAll(resolveEmbeddedColumns(f))
                    return@forEach
                }

                // ----- NORMAL COLUMNS -----
                val name = colAnn?.name?.ifBlank { null } ?: toSnakeCase(f.name)
                val type = sqlType(f, colAnn)

                val cons = mutableListOf<String>()
                if (colAnn?.nullable == false) cons.add("NOT NULL")
                if (colAnn?.unique == true) cons.add("UNIQUE")
                timestampDefault(name)?.let { cons.add(it) }

                cols.add(ColumnDef(name, type, cons))
            }

            val maxName = cols.maxOfOrNull { it.name.length } ?: 1
            val maxType = cols.maxOfOrNull { it.type.length } ?: 1

            val body = cols.joinToString(",\n") { it.toSql(maxName, maxType) }

            return buildString {
                appendLine("-- create_${tableName}_table")
                appendLine("-- Entity: ${entity.simpleName} [${entity.name}]")
                appendLine("-- Database: $databaseType")
                appendLine()
                appendLine("CREATE TABLE $tableName (")
                append(body)
                appendLine()
                appendLine(");")
            }
        }

        private fun getIdType(entityClass: Class<*>): String {
            val idField = entityClass.idField()
                ?: return if (databaseType == DatabaseType.POSTGRES) "BIGINT" else "BIGINT"

            val col = idField.getAnnotation(Column::class.java)
            val gen = idField.getAnnotation(GeneratedValue::class.java)

            if (gen?.strategy == GenerationType.UUID)
                return if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

            if (gen?.strategy == GenerationType.IDENTITY)
                return if (databaseType == DatabaseType.POSTGRES) "BIGSERIAL" else "BIGINT"

            if (idField.type.simpleName == "UUID")
                return if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

            return sqlType(idField, col)
        }

        fun generateUuidExtensionSetup(): String {
            return when (uuidType) {

                UUIDType.UUID_V7 -> buildString {
                    appendLine("-- Setup UUID v7 (pgcrypto + idempotent function creation)")
                    appendLine("CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA public;")
                    appendLine()
                    appendLine("DO $$")
                    appendLine("BEGIN")
                    appendLine("    IF NOT EXISTS (")
                    appendLine("        SELECT 1 FROM pg_proc WHERE proname = 'uuid_generate_v7'")
                    appendLine("    ) THEN")
                    appendLine("        CREATE FUNCTION public.uuid_generate_v7()")
                    appendLine($$"        RETURNS UUID AS $fn$")
                    appendLine("        DECLARE")
                    appendLine("            unix_ts_ms BIGINT;")
                    appendLine("            uuid_bytes BYTEA;")
                    appendLine("        BEGIN")
                    appendLine("            unix_ts_ms := (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;")
                    appendLine("            uuid_bytes := public.gen_random_bytes(16);")
                    appendLine("            uuid_bytes := OVERLAY(uuid_bytes PLACING substring(int8send(unix_ts_ms) FROM 3) FROM 1 FOR 6);")
                    appendLine("            uuid_bytes := SET_BYTE(uuid_bytes, 6, (GET_BYTE(uuid_bytes, 6) & 15) | 112);")
                    appendLine("            uuid_bytes := SET_BYTE(uuid_bytes, 8, (GET_BYTE(uuid_bytes, 8) & 63) | 128);")
                    appendLine("            RETURN encode(uuid_bytes, 'hex')::UUID;")
                    appendLine("        END;")
                    appendLine($$"        $fn$ LANGUAGE plpgsql VOLATILE;")
                    appendLine("    END IF;")
                    appendLine("END")
                    appendLine("$$;")
                }

                UUIDType.UUID_V4 -> buildString {
                    appendLine("-- Setup UUID v4")
                    appendLine("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA public;")
                }
            }
        }

        private fun resolveEmbeddedColumns(field: Field): List<ColumnDef> {
            val prefix = toSnakeCase(field.name) + "_"
            val type = field.type

            if (!type.isAnnotationPresent(Embeddable::class.java)) return emptyList()

            val overrides = mutableMapOf<String, Column>()
            field.getAnnotationsByType(AttributeOverride::class.java).forEach {
                overrides[it.name] = it.column
            }
            field.getAnnotation(AttributeOverrides::class.java)?.value?.forEach {
                overrides[it.name] = it.column
            }

            val result = mutableListOf<ColumnDef>()

            type.declaredFields
                .filter { !it.isSynthetic && !Modifier.isStatic(it.modifiers) }
                .forEach { ef ->
                    val override = overrides[ef.name]
                    val colAnn = override ?: ef.getAnnotation(Column::class.java)

                    val colName = override?.name?.ifBlank { null }
                        ?: colAnn?.name?.ifBlank { null }
                        ?: (prefix + toSnakeCase(ef.name))

                    val nullable = colAnn?.nullable ?: true
                    val typeStr = sqlType(ef, colAnn)
                    val cons = if (!nullable) listOf("NOT NULL") else emptyList()

                    result.add(ColumnDef(colName, typeStr, cons))
                }

            return result
        }
    }

    private inner class ForeignKeyGenerator {

        fun generateAllForeignKeys(entities: List<Class<*>>): List<String> {
            val out = mutableListOf<String>()

            entities.forEach { e ->
                val table = getTableName(e)

                allPersistentFields(e).forEach { f ->
                    val rel =
                        f.findAnnotationOnFieldOrGetter<ManyToOne>(e)
                            ?: f.findAnnotationOnFieldOrGetter<OneToOne>(e)
                    if (rel == null) return@forEach

                    val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(e)
                    val colName = join?.name?.ifBlank { null } ?: "${toSnakeCase(f.name)}_id"
                    val refTable = getTableName(f.type)
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
                        }
                    )
                }
            }

            return out
        }
    }

    private inner class IndexGenerator {

        fun generateAllIndexes(entities: List<Class<*>>): List<String> {
            val out = mutableListOf<String>()

            entities.forEach { e ->
                val table = getTableName(e)
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
                        }
                    )
                }

                // ------ Foreign-key indexes + heuristics ------
                allPersistentFields(e).forEach { f ->

                    // FK index
                    if (
                        f.findAnnotationOnFieldOrGetter<ManyToOne>(e) != null ||
                        f.findAnnotationOnFieldOrGetter<OneToOne>(e) != null
                    ) {
                        val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(e)
                        val col = join?.name?.ifBlank { null } ?: "${toSnakeCase(f.name)}_id"

                        // check if custom index already exists
                        val custom = tableAnn?.indexes?.any { idx ->
                            idx.columnList.split(",").any { it.trim() == col }
                        } ?: false

                        if (!custom) {
                            out.add(
                                buildString {
                                    appendLine("-- Index on foreign key: $table.$col")
                                    appendLine("CREATE INDEX idx_${table}_${col} ON $table ($col);")
                                }
                            )
                        }
                        return@forEach
                    }

                    // heuristic indexes for common lookup fields
                    val colAnn = f.findAnnotationOnFieldOrGetter<Column>(e)
                    val name = colAnn?.name?.ifBlank { null } ?: toSnakeCase(f.name)
                    if (name in listOf("email", "username", "subject", "code")) {
                        out.add(
                            buildString {
                                appendLine("-- Lookup index on $table.$name")
                                appendLine("CREATE INDEX idx_${table}_${name} ON $table ($name);")
                            }
                        )
                    }
                }
            }

            return out.distinct()
        }
    }

    private fun hashOf(text: String): String {
        val md = MessageDigest.getInstance("SHA-256")
        val hash = md.digest(text.toByteArray())
        return hash.joinToString("") { "%02x".format(it) }.take(16)
    }

    private fun addHeaderWithHash(sql: String): String {
        val hash = hashOf(sql)
        return "-- HASH: $hash\n$sql"
    }

    private fun writeMigrationFile(targetDir: File, sortNumber: String, baseName: String, content: String) {

        val newHash = hashOf(content)

        val pattern = Regex("""V\d{14}${sortNumber}__${baseName}\.sql""")

        val existingFiles = targetDir
            .listFiles()
            .orEmpty()
            .filter { it.name.matches(pattern) }

        val exactMatch = existingFiles.firstOrNull { file ->
            val first = file.useLines { it.firstOrNull() }
            first?.startsWith("-- HASH:") == true &&
                    first.substringAfter(": ").trim() == newHash
        }

        if (exactMatch != null) {
            logger.info { "Skipped (unchanged): ${exactMatch.name} -> ($exactMatch)" }
            return
        }


        val version = executionTimestamp + sortNumber // e.g. 202511201733580001
        val filename = "V${version}__${baseName}.sql"

        val newFile = File(targetDir, filename)
        newFile.writeText(addHeaderWithHash(content))
        logger.info { "Created: ${newFile.name} -> ($newFile)" }
    }

    internal fun testWriteFileForTest(
        dir: File,
        baseName: String,
        sortNumber: String,
        content: String
    ) {
        executionTimestamp =  buildTimestamp() //needed for tests
        writeMigrationFile(targetDir = dir, sortNumber = sortNumber, baseName = baseName, content = content)
    }

    internal fun testFindEntities(): List<Class<*>> = entityScanner.findEntities()


}

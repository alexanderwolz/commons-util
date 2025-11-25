package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.log.Logger
import de.alexanderwolz.commons.util.database.migration.*
import de.alexanderwolz.commons.util.database.migration.schema.*
import de.alexanderwolz.commons.util.database.provider.DefaultSchemaProvider
import de.alexanderwolz.commons.util.database.provider.SchemaProvider
import jakarta.persistence.*
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.security.MessageDigest
import java.time.LocalDateTime

class SchemaGenerator(
    private val basePackage: String,
    private val outDir: File,
    private val databaseType: DatabaseType = DatabaseType.POSTGRES,
    private val uuidType: UUIDType = UUIDType.UUID_V7,
    private val provider: SchemaProvider = DefaultSchemaProvider(),
    private val clearTargetFolders: Boolean = false,
    private val migrationMode: MigrationMode = MigrationMode.SMART
) {

    private val logger = Logger(javaClass)
    private val entityScanner = EntityScanner(basePackage)

    enum class DatabaseType { POSTGRES, MARIADB }
    enum class UUIDType { UUID_V4, UUID_V7 }
    enum class MigrationMode { CREATE_ONLY, ALTER_ONLY, SMART }

    private var executionTimestamp: LocalDateTime? = null

    private val sqlFileSchemaExtractor = SqlFileSchemaExtractor(outDir)
    private val indexGenerator = IndexGenerator()
    private val foreignKeyGenerator = ForeignKeyGenerator()
    private val tableGenerator = TableSqlGenerator()


    fun generate() {
        try {
            executionTimestamp = LocalDateTime.now()

            logger.info { "Generating SQL migrations from classes within '$basePackage'" }
            logger.info { "Database Type: $databaseType, UUID Type: $uuidType, Mode: $migrationMode" }

            val rawEntities = entityScanner.findEntities()
            DatabaseMigrationUtils.validateUniqueTableNames(rawEntities)
            val entities = rawEntities.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }

            when (migrationMode) {
                MigrationMode.CREATE_ONLY -> generateCreateMode(entities)
                MigrationMode.ALTER_ONLY -> generateAlterMode(entities)
                MigrationMode.SMART -> generateSmartMode(entities)
            }

            logger.info { "done" }
        } finally {
            executionTimestamp = LocalDateTime.now()
        }
    }

    private fun generateCreateMode(entities: List<Class<*>>) {
        generateSetupFiles(entities)
        DatabaseMigrationUtils.groupByPartition(entities, provider, outDir).forEach { (schema, entitiesInPartition) ->
            val sorted = entitiesInPartition.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }
            generateFiles(sorted, schema)
        }
    }

    private fun generateAlterMode(entities: List<Class<*>>) {
        DatabaseMigrationUtils.groupByPartition(entities, provider, outDir).forEach { (schema, entitiesInPartition) ->
            val sorted = entitiesInPartition.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }
            generateAlterScripts(sorted, schema)
        }
    }

    private fun generateSmartMode(entities: List<Class<*>>) {

        val entitiesBySchema = DatabaseMigrationUtils.groupByPartition(entities, provider, outDir)

        val existingTablesBySchema = entitiesBySchema.mapValues { (schema, _) ->
            sqlFileSchemaExtractor.getExistingTables(schema)
        }

        val newEntities = mutableListOf<Class<*>>()
        val existingEntities = mutableListOf<Class<*>>()

        entities.forEach { entity ->
            val schema = provider.getFolderFor(entity, outDir)?.trim().orEmpty().ifBlank { "default" }
            val table = DatabaseMigrationUtils.getTableName(entity)

            val exists = existingTablesBySchema[schema]?.contains(table) == true

            if (exists) existingEntities += entity
            else newEntities += entity
        }

        if (newEntities.isNotEmpty()) {
            logger.info { "Found ${newEntities.size} new entities -> generating CREATE scripts" }
            generateSetupFiles(newEntities)

            val groupedNew = DatabaseMigrationUtils.groupByPartition(newEntities, provider, outDir)
            groupedNew.forEach { (schema, entitiesInPartition) ->
                val sorted = entitiesInPartition.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }
                generateFiles(sorted, schema)
            }
        }

        if (existingEntities.isNotEmpty()) {
            logger.info { "Found ${existingEntities.size} existing entities -> checking for changes" }

            val groupedExisting = DatabaseMigrationUtils.groupByPartition(existingEntities, provider, outDir)
            groupedExisting.forEach { (schema, entitiesInPartition) ->
                val sorted = entitiesInPartition.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }
                generateAlterScripts(sorted, schema)
            }
        }
    }


    private fun generateAlterScripts(entities: List<Class<*>>, schema: String) {
        if (entities.isEmpty()) return

        val target = prepareTargetDirectory(schema)
        val migrationGen = MigrationGenerator()

        val sortedEntities = entities.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }
        val sortNumberMap = sortedEntities.mapIndexed { index, entity ->
            DatabaseMigrationUtils.getTableName(entity) to (1000 + index).toString().padStart(4, '0')
        }.toMap()

        entities.forEach { entity ->
            val table = DatabaseMigrationUtils.getTableName(entity)
            val currentSchema = extractCurrentSchema(entity)

            val previousSchema = sqlFileSchemaExtractor.loadTableSchema(schema, table)

            if (previousSchema == null) {
                logger.warn { "No previous CREATE TABLE found for $table - consider using CREATE mode first" }
                return@forEach
            }

            val diff = migrationGen.compareSchemasAndGenerateMigration(
                tableName = table, entityClass = entity, oldSchema = previousSchema, newSchema = currentSchema
            )

            if (diff.isEmpty()) {
                logger.info { "No changes detected for $table" }
                return@forEach
            } else {
                val sortNumber = sortNumberMap[table] ?: generateMigrationNumber()
                val sql = formatPlainSql(diff)
                writeMigrationFile(
                    targetDir = target, sortNumber = sortNumber, baseName = "alter_${table}_table", content = sql
                )
            }
        }
    }

    private fun getIdType(entityClass: Class<*>): String {
        val idField = entityClass.idField() ?: return if (databaseType == DatabaseType.POSTGRES) "BIGINT" else "BIGINT"

        val col = idField.getAnnotation(Column::class.java)
        val gen = idField.getAnnotation(GeneratedValue::class.java)

        if (gen?.strategy == GenerationType.UUID) return if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

        if (gen?.strategy == GenerationType.IDENTITY) return if (databaseType == DatabaseType.POSTGRES) "BIGSERIAL" else "BIGINT"

        if (idField.type.simpleName == "UUID") return if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

        return sqlType(idField, col)
    }

    private fun extractCurrentSchema(entity: Class<*>): TableSchema {
        val columns = mutableListOf<ColumnSchema>()
        val indexes = mutableListOf<IndexSchema>()
        val foreignKeys = mutableListOf<ForeignKeySchema>()

        val tableAnn = entity.getAnnotation(Table::class.java)
        val tableName = DatabaseMigrationUtils.getTableName(entity)

        // ----------------------------------------------------------------------------
        //                                COLUMNS
        // ----------------------------------------------------------------------------
        DatabaseMigrationUtils.allPersistentFields(entity).forEach { f ->
            val colAnn = DatabaseMigrationUtils.findAnnotation<Column>(f, entity)

            // ---------- PRIMARY KEY ----------
            if (DatabaseMigrationUtils.findAnnotation<Id>(f, entity) != null) {
                val colName = colAnn?.name?.ifBlank { null } ?: DatabaseMigrationUtils.toSnakeCase(f.name)
                val gen = DatabaseMigrationUtils.findAnnotation<GeneratedValue>(f, entity)

                val type = when (gen?.strategy) {
                    GenerationType.UUID -> if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

                    GenerationType.IDENTITY -> if (databaseType == DatabaseType.POSTGRES) "BIGSERIAL" else "BIGINT"

                    else -> sqlType(f, colAnn)
                }

                // FIX: Include the defaultValue for UUID generation to match generated SQL
                val defaultValue = when (gen?.strategy) {
                    GenerationType.UUID -> when (databaseType) {
                        DatabaseType.POSTGRES -> when (uuidType) {
                            UUIDType.UUID_V7 -> "public.uuid_generate_v7()"
                            UUIDType.UUID_V4 -> "public.uuid_generate_v4()"
                        }

                        DatabaseType.MARIADB -> "(UUID())"
                    }

                    else -> null
                }

                columns.add(
                    ColumnSchema(
                        name = colName,
                        type = type,
                        nullable = false,
                        isPrimaryKey = true,
                        defaultValue = defaultValue  // FIX: Added to match generated SQL
                    )
                )
                return@forEach
            }

            // ---------- RELATIONS (ManyToOne / OneToOne) ----------
            val relMany = DatabaseMigrationUtils.findAnnotation<ManyToOne>(f, entity)
            val relOne = DatabaseMigrationUtils.findAnnotation<OneToOne>(f, entity)
            if (relMany != null || relOne != null) {
                val join = DatabaseMigrationUtils.findAnnotation<JoinColumn>(f, entity)
                val name = join?.name?.ifBlank { null } ?: "${DatabaseMigrationUtils.toSnakeCase(f.name)}_id"
                val nullable = join?.nullable ?: true
                val refType = getIdType(f.type)
                val refTable = DatabaseMigrationUtils.getTableName(f.type)

                columns.add(
                    ColumnSchema(
                        name = name, type = refType, nullable = nullable
                    )
                )

                foreignKeys.add(
                    ForeignKeySchema(
                        columnName = name,
                        referencedTable = refTable,
                        referencedColumn = "id",
                        onDelete = if (nullable) "SET NULL" else "CASCADE"
                    )
                )
                return@forEach
            }

            // ---------- EMBEDDED ----------
            if (DatabaseMigrationUtils.findAnnotation<Embedded>(f, entity) != null) {
                val embType = f.type
                val prefix = DatabaseMigrationUtils.toSnakeCase(f.name) + "_"

                if (embType.isAnnotationPresent(Embeddable::class.java)) {
                    embType.declaredFields.filter { !it.isSynthetic && !Modifier.isStatic(it.modifiers) }
                        .forEach { ef ->

                            val subColAnn = ef.getAnnotation(Column::class.java)
                            val subName =
                                subColAnn?.name?.ifBlank { null } ?: (prefix + DatabaseMigrationUtils.toSnakeCase(ef.name))

                            val type = sqlType(ef, subColAnn)
                            val nullable = subColAnn?.nullable ?: true
                            val unique = subColAnn?.unique ?: false

                            columns.add(
                                ColumnSchema(
                                    name = subName, type = type, nullable = nullable, unique = unique
                                )
                            )
                        }
                }

                return@forEach
            }


            // ---------- NORMAL COLUMNS ----------
            val colName = colAnn?.name?.ifBlank { null } ?: DatabaseMigrationUtils.toSnakeCase(f.name)
            val nullable = colAnn?.nullable ?: true
            val unique = colAnn?.unique ?: false
            val type = sqlType(f, colAnn)
            // FIX: Include both explicit defaults AND implicit timestamp defaults
            val defaultValue = DatabaseMigrationUtils.extractDefaultValue(colAnn) ?: timestampDefault(colName)

            columns.add(
                ColumnSchema(
                    name = colName, type = type, nullable = nullable, unique = unique, defaultValue = defaultValue
                )
            )
        }

        // ----------------------------------------------------------------------------
        //                                INDEXES (custom via @Table)
        // ----------------------------------------------------------------------------
        tableAnn?.indexes?.forEach { idx ->
            val name = idx.name.ifBlank {
                "idx_${tableName}_${idx.columnList.replace(",", "_").replace(" ", "")}"
            }

            val cols = idx.columnList.split(",").map { it.trim() }

            indexes.add(
                IndexSchema(
                    name = name, columns = cols, unique = idx.unique
                )
            )
        }

        // ----------------------------------------------------------------------------
        //              INDEXES (ForeignKey indexes + heuristic lookup indexes)
        // ----------------------------------------------------------------------------

        DatabaseMigrationUtils.allPersistentFields(entity).forEach { f ->

            // ---------- FK-INDEXE ----------
            if (DatabaseMigrationUtils.findAnnotation<ManyToOne>(f, entity) != null || DatabaseMigrationUtils.findAnnotation<OneToOne>(
                    f,
                    entity
                ) != null
            ) {
                val join = DatabaseMigrationUtils.findAnnotation<JoinColumn>(f, entity)
                val col = join?.name?.ifBlank { null } ?: "${DatabaseMigrationUtils.toSnakeCase(f.name)}_id"

                val customExists = tableAnn?.indexes?.any { idx ->
                    idx.columnList.split(",").any { it.trim() == col }
                } ?: false

                if (!customExists) {
                    val idxName = "idx_${tableName}_${col}"
                    if (indexes.none { it.name == idxName && it.columns == listOf(col) }) {
                        indexes.add(
                            IndexSchema(
                                name = idxName, columns = listOf(col), unique = false
                            )
                        )
                    }
                }
                return@forEach
            }

            // ---------- HEURISTISCHE LOOKUP-INDEXE ----------
            val colAnn = DatabaseMigrationUtils.findAnnotation<Column>(f, entity)
            val colName = colAnn?.name?.ifBlank { null } ?: DatabaseMigrationUtils.toSnakeCase(f.name)

            if (colName in listOf("email", "username", "subject", "code")) {
                val idxName = "idx_${tableName}_${colName}"
                if (indexes.none { it.name == idxName && it.columns == listOf(colName) }) {
                    indexes.add(
                        IndexSchema(
                            name = idxName, columns = listOf(colName), unique = false
                        )
                    )
                }
            }
        }

        return TableSchema(
            columns = columns, indexes = indexes, foreignKeys = foreignKeys
        )
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

    private fun generateSetupFiles(entities: List<Class<*>>) {
        val setupFolder = provider.getSetupFolder(outDir) ?: ""

        if (databaseType == DatabaseType.POSTGRES && uuidType == UUIDType.UUID_V7) {
            val usesUuid = entities.any { it.idField() != null }
            if (usesUuid) {
                val target = prepareTargetDirectory(setupFolder)
                val sql = formatPlainSql(TableSqlGenerator().generateUuidExtensionSetup())
                writeMigrationFile(
                    targetDir = target, sortNumber = "0001", baseName = "setup_uuid_extension", content = sql
                )
            }
        }
    }

    private fun generateFiles(entities: List<Class<*>>, schema: String) {
        if (entities.isEmpty()) return

        val target = prepareTargetDirectory(schema)

        entities.forEachIndexed { index, entity ->
            val table = DatabaseMigrationUtils.getTableName(entity)
            val sql = DatabaseMigrationUtils.formatCreateTableSql(tableGenerator.generateCreateTableSql(entity, table))
            val sortNumber = (1000 + index).toString().padStart(4, '0')
            writeMigrationFile(
                targetDir = target, sortNumber = sortNumber, baseName = "create_${table}_table", content = sql
            )
        }

        val fkList = foreignKeyGenerator.generateAllForeignKeys(entities)
        if (fkList.isNotEmpty()) {
            val sql = "-- Foreign Keys\n" + fkList.joinToString("\n\n")
            writeMigrationFile(
                targetDir = target, sortNumber = "5000", baseName = "add_foreign_keys", content = formatPlainSql(sql)
            )
        }

        val idxList = indexGenerator.generateAllIndexes(entities)
        if (idxList.isNotEmpty()) {
            val sql = "-- Indexes\n" + idxList.joinToString("\n\n")
            writeMigrationFile(
                targetDir = target, sortNumber = "9000", baseName = "add_indexes", content = formatPlainSql(sql)
            )
        }
    }

    private fun prepareTargetDirectory(schemaKey: String): File {
        return File(outDir, schemaKey.lowercase()).apply {
            mkdirs()
            if (clearTargetFolders) {
                logger.info { "Clearing target folder ${this.absolutePath}" }
                listFiles()?.forEach { it.deleteRecursively() }
            }
        }
    }


    private fun stableSortKey(tableName: String): String {
        return tableName.lowercase()
    }

    private fun Class<*>.idField(): Field? {
        return DatabaseMigrationUtils.allPersistentFields(this).firstOrNull {
            DatabaseMigrationUtils.findAnnotation<Id>(it, this) != null
        }
    }

    private fun generateMigrationNumber(): String {
        return System.currentTimeMillis().toString().takeLast(4)
    }

    private fun formatPlainSql(sql: String): String {
        return sql.trim()
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

        val pattern = provider.getFileNameRegex(
            timestamp = executionTimestamp!!, sortNumber = sortNumber, baseName = baseName
        )

        val existingFiles = targetDir.listFiles().orEmpty().filter { it.name.matches(pattern) }

        val exactMatch = existingFiles.firstOrNull { file ->
            val first = file.useLines { it.firstOrNull() }
            first?.startsWith("-- HASH:") == true && first.substringAfter(": ").trim() == newHash
        }

        if (exactMatch != null) {
            logger.info { "Skipped (unchanged): ${exactMatch.name} -> ($exactMatch)" }
            return
        }


        val filename = provider.getFileName(
            timestamp = executionTimestamp!!, sortNumber = sortNumber, baseName = baseName
        )

        val newFile = File(targetDir, filename)
        val content = addHeaderWithHash(content)
        newFile.writeText(content)
        logger.info { "Created: ${newFile.name} -> ($newFile)" }
    }

    private inner class TableSqlGenerator {

        fun generateCreateTableSql(entity: Class<*>, tableName: String): String {
            val cols = mutableListOf<ColumnDef>()

            DatabaseMigrationUtils.allPersistentFields(entity).forEach { f ->
                val colAnn = DatabaseMigrationUtils.findAnnotation<Column>(f, entity)

                // ----- ID FIELD -----
                if (DatabaseMigrationUtils.findAnnotation<Id>(f, entity) != null) {
                    val colName = colAnn?.name?.ifBlank { null } ?: DatabaseMigrationUtils.toSnakeCase(f.name)
                    val gen = DatabaseMigrationUtils.findAnnotation<GeneratedValue>(f, entity)

                    val type = when (gen?.strategy) {
                        GenerationType.UUID -> if (databaseType == DatabaseType.POSTGRES) "UUID" else "CHAR(36)"

                        GenerationType.IDENTITY -> if (databaseType == DatabaseType.POSTGRES) "BIGSERIAL" else "BIGINT"

                        else -> sqlType(f, colAnn)
                    }

                    val constraints = mutableListOf("PRIMARY KEY")
                    if (gen?.strategy == GenerationType.UUID) constraints.add(uuidDefaultSql())
                    if (gen?.strategy == GenerationType.IDENTITY && databaseType == DatabaseType.MARIADB) constraints.add(
                        "AUTO_INCREMENT"
                    )

                    cols.add(ColumnDef(colName, type, constraints))
                    return@forEach
                }

                // ----- RELATIONS -----
                val relMany = DatabaseMigrationUtils.findAnnotation<ManyToOne>(f, entity)
                val relOne = DatabaseMigrationUtils.findAnnotation<OneToOne>(f, entity)
                if (relMany != null || relOne != null) {
                    val join = DatabaseMigrationUtils.findAnnotation<JoinColumn>(f, entity)
                    val name = join?.name?.ifBlank { null } ?: "${DatabaseMigrationUtils.toSnakeCase(f.name)}_id"
                    val nullable = join?.nullable ?: true
                    val refType = getIdType(f.type)

                    val cons = if (!nullable) listOf("NOT NULL") else emptyList()
                    cols.add(ColumnDef(name, refType, cons))
                    return@forEach
                }

                // Skip OneToMany or ManyToMany
                if (DatabaseMigrationUtils.findAnnotation<OneToMany>(
                        f, entity
                    ) != null || DatabaseMigrationUtils.findAnnotation<ManyToMany>(
                        f, entity
                    ) != null
                ) return@forEach

                // ----- EMBEDDED -----
                if (DatabaseMigrationUtils.findAnnotation<Embedded>(f, entity) != null) {
                    cols.addAll(resolveEmbeddedColumns(f))
                    return@forEach
                }

                // ----- NORMAL COLUMNS -----
                val name = colAnn?.name?.ifBlank { null } ?: DatabaseMigrationUtils.toSnakeCase(f.name)
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
            val prefix = DatabaseMigrationUtils.toSnakeCase(field.name) + "_"
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

            type.declaredFields.filter { !it.isSynthetic && !Modifier.isStatic(it.modifiers) }.forEach { ef ->
                val override = overrides[ef.name]
                val colAnn = override ?: ef.getAnnotation(Column::class.java)

                val colName = override?.name?.ifBlank { null } ?: colAnn?.name?.ifBlank { null }
                ?: (prefix + DatabaseMigrationUtils.toSnakeCase(ef.name))

                val nullable = colAnn?.nullable ?: true
                val typeStr = sqlType(ef, colAnn)
                val cons = if (!nullable) listOf("NOT NULL") else emptyList()

                result.add(ColumnDef(colName, typeStr, cons))
            }

            return result
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


    internal fun testWriteFileForTest(
        dir: File, baseName: String, sortNumber: String, content: String
    ) {
        executionTimestamp = LocalDateTime.now()
        writeMigrationFile(targetDir = dir, sortNumber = sortNumber, baseName = baseName, content = content)
    }

    internal fun testFindEntities(): List<Class<*>> = entityScanner.findEntities()

    internal fun testGenerateAlterForTest(
        entityClass: Class<*>, schema: String, oldSchema: TableSchema, newSchema: TableSchema
    ): File? {
        executionTimestamp = LocalDateTime.now()

        val target = prepareTargetDirectory(schema)
        val migrationGen = MigrationGenerator()
        val table = DatabaseMigrationUtils.getTableName(entityClass)

        val diff = migrationGen.compareSchemasAndGenerateMigration(
            tableName = table, entityClass = entityClass, oldSchema = oldSchema, newSchema = newSchema
        )

        if (diff.isEmpty()) {
            logger.info { "No changes detected for $table" }
            return null
        }

        val allEntities = entityScanner.findEntities()
        val sortedEntities = allEntities.sortedBy { stableSortKey(DatabaseMigrationUtils.getTableName(it)) }
        val sortNumber = sortedEntities.indexOfFirst { DatabaseMigrationUtils.getTableName(it) == table }
            .let { if (it >= 0) (1000 + it).toString().padStart(4, '0') else "9999" }

        val sql = formatPlainSql(diff)
        writeMigrationFile(
            targetDir = target, sortNumber = sortNumber, baseName = "alter_${table}_table", content = sql
        )

        val filename = provider.getFileName(
            timestamp = executionTimestamp!!, sortNumber = sortNumber, baseName = "alter_${table}_table"
        )

        return File(target, filename)
    }

}
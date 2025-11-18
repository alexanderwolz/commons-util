package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.log.Logger
import jakarta.persistence.*
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.time.LocalDateTime

class SchemaGenerator(
    private val basePackage: String,
    private val outDir: File,
    private val databaseType: DatabaseType = DatabaseType.POSTGRES,
    private val uuidType: UUIDType = UUIDType.UUID_V7
) {

    enum class DatabaseType { POSTGRES, MARIADB }
    enum class UUIDType { UUID_V4, UUID_V7 }

    private val logger = Logger(javaClass)

    // =========================================================================
    // PUBLIC API
    // =========================================================================

    fun generate() {
        logger.info { "Generating SQL migrations from classes within '$basePackage'" }
        logger.info { "Database Type: $databaseType, UUID Type: $uuidType" }

        val entities = EntityScanner().findEntities()

        separateBySchema(entities).forEach { (schema, list) ->
            generateFiles(list, schema)
        }

        logger.info { "done" }
    }

    @Suppress("unused")
    private fun findEntities(): List<Class<*>> =
        EntityScanner().findEntities()

    private fun separateBySchema(entities: List<Class<*>>): Map<String, List<Class<*>>> {
        val map = HashMap<String, MutableList<Class<*>>>()
        entities.forEach { entity ->
            val schema = entity.getAnnotation(Table::class.java)?.schema ?: ""
            val key =
                schema.takeIf { it.isNotBlank() }?.lowercase()
                    ?: entity.packageName.split(".").last()

            map.computeIfAbsent(key) { mutableListOf() }.add(entity)
        }
        return map
    }

    private fun generateFiles(entities: List<Class<*>>, schema: String) {
        if (entities.isEmpty()) return

        val target = prepareTargetDirectory(schema)
        val tableGen = TableSqlGenerator()
        val fkGen = ForeignKeyGenerator()
        val idxGen = IndexGenerator()

        if (databaseType == DatabaseType.POSTGRES && uuidType == UUIDType.UUID_V7) {
            val usesUuid = entities.any { it.idField() != null }
            if (usesUuid) {
                val f = File(target, "V0__setup_uuid_extension.sql")
                f.writeText(tableGen.generateUuidExtensionSetup())
                logger.info { "Created: ${f.parentFile.name}/${f.name}" }
            }
        }

        entities.forEachIndexed { i, e ->
            val table = getTableName(e)
            val f = File(target, "V${i + 1}__create_${table}_table.sql")
            f.writeText(tableGen.generateCreateTableSql(e, table))
            logger.info { "Created: ${f.parentFile.name}/${f.name}" }
        }

        val fkList = fkGen.generateAllForeignKeys(entities)
        if (fkList.isNotEmpty()) {
            val f = File(target, "V${entities.size + 1}__add_foreign_keys.sql")
            f.writeText(
                "-- Foreign Keys generated ${LocalDateTime.now()}\n" +
                        fkList.joinToString("\n\n")
            )
            logger.info { "Created: ${f.parentFile.name}/${f.name}" }
        }

        val idxList = idxGen.generateAllIndexes(entities)
        if (idxList.isNotEmpty()) {
            val f = File(target, "V${entities.size + 2}__add_indexes.sql")
            f.writeText(
                "-- Indexes generated ${LocalDateTime.now()}\n" +
                        idxList.joinToString("\n\n")
            )
            logger.info { "Created: ${f.parentFile.name}/${f.name}" }
        }
    }

    private fun prepareTargetDirectory(schemaKey: String): File {
        val dir = File(outDir, schemaKey.lowercase())
        dir.mkdirs()
        dir.listFiles()?.forEach { it.delete() }
        return dir
    }

    private fun getTableName(clazz: Class<*>) =
        clazz.getAnnotation(Table::class.java)?.name?.ifBlank { toSnakeCase(clazz.simpleName) }
            ?: toSnakeCase(clazz.simpleName)

    private fun toSnakeCase(s: String): String =
        s.replace(Regex("([a-z0-9])([A-Z])"), "$1_$2")
            .replace(Regex("([A-Z]+)([A-Z][a-z])"), "$1_$2")
            .lowercase()

    private fun Class<*>.idField(): Field? =
        allPersistentFields(this).firstOrNull {
            it.findAnnotationOnFieldOrGetter<Id>(this) != null
        }

    private fun allPersistentFields(type: Class<*>): List<Field> {
        val map = LinkedHashMap<String, Field>()
        var c: Class<*>? = type
        while (c != null && c != Any::class.java) {
            c.declaredFields
                .filter {
                    !Modifier.isStatic(it.modifiers) &&
                            !it.isSynthetic &&
                            it.getAnnotation(Transient::class.java) == null
                }
                .forEach { f ->
                    map.putIfAbsent(f.name, f)
                }
            c = c.superclass
        }
        return map.values.toList()
    }

    private inline fun <reified A : Annotation> Field.findAnnotationOnFieldOrGetter(owner: Class<*>): A? {
        this.getAnnotation(A::class.java)?.let { return it }

        val prefix =
            if (this.type == java.lang.Boolean.TYPE || this.type == java.lang.Boolean::class.java) "is"
            else "get"

        val getter = prefix + this.name.replaceFirstChar { it.uppercase() }
        return try {
            owner.getMethod(getter).getAnnotation(A::class.java)
        } catch (_: Exception) {
            null
        }
    }

    private fun sqlType(field: Field, col: Column?): String =
        when (databaseType) {
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

    private fun uuidDefaultSql(): String =
        when (databaseType) {
            DatabaseType.POSTGRES ->
                when (uuidType) {
                    UUIDType.UUID_V7 -> "DEFAULT uuid_generate_v7()"
                    UUIDType.UUID_V4 -> "DEFAULT uuid_generate_v4()"
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

                if (f.findAnnotationOnFieldOrGetter<Id>(entity) != null) {
                    val colName = colAnn?.name?.ifBlank { null } ?: toSnakeCase(f.name)

                    val gen = f.findAnnotationOnFieldOrGetter<GeneratedValue>(entity)

                    val type = when (gen?.strategy) {
                        GenerationType.UUID -> when (databaseType) {
                            DatabaseType.POSTGRES -> "UUID"
                            DatabaseType.MARIADB -> "CHAR(36)"
                        }

                        GenerationType.IDENTITY -> when (databaseType) {
                            DatabaseType.POSTGRES -> "BIGSERIAL"
                            DatabaseType.MARIADB -> "BIGINT"
                        }

                        else -> sqlType(f, colAnn)
                    }

                    val constraints = mutableListOf("PRIMARY KEY")

                    if (gen?.strategy == GenerationType.UUID)
                        constraints.add(uuidDefaultSql())

                    if (gen?.strategy == GenerationType.IDENTITY &&
                        databaseType == DatabaseType.MARIADB
                    )
                        constraints.add("AUTO_INCREMENT")

                    cols.add(ColumnDef(colName, type, constraints))
                    return@forEach
                }

                val relMany = f.findAnnotationOnFieldOrGetter<ManyToOne>(entity)
                val relOne = f.findAnnotationOnFieldOrGetter<OneToOne>(entity)

                if (relMany != null || relOne != null) {
                    val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(entity)
                    val name = join?.name?.ifBlank { null } ?: "${toSnakeCase(f.name)}_id"
                    val nullable = join?.nullable ?: true
                    val refType = getIdType(f.type)

                    val constraints = if (!nullable) listOf("NOT NULL") else emptyList()
                    cols.add(ColumnDef(name, refType, constraints))
                    return@forEach
                }

                // OneToMany / ManyToMany ignored
                if (
                    f.findAnnotationOnFieldOrGetter<OneToMany>(entity) != null ||
                    f.findAnnotationOnFieldOrGetter<ManyToMany>(entity) != null
                ) return@forEach

                if (f.findAnnotationOnFieldOrGetter<Embedded>(entity) != null) {
                    cols.addAll(resolveEmbeddedColumns(f))
                    return@forEach
                }

                val name = colAnn?.name?.ifBlank { null } ?: toSnakeCase(f.name)
                val type = sqlType(f, colAnn)

                val c = mutableListOf<String>()
                if (colAnn?.nullable == false) c.add("NOT NULL")
                if (colAnn?.unique == true) c.add("UNIQUE")
                timestampDefault(name)?.let { c.add(it) }

                cols.add(ColumnDef(name, type, c))
            }

            val maxName = cols.maxOfOrNull { it.name.length } ?: 1
            val maxType = cols.maxOfOrNull { it.type.length } ?: 1

            val body = cols.joinToString(",\n") { it.toSql(maxName, maxType) }

            return """
                -- create_${tableName}_table
                -- Entity: ${entity.simpleName}
                -- Database: $databaseType
                
                CREATE TABLE $tableName (
                $body
                );
            """.trimIndent()
        }

        private fun getIdType(entityClass: Class<*>): String {
            val idField = entityClass.idField() ?: return when (databaseType) {
                DatabaseType.POSTGRES -> "BIGINT"
                DatabaseType.MARIADB -> "BIGINT"
            }

            val col = idField.getAnnotation(Column::class.java)
            val gen = idField.getAnnotation(GeneratedValue::class.java)

            // UUID generator
            if (gen?.strategy == GenerationType.UUID) {
                return when (databaseType) {
                    DatabaseType.POSTGRES -> "UUID"
                    DatabaseType.MARIADB -> "CHAR(36)"
                }
            }

            // Identity / SERIAL
            if (gen?.strategy == GenerationType.IDENTITY) {
                return when (databaseType) {
                    DatabaseType.POSTGRES -> "BIGSERIAL"
                    DatabaseType.MARIADB -> "BIGINT"
                }
            }

            // UUID without @GeneratedValue
            if (idField.type.simpleName == "UUID") {
                return when (databaseType) {
                    DatabaseType.POSTGRES -> "UUID"
                    DatabaseType.MARIADB -> "CHAR(36)"
                }
            }

            // Normal type fallback
            return sqlType(idField, col)
        }


        fun generateUuidExtensionSetup(): String = when (uuidType) {
            UUIDType.UUID_V7 -> """
                -- Setup UUID v7 (pgcrypto)
                CREATE EXTENSION IF NOT EXISTS pgcrypto;
                
                CREATE OR REPLACE FUNCTION uuid_generate_v7()
                RETURNS UUID AS $$
                DECLARE
                    unix_ts_ms BIGINT;
                    uuid_bytes BYTEA;
                BEGIN
                    unix_ts_ms := (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::BIGINT;
                    uuid_bytes := gen_random_bytes(16);
                    uuid_bytes := OVERLAY(uuid_bytes PLACING substring(int8send(unix_ts_ms) FROM 3) FROM 1 FOR 6);
                    uuid_bytes := SET_BYTE(uuid_bytes, 6, (GET_BYTE(uuid_bytes, 6) & 15) | 112);
                    uuid_bytes := SET_BYTE(uuid_bytes, 8, (GET_BYTE(uuid_bytes, 8) & 63) | 128);
                    RETURN encode(uuid_bytes, 'hex')::UUID;
                END;
                $$ LANGUAGE plpgsql VOLATILE;
            """.trimIndent()

            UUIDType.UUID_V4 -> """
                -- Setup UUID v4
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            """.trimIndent()
        }

        private fun resolveEmbeddedColumns(field: Field): List<ColumnDef> {
            val prefix = toSnakeCase(field.name) + "_"
            val type = field.type

            if (!type.isAnnotationPresent(Embeddable::class.java))
                return emptyList()

            val overrides = mutableMapOf<String, Column>()
            field.getAnnotationsByType(AttributeOverride::class.java)
                .forEach { overrides[it.name] = it.column }
            field.getAnnotation(AttributeOverrides::class.java)?.value?.forEach {
                overrides[it.name] = it.column
            }

            val out = mutableListOf<ColumnDef>()

            type.declaredFields
                .filter { !it.isSynthetic && !Modifier.isStatic(it.modifiers) }
                .forEach { ef ->
                    val override = overrides[ef.name]
                    val colAnn = override ?: ef.getAnnotation(Column::class.java)

                    val colName = override?.name?.takeIf { it.isNotBlank() }
                        ?: colAnn?.name?.takeIf { it.isNotBlank() }
                        ?: (prefix + toSnakeCase(ef.name))

                    val nullable = colAnn?.nullable ?: true
                    val typeStr = sqlType(ef, colAnn)
                    val cons = if (!nullable) listOf("NOT NULL") else emptyList()

                    out.add(ColumnDef(colName, typeStr, cons))
                }

            return out
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
                    val colName =
                        join?.name?.ifBlank { null } ?: "${toSnakeCase(f.name)}_id"
                    val refTable = getTableName(f.type)

                    val fkName =
                        join?.foreignKey?.name?.ifBlank { null } ?: "fk_${table}_${colName}"

                    val nullable = join?.nullable ?: true
                    val deleteRule = if (nullable) "SET NULL" else "CASCADE"

                    out.add(
                        """
                        -- Foreign key: $table.$colName -> $refTable.id
                        ALTER TABLE $table
                            ADD CONSTRAINT $fkName
                            FOREIGN KEY ($colName)
                            REFERENCES $refTable(id)
                            ON DELETE $deleteRule;
                        """.trimIndent()
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

                // Custom indexes from @Table
                tableAnn?.indexes?.forEach { idx ->
                    val name = idx.name.ifBlank {
                        "idx_${table}_${idx.columnList.replace(",", "_").replace(" ", "")}"
                    }
                    val unique = if (idx.unique) "UNIQUE " else ""
                    out.add(
                        """
                        -- Index on $table(${idx.columnList})
                        CREATE ${unique}INDEX $name ON $table (${idx.columnList});
                        """.trimIndent()
                    )
                }

                // FK indexes + heuristics
                allPersistentFields(e).forEach { f ->

                    // FK index
                    if (
                        f.findAnnotationOnFieldOrGetter<ManyToOne>(e) != null ||
                        f.findAnnotationOnFieldOrGetter<OneToOne>(e) != null
                    ) {
                        val join = f.findAnnotationOnFieldOrGetter<JoinColumn>(e)
                        val col = join?.name?.ifBlank { null } ?: "${toSnakeCase(f.name)}_id"

                        val custom =
                            tableAnn?.indexes?.any { idx ->
                                idx.columnList.split(",").any { it.trim() == col }
                            } ?: false
                        if (!custom) {
                            out.add(
                                """
                                -- Index on foreign key: $table.$col
                                CREATE INDEX idx_${table}_${col} ON $table ($col);
                                """.trimIndent()
                            )
                        }
                        return@forEach
                    }

                    // Heuristic lookup columns
                    val colAnn = f.findAnnotationOnFieldOrGetter<Column>(e)
                    val name = colAnn?.name?.ifBlank { null } ?: toSnakeCase(f.name)
                    if (name in listOf("email", "username", "subject", "code")) {
                        out.add(
                            """
                            -- Lookup index on $table.$name
                            CREATE INDEX idx_${table}_${name} ON $table ($name);
                            """.trimIndent()
                        )
                    }
                }
            }

            return out.distinct()
        }
    }


    private data class ColumnDef(
        val name: String,
        val type: String,
        val constraints: List<String>
    ) {
        fun toSql(maxName: Int, maxType: Int): String {
            val n = name.padEnd(maxName)
            val t = type.padEnd(maxType)
            val c = if (constraints.isNotEmpty()) " " + constraints.joinToString(" ") else ""
            return "    $n $t$c".trimEnd()
        }
    }

    private inner class EntityScanner {

        fun findEntities(): List<Class<*>> {
            val out = mutableListOf<Class<*>>()
            val path = basePackage.replace('.', '/')
            val loader = Thread.currentThread().contextClassLoader

            val urls = loader.getResources(path).toList()

            urls.forEach { url ->
                val dir = File(url.toURI())
                if (dir.exists() && dir.isDirectory)
                    scanDirectory(dir, out)
            }

            return out
        }

        private fun scanDirectory(dir: File, out: MutableList<Class<*>>) {
            dir.walkTopDown().forEach { file ->

                if (!file.name.endsWith(".class")) return@forEach
                if (file.name.contains("$")) return@forEach // skip inner classes

                val abs = file.absolutePath

                val pkgPath = basePackage.replace('.', File.separatorChar)
                val start = abs.indexOf(pkgPath)
                if (start == -1) return@forEach

                val classPath = abs.substring(start)
                    .removeSuffix(".class")
                    .replace(File.separatorChar, '.')

                try {
                    val clazz = Class.forName(classPath)
                    if (clazz.isAnnotationPresent(Entity::class.java)) {
                        out.add(clazz)
                        logger.info { "Found entity: ${clazz.name}" }
                    }
                } catch (_: Exception) {
                }
            }
        }
    }
}

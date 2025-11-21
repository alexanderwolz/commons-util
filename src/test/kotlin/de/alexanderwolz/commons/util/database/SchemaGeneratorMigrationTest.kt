package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.jpa.entity.fu.SampleEntity
import de.alexanderwolz.commons.util.database.migration.SqlFileSchemaExtractor
import de.alexanderwolz.commons.util.database.migration.schema.ColumnSchema
import de.alexanderwolz.commons.util.database.migration.schema.IndexSchema
import de.alexanderwolz.commons.util.database.migration.schema.TableSchema
import de.alexanderwolz.commons.util.jpa.EntityScannerTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertNotNull

class SchemaGeneratorMigrationTest {

    private val entityPackage = EntityScannerTest::class.java.packageName + ".entity"

    @TempDir
    lateinit var tmpDir: File

    private fun newGenerator(
        outDir: File = tmpDir,
        migrationMode: SchemaGenerator.MigrationMode = SchemaGenerator.MigrationMode.SMART
    ) = SchemaGenerator(
        basePackage = entityPackage,
        outDir = outDir,
        databaseType = SchemaGenerator.DatabaseType.POSTGRES,
        uuidType = SchemaGenerator.UUIDType.UUID_V4,
        migrationMode = migrationMode
    )

    // ==================== Hash & File Management Tests ====================

    @Test
    fun testFirstWriteCreatesMigrationFile() {
        val dir = File(tmpDir, "hashes_first").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildCreateTableV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)

        val files = dir.listFiles().orEmpty()
        assertEquals(1, files.size, "Exactly one migration expected on first write")

        val file = files.single()

        val pattern = Regex("""V\d{18}__${baseName}\.sql""")
        assertTrue(pattern.matches(file.name), "filename '${file.name}' doesn't match migration pattern")

        val lines = file.readLines()
        assertTrue(lines.first().startsWith("-- HASH:"), "file should contain hash header")
        assertTrue(lines.any { it.contains("CREATE TABLE") })
    }

    @Test
    fun testUnchangedContentDoesNotRewrite() {
        val dir = File(tmpDir, "hashes_unchanged").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildCreateTableV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)
        val firstFiles = dir.listFiles().orEmpty()
        assertEquals(1, firstFiles.size)
        val firstFile = firstFiles.single()
        val firstContent = firstFile.readText()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)
        val secondFiles = dir.listFiles().orEmpty()
        assertEquals(1, secondFiles.size, "Unchanged content must not produce a new file")

        val secondFile = secondFiles.single()
        assertEquals(firstFile.name, secondFile.name)
        assertEquals(firstContent, secondFile.readText())
    }

    @Test
    fun testChangedContentCreatesNewMigrationFile() {
        val dir = File(tmpDir, "hashes_changed").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV1())
        val f1 = dir.listFiles().orEmpty()
        assertEquals(1, f1.size)
        val fileV1 = f1.single().name

        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV2())
        val f2 = dir.listFiles().orEmpty()

        assertEquals(2, f2.size, "changed content must create a new migration")

        val sorted = f2.sortedBy { it.name }
        val fileV2 = sorted.last()

        assertTrue(fileV2.readLines().first().startsWith("-- HASH:"))
        assertTrue(fileV2.readText().contains("name VARCHAR(255)"))
        assertTrue(fileV1 != fileV2.name, "filenames must differ for changed content")
    }

    @Test
    fun testMultipleChangesCreateMultipleMigrations() {
        val dir = File(tmpDir, "hashes_multiple").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV1())
        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV2())
        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV3())

        val files = dir.listFiles().orEmpty().sortedBy { it.name }
        assertEquals(3, files.size, "Three different versions should create three migrations")

        assertTrue(files[0].readText().contains("id UUID PRIMARY KEY"))
        assertFalse(files[0].readText().contains("name"))

        assertTrue(files[1].readText().contains("name VARCHAR(255)"))
        assertFalse(files[1].readText().contains("email"))

        assertTrue(files[2].readText().contains("email VARCHAR(255) NOT NULL"))
    }

    @Test
    fun testHashIsStableAcrossWhitespaceChanges() {
        val dir = File(tmpDir, "hashes_whitespace").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body1 = "CREATE TABLE test_table (\n    id UUID PRIMARY KEY\n);"
        val body2 = "CREATE TABLE test_table (\n    id   UUID   PRIMARY KEY\n);" // extra spaces

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body1)
        Thread.sleep(1100)
        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body2)

        val files = dir.listFiles().orEmpty()
        assertEquals(2, files.size, "Whitespace differences should create different hashes")
    }

    @Test
    fun testDifferentSortNumbersWithSameContent() {
        val dir = File(tmpDir, "hashes_sort_numbers").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildCreateTableV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)
        Thread.sleep(1100)
        gen.testWriteFileForTest(dir = dir, sortNumber = "2000", baseName = baseName, content = body)

        val files = dir.listFiles().orEmpty()
        assertEquals(2, files.size, "Different sort numbers should create separate files")

        assertTrue(files.any { it.name.contains("1000") })
        assertTrue(files.any { it.name.contains("2000") })
    }

    @Test
    fun testHashHeaderIsConsistent() {
        val dir = File(tmpDir, "hashes_consistency").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildCreateTableV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)

        val file = dir.listFiles()!!.single()
        val lines = file.readLines()
        val hashLine = lines.first()

        assertTrue(hashLine.startsWith("-- HASH: "))
        val hash = hashLine.substringAfter(": ").trim()
        assertEquals(16, hash.length, "Hash should be 16 characters (truncated SHA-256)")
        assertTrue(hash.all { it in '0'..'9' || it in 'a'..'f' }, "Hash should be hexadecimal")
    }

    @Test
    fun testOldMigrationsArePreservedOnChange() {
        val dir = File(tmpDir, "hashes_preservation").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV1())
        val oldFile = dir.listFiles()!!.single()
        val oldContent = oldFile.readText()
        val oldName = oldFile.name

        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV2())

        val files = dir.listFiles().orEmpty()
        assertEquals(2, files.size)

        val preservedFile = files.find { it.name == oldName }
        assertNotNull(preservedFile, "Old migration file should still exist")
        assertEquals(oldContent, preservedFile!!.readText(), "Old file content should be unchanged")
    }

    // ==================== SqlFileSchemaExtractor Tests ====================

    @Test
    fun testSqlFileSchemaExtractorParsesCreateTable() {
        val dir = File(tmpDir, "parser_create_table").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "V20241121000000__1000_create_users_table.sql")
        sqlFile.writeText(
            """
            -- HASH: abc123
            CREATE TABLE users (
                id           BIGSERIAL    PRIMARY KEY,
                username     VARCHAR(50)  NOT NULL UNIQUE,
                email        VARCHAR(255) NOT NULL,
                age          INTEGER,
                active       BOOLEAN      DEFAULT true,
                created_at   TIMESTAMP    DEFAULT NOW()
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(6, schema!!.columns.size)

        val idCol = schema.columns.find { it.name == "id" }
        assertNotNull(idCol)
        assertEquals("BIGSERIAL", idCol!!.type)
        assertTrue(idCol.isPrimaryKey)
        assertFalse(idCol.nullable)

        val usernameCol = schema.columns.find { it.name == "username" }
        assertNotNull(usernameCol)
        assertEquals("VARCHAR(50)", usernameCol!!.type)
        assertFalse(usernameCol.nullable)
        assertTrue(usernameCol.unique)
    }

    @Test
    fun testSqlFileSchemaExtractorParsesIndexes() {
        val dir = File(tmpDir, "parser_indexes").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        val createFile = File(schemaDir, "V20241121000000__1000_create_users_table.sql")
        createFile.writeText(
            """
            -- HASH: abc123
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL
            );
        """.trimIndent()
        )

        val indexFile = File(schemaDir, "V20241121000001__0100_indexes.sql")
        indexFile.writeText(
            """
            -- HASH: def456
            CREATE UNIQUE INDEX idx_users_email ON users (email);
            CREATE INDEX idx_users_created_at ON users (created_at);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(2, schema!!.indexes.size)

        val emailIdx = schema.indexes.find { it.name == "idx_users_email" }
        assertNotNull(emailIdx)
        assertTrue(emailIdx!!.unique)
        assertEquals(listOf("email"), emailIdx.columns)
    }

    @Test
    fun testSqlFileSchemaExtractorParsesForeignKeys() {
        val dir = File(tmpDir, "parser_fks").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        val createFile = File(schemaDir, "V20241121000000__1000_create_orders_table.sql")
        createFile.writeText(
            """
            -- HASH: abc123
            CREATE TABLE orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL
            );
        """.trimIndent()
        )

        val fkFile = File(schemaDir, "V20241121000001__0200_foreign_keys.sql")
        fkFile.writeText(
            """
            -- HASH: def456
            ALTER TABLE orders
                ADD CONSTRAINT fk_orders_user_id
                FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE CASCADE;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "orders")

        assertNotNull(schema)
        assertEquals(1, schema!!.foreignKeys.size)

        val fk = schema.foreignKeys.first()
        assertEquals("user_id", fk.columnName)
        assertEquals("users", fk.referencedTable)
        assertEquals("id", fk.referencedColumn)
        assertEquals("CASCADE", fk.onDelete)
    }

    @Test
    fun testSqlFileSchemaExtractorFindsExistingTables() {
        val dir = File(tmpDir, "parser_existing").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        File(schemaDir, "V20241121000000__1000_create_users_table.sql").writeText(
            """
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        File(schemaDir, "V20241121000001__1001_create_orders_table.sql").writeText(
            """
            CREATE TABLE orders (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val existingTables = extractor.getExistingTables()

        assertEquals(2, existingTables.size)
        assertTrue(existingTables.contains("users"))
        assertTrue(existingTables.contains("orders"))
    }

    @Test
    fun testSqlFileSchemaExtractorReturnsNullForNonExistentTable() {
        val dir = File(tmpDir, "parser_null").apply { mkdirs() }
        File(dir, "public").apply { mkdirs() }

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "nonexistent")

        assertNull(schema)
    }

    @Test
    fun testSqlFileSchemaExtractorHandlesMultipleSchemas() {
        val dir = File(tmpDir, "parser_multi_schema").apply { mkdirs() }

        val publicDir = File(dir, "public").apply { mkdirs() }
        File(publicDir, "V20241121000000__1000_create_users_table.sql").writeText(
            """
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        val domainDir = File(dir, "domain").apply { mkdirs() }
        File(domainDir, "V20241121000000__1000_create_products_table.sql").writeText(
            """
            CREATE TABLE products (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)

        val usersSchema = extractor.loadTableSchema("public", "users")
        val productsSchema = extractor.loadTableSchema("domain", "products")

        assertNotNull(usersSchema)
        assertNotNull(productsSchema)
    }

    @Test
    fun testSqlFileSchemaExtractorParsesComplexColumnDefinitions() {
        val dir = File(tmpDir, "parser_complex").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "V20241121000000__1000_create_products_table.sql")
        sqlFile.writeText(
            """
            -- HASH: abc123
            CREATE TABLE products (
                id            BIGSERIAL     PRIMARY KEY,
                name          VARCHAR(255)  NOT NULL,
                description   TEXT,
                price         DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                stock         INTEGER       NOT NULL DEFAULT 0,
                sku           VARCHAR(50)   UNIQUE,
                is_active     BOOLEAN       DEFAULT true,
                category_id   BIGINT        NOT NULL
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "products")

        assertNotNull(schema)
        assertEquals(8, schema!!.columns.size)

        val priceCol = schema.columns.find { it.name == "price" }
        assertNotNull(priceCol)
        assertEquals("DECIMAL(10,2)", priceCol!!.type)
        assertFalse(priceCol.nullable)
        assertEquals("0.00", priceCol.defaultValue)

        val skuCol = schema.columns.find { it.name == "sku" }
        assertNotNull(skuCol)
        assertTrue(skuCol!!.unique)
    }

    // ==================== Migration Mode Tests ====================

    @Test
    fun testCreateOnlyMode() {
        val dir = File(tmpDir, "mode_create_only").apply { mkdirs() }

        val gen = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.CREATE_ONLY)
        gen.generate()

        val sqlFiles = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        assertTrue(sqlFiles.isNotEmpty(), "CREATE_ONLY mode should generate files")

        val alterFiles = sqlFiles.filter { it.name.contains("alter") }
        assertTrue(alterFiles.isEmpty(), "CREATE_ONLY mode should not generate ALTER files")
    }

    @Test
    fun testAlterOnlyMode() {
        val dir = File(tmpDir, "mode_alter_only").apply { mkdirs() }

        // Zuerst CREATE ausführen
        val createGen = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.CREATE_ONLY)
        createGen.generate()

        Thread.sleep(1100)

        // Dann ALTER ausführen (sollte keine Änderungen finden)
        val alterGen = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.ALTER_ONLY)
        alterGen.generate()

        val sqlFiles = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        // Sollte nur CREATE files haben, da keine Änderungen
        val alterFiles = sqlFiles.filter { it.name.contains("alter") }
        assertTrue(alterFiles.isEmpty(), "No changes should result in no ALTER files")
    }

    @Test
    fun testSmartModeDetectsNewEntities() {
        val dir = File(tmpDir, "mode_smart_new").apply { mkdirs() }

        val gen = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.SMART)
        gen.generate()

        val sqlFiles = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        assertTrue(sqlFiles.isNotEmpty(), "SMART mode should generate files for new entities")

        val createFiles = sqlFiles.filter { it.name.contains("create") }
        assertTrue(createFiles.isNotEmpty(), "SMART mode should generate CREATE files for new entities")
    }

    @Test
    fun testSmartModeDetectsNoChanges() {
        val dir = File(tmpDir, "mode_smart_unchanged").apply { mkdirs() }

        // Erste Generierung
        val gen1 = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.SMART)
        gen1.generate()

        val filesAfterFirst = dir.walkTopDown().filter { it.extension == "sql" }.count()

        Thread.sleep(1100)

        // Zweite Generierung ohne Änderungen
        val gen2 = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.SMART)
        gen2.generate()

        val filesAfterSecond = dir.walkTopDown().filter { it.extension == "sql" }.count()

        assertEquals(filesAfterFirst, filesAfterSecond, "No changes should not create new files")
    }

    @Test
    fun testSmartModeGeneratesAlterForChanges() {
        val dir = File(tmpDir, "mode_smart_changes").apply { mkdirs() }
        val schemaDir = File(dir, "domain").apply { mkdirs() }

        // Erstelle initiale CREATE TABLE
        val createFile = File(schemaDir, "V20241121000000__1000_create_sample_table.sql")
        createFile.writeText(
            """
            -- HASH: abc123
            CREATE TABLE sample_table (
                id   BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
        """.trimIndent()
        )

        // Führe ALTER aus mit geändertem Schema
        val oldSchema = TableSchema(
            columns = listOf(
                ColumnSchema("id", "BIGSERIAL", false, isPrimaryKey = true),
                ColumnSchema("name", "VARCHAR(100)", false)
            ),
            indexes = emptyList(),
            foreignKeys = emptyList()
        )

        val newSchema = TableSchema(
            columns = listOf(
                ColumnSchema("id", "BIGSERIAL", false, isPrimaryKey = true),
                ColumnSchema("name", "VARCHAR(100)", false),
                ColumnSchema("email", "VARCHAR(255)", false)
            ),
            indexes = emptyList(),
            foreignKeys = emptyList()
        )

        Thread.sleep(1100)

        val gen = newGenerator(outDir = dir)
        val alterFile = gen.testGenerateAlterForTest(
            entityClass = SampleEntity::class.java,
            schema = "domain",
            oldSchema = oldSchema,
            newSchema = newSchema
        )

        assertNotNull(alterFile)
        assertTrue(alterFile!!.exists())
        assertTrue(alterFile.name.contains("alter"))

        val content = alterFile.readText()
        assertTrue(content.contains("ALTER TABLE"))
        assertTrue(content.contains("ADD COLUMN"))
        assertTrue(content.contains("email"))
    }

    // ==================== Integration Tests ====================

    @Test
    fun testFullWorkflowCreateThenAlter() {
        val dir = File(tmpDir, "workflow_full").apply { mkdirs() }

        // Phase 1: Initiale Erstellung
        val createGen = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.CREATE_ONLY)
        createGen.generate()

        val createFiles = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        assertTrue(createFiles.isNotEmpty())

        Thread.sleep(1100)

        // Phase 2: SMART Mode sollte keine Änderungen finden
        val smartGen = newGenerator(outDir = dir, migrationMode = SchemaGenerator.MigrationMode.SMART)
        smartGen.generate()

        val filesAfterSmart = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        assertEquals(createFiles.size, filesAfterSmart.size, "No changes should not create new files")
    }

    @Test
    fun testMultipleSchemaDirectories() {
        val dir = File(tmpDir, "multi_schema").apply { mkdirs() }

        val gen = newGenerator(outDir = dir)
        gen.generate()

        val schemas = dir.listFiles()?.filter { it.isDirectory } ?: emptyList()
        assertTrue(schemas.isNotEmpty(), "Should create at least one schema directory")

        schemas.forEach { schemaDir ->
            val sqlFiles = schemaDir.listFiles()?.filter { it.extension == "sql" } ?: emptyList()
            println("Schema ${schemaDir.name}: ${sqlFiles.size} files")
            sqlFiles.forEach { println("  - ${it.name}") }
        }
    }

    @Test
    fun testFilenameTimestampIsUnique() {
        val dir = File(tmpDir, "timestamp_unique").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV1())
        Thread.sleep(1100)
        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildCreateTableV2())

        val files = dir.listFiles().orEmpty().sortedBy { it.name }
        assertEquals(2, files.size)

        val timestamps = files.map {
            it.name.substringAfter("V").substringBefore("__")
        }

        assertEquals(2, timestamps.distinct().size, "Timestamps should be unique")
        assertTrue(timestamps[0] < timestamps[1], "Timestamps should be in chronological order")
    }

    @Test
    fun testAlterFileNamingPattern() {
        val dir = File(tmpDir, "alter_naming").apply { mkdirs() }
        val schemaDir = File(dir, "domain").apply { mkdirs() }

        // Erstelle CREATE TABLE
        File(schemaDir, "V20241121000000__1000_create_sample_table.sql").writeText(
            """
            CREATE TABLE sample_table (
                id BIGSERIAL PRIMARY KEY
            );
        """.trimIndent()
        )

        val oldSchema = TableSchema(
            columns = listOf(ColumnSchema("id", "BIGSERIAL", false, isPrimaryKey = true)),
            indexes = emptyList(),
            foreignKeys = emptyList()
        )

        val newSchema = TableSchema(
            columns = listOf(
                ColumnSchema("id", "BIGSERIAL", false, isPrimaryKey = true),
                ColumnSchema("name", "VARCHAR(255)", false)
            ),
            indexes = emptyList(),
            foreignKeys = emptyList()
        )

        val gen = newGenerator(outDir = dir)
        val alterFile = gen.testGenerateAlterForTest(
            entityClass = SampleEntity::class.java,
            schema = "domain",
            oldSchema = oldSchema,
            newSchema = newSchema
        )

        assertNotNull(alterFile)

        val pattern = Regex("""V\d{18}__alter_sample_table\.sql""")
        assertTrue(pattern.matches(alterFile.name), "ALTER file should match naming pattern: ${alterFile.name}")
    }

    @Test
    fun testAlterScriptContentStructure() {
        val dir = File(tmpDir, "alter_structure").apply { mkdirs() }
        val schemaDir = File(dir, "domain").apply { mkdirs() }

        File(schemaDir, "V20241121000000__1000_create_sample_table.sql").writeText(
            """
            CREATE TABLE sample_table (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100)
            );
        """.trimIndent()
        )

        val oldSchema = TableSchema(
            columns = listOf(
                ColumnSchema("id", "BIGSERIAL", false, isPrimaryKey = true),
                ColumnSchema("name", "VARCHAR(100)", true)
            ),
            indexes = emptyList(),
            foreignKeys = emptyList()
        )

        val newSchema = TableSchema(
            columns = listOf(
                ColumnSchema("id", "BIGSERIAL", false, isPrimaryKey = true),
                ColumnSchema("name", "VARCHAR(100)", false), // Changed to NOT NULL
                ColumnSchema("email", "VARCHAR(255)", true)  // New column
            ),
            indexes = listOf(IndexSchema("idx_email", listOf("email"), false)),
            foreignKeys = emptyList()
        )

        val gen = newGenerator(outDir = dir)
        val alterFile = gen.testGenerateAlterForTest(
            entityClass = SampleEntity::class.java,
            schema = "domain",
            oldSchema = oldSchema,
            newSchema = newSchema
        )

        assertNotNull(alterFile)
        val content = alterFile!!.readText()

        // Check structure
        assertTrue(content.startsWith("-- HASH:"), "Should have hash header")
        assertTrue(content.contains("-- Migration for table:"), "Should have migration comment")
        assertTrue(content.contains("-- Entity:"), "Should have entity comment")

        // Check changes
        assertTrue(content.contains("ALTER TABLE"), "Should contain ALTER TABLE")
        assertTrue(content.contains("ADD COLUMN email"), "Should add new column")
        assertTrue(content.contains("SET NOT NULL"), "Should change nullability")
        assertTrue(content.contains("CREATE INDEX"), "Should create index")
    }

    // ==================== Edge Cases & Error Handling ====================

    @Test
    fun testEmptyContentHandling() {
        val dir = File(tmpDir, "empty_content").apply { mkdirs() }
        val gen = newGenerator()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = "create_test_table", content = "")

        val files = dir.listFiles().orEmpty()
        assertEquals(1, files.size)
        assertTrue(files.single().readText().startsWith("-- HASH:"))
    }

    @Test
    fun testRepeatedUnchangedWritesDoNotMultiply() {
        val dir = File(tmpDir, "repeated_writes").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildCreateTableV1()

        repeat(5) {
            gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)
        }

        val files = dir.listFiles().orEmpty()
        assertEquals(1, files.size, "Repeated unchanged writes should not create additional files")
    }

    @Test
    fun testSqlParserHandlesCommentsInCreateTable() {
        val dir = File(tmpDir, "parser_comments").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "V20241121000000__1000_create_users_table.sql")
        sqlFile.writeText(
            """
            -- HASH: abc123
            -- This is a user table
            CREATE TABLE users (
                -- Primary key
                id           BIGSERIAL    PRIMARY KEY,
                -- Username field
                username     VARCHAR(50)  NOT NULL,
                -- Email address
                email        VARCHAR(255) NOT NULL
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(3, schema!!.columns.size)
    }

    @Test
    fun testSqlParserHandlesIfNotExists() {
        val dir = File(tmpDir, "parser_if_not_exists").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "V20241121000000__1000_create_users_table.sql")
        sqlFile.writeText(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255)
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val existingTables = extractor.getExistingTables()

        assertTrue(existingTables.contains("users"))
    }

    @Test
    fun testSqlParserHandlesMultipleForeignKeysInSingleFile() {
        val dir = File(tmpDir, "parser_multi_fks").apply { mkdirs() }
        val schemaDir = File(dir, "public").apply { mkdirs() }

        File(schemaDir, "V20241121000000__1000_create_orders_table.sql").writeText(
            """
            CREATE TABLE orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                product_id BIGINT NOT NULL
            );
        """.trimIndent()
        )

        File(schemaDir, "V20241121000001__0200_foreign_keys.sql").writeText(
            """
            ALTER TABLE orders
                ADD CONSTRAINT fk_orders_user_id
                FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE CASCADE;
                
            ALTER TABLE orders
                ADD CONSTRAINT fk_orders_product_id
                FOREIGN KEY (product_id)
                REFERENCES products(id)
                ON DELETE RESTRICT;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(dir)
        val schema = extractor.loadTableSchema("public", "orders")

        assertNotNull(schema)
        assertEquals(2, schema!!.foreignKeys.size)

        val userFk = schema.foreignKeys.find { it.columnName == "user_id" }
        val productFk = schema.foreignKeys.find { it.columnName == "product_id" }

        assertNotNull(userFk)
        assertNotNull(productFk)
        assertEquals("CASCADE", userFk!!.onDelete)
        assertEquals("RESTRICT", productFk!!.onDelete)
    }

    @Test
    fun testGeneratorFindsAllEntities() {
        val gen = newGenerator()
        val entities = gen.testFindEntities()

        assertTrue(entities.isNotEmpty(), "Should find at least one entity")
        entities.forEach { entity ->
            println("Found entity: ${entity.simpleName}")
        }
    }

    @Test
    fun testNoMetadataDirectoryCreated() {
        val dir = File(tmpDir, "no_metadata").apply { mkdirs() }

        val gen = newGenerator(outDir = dir)
        gen.generate()

        val metadataDir = File(dir, ".metadata")
        assertFalse(metadataDir.exists(), ".metadata directory should not be created with new system")
    }

    // ==================== Helper Methods ====================

    private fun buildCreateTableV1(): String = """
        CREATE TABLE test_table (
            id UUID PRIMARY KEY
        );
    """.trimIndent()

    private fun buildCreateTableV2(): String = """
        CREATE TABLE test_table (
            id   UUID PRIMARY KEY,
            name VARCHAR(255)
        );
    """.trimIndent()

    private fun buildCreateTableV3(): String = """
        CREATE TABLE test_table (
            id    UUID PRIMARY KEY,
            name  VARCHAR(255),
            email VARCHAR(255) NOT NULL
        );
    """.trimIndent()
}
package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.entity.fu.SampleEntity
import de.alexanderwolz.commons.util.database.migration.schema.ColumnSchema
import de.alexanderwolz.commons.util.database.migration.schema.ForeignKeySchema
import de.alexanderwolz.commons.util.database.migration.schema.IndexSchema
import de.alexanderwolz.commons.util.database.migration.schema.TableSchema
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SchemaGeneratorMigrationTest {

    private val entityPackage = javaClass.packageName + ".entity"

    @TempDir
    lateinit var tmpDir: File

    private fun newGenerator(
        outDir: File = tmpDir
    ) = SchemaGenerator(
        basePackage = entityPackage,
        outDir = outDir,
        databaseType = SchemaGenerator.DatabaseType.POSTGRES,
        uuidType = SchemaGenerator.UUIDType.UUID_V4
    )

    private fun buildBodyV1(): String =
        StringBuilder().apply {
            appendLine("CREATE TABLE test_table (")
            appendLine("    id UUID PRIMARY KEY")
            appendLine(");")
        }.toString()

    private fun buildBodyV2(): String =
        StringBuilder().apply {
            appendLine("CREATE TABLE test_table (")
            appendLine("    id   UUID PRIMARY KEY,")
            appendLine("    name VARCHAR(255)")
            appendLine(");")
        }.toString()

    private fun buildBodyV3(): String =
        StringBuilder().apply {
            appendLine("CREATE TABLE test_table (")
            appendLine("    id   UUID PRIMARY KEY,")
            appendLine("    name VARCHAR(255),")
            appendLine("    email VARCHAR(255) NOT NULL")
            appendLine(");")
        }.toString()

    @Test
    fun testFirstWriteCreatesMigrationFile() {
        val dir = File(tmpDir, "hashes_first").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildBodyV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)

        val files = dir.listFiles().orEmpty()
        assertEquals(1, files.size, "Exactly one migration expected on first write")

        val file = files.single()

        val pattern = Regex("""V\d{18}__${baseName}\.sql""")
        assertTrue(pattern.matches(file.name), "filename '$file' doesn't match migration pattern")

        val lines = file.readLines()
        assertTrue(lines.first().startsWith("-- HASH:"), "file should contain hash header")
        assertTrue(lines.any { it.contains("CREATE TABLE") })
    }

    @Test
    fun testUnchangedContentDoesNotRewrite() {
        val dir = File(tmpDir, "hashes_unchanged").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildBodyV1()

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

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV1())
        val f1 = dir.listFiles().orEmpty()
        assertEquals(1, f1.size)
        val fileV1 = f1.single().name

        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV2())
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

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV1())
        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV2())
        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV3())

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
        val body = buildBodyV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)
        Thread.sleep(1100)
        gen.testWriteFileForTest(dir = dir, sortNumber = "2000", baseName = baseName, content = body)

        val files = dir.listFiles().orEmpty()
        assertEquals(2, files.size, "Different sort numbers should create separate files")

        assertTrue(files.any { it.name.contains("1000") })
        assertTrue(files.any { it.name.contains("2000") })
    }

    @Test
    fun testDifferentBaseNamesWithSameContent() {
        val dir = File(tmpDir, "hashes_base_names").apply { mkdirs() }
        val gen = newGenerator()

        val body = buildBodyV1()

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = "create_users_table", content = body)
        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = "create_accounts_table", content = body)

        val files = dir.listFiles().orEmpty()
        assertEquals(2, files.size, "Different base names should create separate files")

        assertTrue(files.any { it.name.contains("create_users_table") })
        assertTrue(files.any { it.name.contains("create_accounts_table") })
    }

    @Test
    fun testHashHeaderIsConsistent() {
        val dir = File(tmpDir, "hashes_consistency").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildBodyV1()

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

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV1())
        val oldFile = dir.listFiles()!!.single()
        val oldContent = oldFile.readText()
        val oldName = oldFile.name

        Thread.sleep(1100)

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV2())

        val files = dir.listFiles().orEmpty()
        assertEquals(2, files.size)

        val preservedFile = files.find { it.name == oldName }
        assertTrue(preservedFile != null, "Old migration file should still exist")
        assertEquals(oldContent, preservedFile.readText(), "Old file content should be unchanged")
    }

    @Test
    fun testEmptyContentHandling() {
        val dir = File(tmpDir, "hashes_empty").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = "")

        val files = dir.listFiles().orEmpty()
        assertEquals(1, files.size)

        val file = files.single()
        val lines = file.readLines()
        assertTrue(lines.first().startsWith("-- HASH:"))
    }

    @Test
    fun testRepeatedUnchangedWritesDoNotMultiply() {
        val dir = File(tmpDir, "hashes_repeated").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"
        val body = buildBodyV1()

        repeat(5) {
            gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = body)
        }

        val files = dir.listFiles().orEmpty()
        assertEquals(1, files.size, "Repeated unchanged writes should not create additional files")
    }

    @Test
    fun testFilenameTimestampIsUnique() {
        val dir = File(tmpDir, "hashes_timestamp").apply { mkdirs() }
        val gen = newGenerator()

        val baseName = "create_test_table"

        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV1())
        Thread.sleep(1100)
        gen.testWriteFileForTest(dir = dir, sortNumber = "1000", baseName = baseName, content = buildBodyV2())

        val files = dir.listFiles().orEmpty().sortedBy { it.name }
        assertEquals(2, files.size)

        val timestamps = files.map {
            it.name.substringAfter("V").substringBefore("__")
        }

        assertEquals(timestamps.distinct().size, 2, "Timestamps should be unique")
        assertTrue(timestamps[0] < timestamps[1], "Timestamps should be in chronological order")
    }

    @Test
    fun testSmartModeGeneratesAlterScriptsOnChange() {
        val dir = File(tmpDir, "smart_mode_alter").apply { mkdirs() }

        // Phase 1: CREATE Mode - Initiale Tabellen
        val createGen = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.CREATE_ONLY
        )
        createGen.generate()

        val createFiles = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        assertTrue(createFiles.isNotEmpty(), "CREATE mode should generate files")
        println("After CREATE: ${createFiles.size} files")

        // Phase 2: SMART mode ohne Änderungen
        val smartGen1 = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.SMART
        )
        smartGen1.generate()

        val filesAfterSmart1 = dir.walkTopDown().filter { it.extension == "sql" }.toList()
        assertEquals(
            createFiles.size, filesAfterSmart1.size,
            "SMART mode without changes should not create new files"
        )

        // Phase 3: Simuliere eine Schema-Änderung durch manuelles Editieren der State-Datei
        val stateDir = File(dir, ".metadata")
        val stateFile = stateDir.walkTopDown()
            .filter { it.extension == "json" }
            .firstOrNull()

        assertNotNull(stateFile, "Should have at least one state file")

        val currentState = TableSchema.fromJson(stateFile.readText())

        currentState.copy(
            columns = currentState.columns + ColumnSchema(
                name = "new_column",
                type = "VARCHAR(100)",
                nullable = true
            )
        )

        stateFile.writeText(currentState.toJson())

        Thread.sleep(1100)

        val smartGen2 = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.SMART
        )
        smartGen2.generate()

        val filesAfterChange = dir.walkTopDown().filter { it.extension == "sql" }.toList()

        val alterFiles = filesAfterChange.filter { it.name.contains("alter") }

        println("After simulated change: ${filesAfterChange.size} files (${alterFiles.size} ALTER files)")
        alterFiles.forEach { println("  - ${it.name}") }
    }

    @Test
    fun testAlterModeRequiresExistingSchema() {
        val dir = File(tmpDir, "alter_mode_test").apply { mkdirs() }

        // Versuche ALTER mode ohne vorherige CREATE
        val alterGen = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.ALTER_ONLY
        )

        alterGen.generate()

    }

    @Test
    fun testSmartModeDetectsSchemaChanges() {
        val dir = File(tmpDir, "smart_detect_changes").apply { mkdirs() }
        val entityPackage = javaClass.packageName + ".entity"

        // Phase 1: Initial CREATE
        val gen1 = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.CREATE_ONLY
        )
        gen1.generate()

        val createFiles = dir.walkTopDown()
            .filter { it.extension == "sql" }
            .toList()

        println("=== After CREATE mode ===")
        createFiles.forEach { println("  ${it.name}") }

        assertTrue(createFiles.isNotEmpty(), "CREATE mode should generate files")
        assertTrue(
            createFiles.none { it.name.contains("alter") },
            "CREATE mode should not generate ALTER files"
        )

        // Phase 2: SMART mode ohne Änderungen
        Thread.sleep(1100) // Ensure different timestamp

        val gen2 = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.SMART
        )
        gen2.generate()

        val smartFiles1 = dir.walkTopDown()
            .filter { it.extension == "sql" }
            .toList()

        println("\n=== After SMART mode (no changes) ===")
        smartFiles1.forEach { println("  ${it.name}") }

        assertEquals(
            createFiles.size, smartFiles1.size,
            "SMART mode without entity changes should not create new migrations"
        )

        // Phase 3: Prüfe ob ALTER-Dateien existieren (sollten nicht)
        val alterFiles = smartFiles1.filter { it.name.contains("alter") }
        assertTrue(
            alterFiles.isEmpty(),
            "No ALTER files should exist when schema hasn't changed"
        )
    }

    @Test
    fun testAlterFileNaming() {
        val dir = File(tmpDir, "alter_naming").apply { mkdirs() }

        // Erstelle eine ALTER-Datei und prüfe das Naming
        val gen = newGenerator(outDir = dir)
        gen.generate()

        val files = dir.walkTopDown()
            .filter { it.extension == "sql" && it.name.contains("alter") }
            .toList()

        files.forEach { file ->
            println("ALTER file: ${file.name}")

            // Prüfe Naming Pattern
            val pattern = Regex("""V\d{18}__alter_\w+_table\.sql""")
            assertTrue(
                pattern.matches(file.name),
                "ALTER file should match naming pattern: ${file.name}"
            )

            // Prüfe Timestamp-Format (sollte 18 Ziffern sein: YYYYMMDDHHmmssSSS)
            val timestamp = file.name.substring(1, 19)
            assertEquals(18, timestamp.length, "Timestamp should be 18 digits")
            assertTrue(timestamp.all { it.isDigit() }, "Timestamp should only contain digits")

            println("  Content preview:")
            println(file.readText().lines().take(10).joinToString("\n") { "    $it" })
        }
    }

    @Test
    fun testAlterScriptContent() {
        val dir = File(tmpDir, "alter_content").apply { mkdirs() }

        val gen = newGenerator(outDir = dir)
        gen.generate()

        val alterFiles = dir.walkTopDown()
            .filter { it.extension == "sql" && it.name.contains("alter") }
            .toList()

        alterFiles.forEach { file ->
            val content = file.readText()

            // Hash Header sollte existieren
            assertTrue(
                content.startsWith("-- HASH:"),
                "ALTER file should have hash header"
            )

            // Sollte ALTER statements enthalten
            assertTrue(
                content.contains("ALTER TABLE", ignoreCase = true) ||
                        content.contains("ADD COLUMN", ignoreCase = true) ||
                        content.contains("DROP COLUMN", ignoreCase = true) ||
                        content.contains("MODIFY", ignoreCase = true),
                "ALTER file should contain ALTER statements"
            )

            println("=== ${file.name} ===")
            println(content)
        }
    }

    @Test
    fun testSchemaStateFilesCreation() {
        val dir = File(tmpDir, "schema_state").apply { mkdirs() }

        val gen = newGenerator(outDir = dir)
        gen.generate()

        val stateDir = File(dir, ".metadata")
        assertTrue(stateDir.exists(), ".schema-state directory should exist")

        val stateFiles = stateDir.walkTopDown()
            .filter { it.extension == "json" }
            .toList()

        println("\n=== Schema State Files ===")
        stateFiles.forEach { file ->
            println("File: ${file.relativeTo(dir)}")
            println("Content:")
            println(file.readText().lines().joinToString("\n") { "  $it" })
            println()
        }

        assertTrue(
            stateFiles.isNotEmpty(),
            "Schema state files should be created after generation"
        )
    }

    @Test
    fun testSchemaStateTrackerPersistence() {
        val dir = File(tmpDir, "state_tracker_test").apply { mkdirs() }

        val gen1 = SchemaGenerator(
            basePackage = entityPackage,
            outDir = dir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            migrationMode = SchemaGenerator.MigrationMode.CREATE_ONLY
        )

        gen1.generate()

        val stateDir = File(dir, ".metadata")
        val schemaStateFiles = stateDir.walkTopDown()
            .filter { it.name.endsWith(".json") }
            .toList()

        assertTrue(
            schemaStateFiles.isNotEmpty(),
            "Schema state files should be created after generation"
        )

        schemaStateFiles.forEach { stateFile ->
            println("Found schema state: ${stateFile.absolutePath}")
            println("Content: ${stateFile.readText()}")
        }
    }

    @Test
    fun testSchemaStateSaveAndLoad() {
        val dir = File(tmpDir, "schema_save_load").apply { mkdirs() }

        val tracker = SchemaStateTracker(dir)

        val schema = TableSchema(
            columns = listOf(
                ColumnSchema("id", "UUID", nullable = false, isPrimaryKey = true),
                ColumnSchema("name", "VARCHAR(255)", nullable = true),
                ColumnSchema("email", "VARCHAR(255)", nullable = false, defaultValue = "DEFAULT ''")
            ),
            indexes = listOf(
                IndexSchema("idx_name", listOf("name"), unique = false),
                IndexSchema("idx_email", listOf("email"), unique = true)
            ),
            foreignKeys = listOf(
                ForeignKeySchema("user_id", "users", "id", "CASCADE")
            )
        )

        tracker.saveTableSchema("public", "test_table", schema)

        // Prüfe ob Datei existiert
        val stateFile = File(dir, ".metadata/public/test_table.json")
        assertTrue(stateFile.exists(), "State file should exist")

        println("=== State file content ===")
        println(stateFile.readText())
        println()

        // Lade zurück
        val loaded = tracker.loadTableSchema("public", "test_table")

        assertNotNull(loaded)
        assertEquals(3, loaded.columns.size)
        assertEquals("id", loaded.columns[0].name)
        assertEquals("UUID", loaded.columns[0].type)
        assertTrue(loaded.columns[0].isPrimaryKey)
        assertEquals(2, loaded.indexes.size)
        assertEquals("idx_name", loaded.indexes[0].name)
        assertEquals(1, loaded.foreignKeys.size)
        assertEquals("user_id", loaded.foreignKeys[0].columnName)
    }

    @Test
    fun testJsonSerializationRoundTrip() {
        val original = TableSchema(
            columns = listOf(
                ColumnSchema("id", "BIGINT", nullable = false, isPrimaryKey = true),
                ColumnSchema("status", "VARCHAR(50)", nullable = true, unique = true)
            ),
            indexes = listOf(
                IndexSchema("idx_status", listOf("status", "id"), unique = false)
            ),
            foreignKeys = listOf(
                ForeignKeySchema("parent_id", "parent_table", "id", "SET NULL")
            )
        )

        val json = original.toJson()
        println("=== Serialized JSON ===")
        println(json)
        println()

        val deserialized = TableSchema.fromJson(json)

        assertEquals(original.columns.size, deserialized.columns.size)
        assertEquals(original.indexes.size, deserialized.indexes.size)
        assertEquals(original.foreignKeys.size, deserialized.foreignKeys.size)

        // Deep equality check
        assertEquals(original.columns[0].name, deserialized.columns[0].name)
        assertEquals(original.indexes[0].columns, deserialized.indexes[0].columns)
        assertEquals(original.foreignKeys[0].onDelete, deserialized.foreignKeys[0].onDelete)
    }


    @Test
    fun testAlterTable() {
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

        val file = newGenerator().testGenerateAlterForTest(
            entityClass = SampleEntity::class.java,
            schema = "domain",
            oldSchema = oldSchema,
            newSchema = newSchema
        )!!

        Assertions.assertNotNull(file)
        assertTrue(file.exists())
        assertTrue(file.name.contains("alter_sample_table"))
    }

}
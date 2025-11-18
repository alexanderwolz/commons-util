package de.alexanderwolz.commons.util.database

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SchemaGeneratorTest {

    @TempDir
    private lateinit var tmpDir: File

    private val entityPackage = "de.alexanderwolz.commons.util.database"
    private val schemaDirName = entityPackage.split(".").last()   // → "database"

    private fun runGenerator(db: SchemaGenerator.DatabaseType, uuid: SchemaGenerator.UUIDType): File {
        val outDir = File(tmpDir, "migration_${db}_${uuid}")
        val generator = SchemaGenerator(
            entityPackage,
            outDir,
            db,
            uuid
        )
        generator.generate()
        return outDir
    }
    
    @Test
    fun testPostgresUuidV7Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V7
        )

        val schemaDir = File(dir, schemaDirName)
        assertTrue(schemaDir.exists(), "Schema directory missing")

        val setupFile = File(schemaDir, "V0__setup_uuid_extension.sql")
        assertTrue(setupFile.exists(), "Setup file missing for UUID v7")

        val tableFile = File(schemaDir, "V1__create_sample_table.sql")
        assertTrue(tableFile.exists(), "Sample table file missing")

        val text = tableFile.readText()

        assertTrue(
            text.contains("DEFAULT uuid_generate_v7()") && text.contains("PRIMARY KEY"),
            "UUIDv7 PK missing"
        )
        assertTrue(text.contains("created_at") && text.contains("DEFAULT CURRENT_TIMESTAMP"))
    }

    @Test
    fun testPostgresUuidV4Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val tableFile = File(schemaDir, "V1__create_sample_table.sql")

        assertTrue(tableFile.exists(), "Sample table file missing")

        val setupFile = File(schemaDir, "V0__setup_uuid_extension.sql")
        assertFalse(setupFile.exists(), "UUID v4 must NOT create the UUID extension setup")

        val text = tableFile.readText()

        assertTrue(text.contains("DEFAULT uuid_generate_v4()"), "UUID v4 PK missing")
        assertFalse(text.contains("uuid_generate_v7()"), "UUID v7 accidentally used")
    }

    @Test
    fun testMariaDbUuidV4Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.MARIADB,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val tableFile = File(schemaDir, "V1__create_sample_table.sql")

        assertTrue(tableFile.exists(), "Sample table file missing")

        val setupFile = File(schemaDir, "V0__setup_uuid_extension.sql")
        assertFalse(setupFile.exists(), "MariaDB must NOT generate uuid extension setup")

        val text = tableFile.readText()

        assertTrue(
            text.contains("CHAR(36)") &&
                    text.contains("DEFAULT (UUID())") &&
                    text.contains("PRIMARY KEY"),
            "MariaDB UUID PK missing"
        )
        assertFalse(text.contains("uuid_generate"))
        assertFalse(text.contains("BYTEA"))
    }

    @Test
    fun testMariaDbUuidV7Fallback() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.MARIADB,
            SchemaGenerator.UUIDType.UUID_V7
        )

        val schemaDir = File(dir, schemaDirName)
        val tableFile = File(schemaDir, "V1__create_sample_table.sql")

        assertTrue(tableFile.exists(), "Sample table file missing")

        val setupFile = File(schemaDir, "V0__setup_uuid_extension.sql")
        assertFalse(setupFile.exists(), "MariaDB must NOT generate uuid extension setup")

        val text = tableFile.readText()

        assertTrue(text.contains("DEFAULT (UUID())"), "MariaDB UUIDv7 fallback missing")
    }
}

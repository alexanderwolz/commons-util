package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.entity.SampleEntity
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.*

class SchemaGeneratorTest {

    @TempDir
    private lateinit var tmpDir: File

    private val entityPackage = SampleEntity::class.java.packageName
    private val schemaDirName = entityPackage.split(".").last()

    private fun runGenerator(
        db: SchemaGenerator.DatabaseType,
        uuid: SchemaGenerator.UUIDType
    ): File {
        val outDir = File(tmpDir, "migration_${db}_${uuid}")
        SchemaGenerator(entityPackage, outDir, db, uuid).generate()
        return outDir
    }

    private fun File.findFileEndingWith(suffix: String): File =
        this.listFiles()?.firstOrNull { it.name.endsWith(suffix) }
            ?: error("No file found with suffix: $suffix in dir: ${this.listFiles()?.toList()}")

    @Test
    fun testPostgresUuidV7Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V7
        )

        val schemaDir = File(dir, schemaDirName)
        assertTrue(schemaDir.exists())

        val setup = schemaDir.findFileEndingWith("__setup_uuid_extension.sql")
        val content = setup.readText()

        assertTrue(content.contains("pgcrypto"))
        assertTrue(content.contains("uuid_generate_v7"))

        val tableFile = schemaDir.findFileEndingWith("__create_sample_table.sql")
        val text = tableFile.readText()

        assertTrue(text.contains("DEFAULT public.uuid_generate_v7()"))
        assertTrue(text.contains("PRIMARY KEY"))
        assertTrue(text.contains("created_at") && text.contains("DEFAULT CURRENT_TIMESTAMP"))
    }

    @Test
    fun testPostgresUuidV4Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)

        // V4 darf KEIN Setup erzeugen
        assertNull(
            schemaDir.listFiles()?.firstOrNull { it.name.contains("__setup_uuid_extension") },
            "UUIDv4 must NOT generate setup file"
        )

        val tableFile = schemaDir.findFileEndingWith("__create_sample_table.sql")
        val text = tableFile.readText()

        assertTrue(text.contains("DEFAULT public.uuid_generate_v4()"))
        assertFalse(text.contains("uuid_generate_v7()"))
    }

    @Test
    fun testMariaDbUuidV4Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.MARIADB,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val tableFile = schemaDir.findFileEndingWith("__create_sample_table.sql")
        val text = tableFile.readText()

        assertTrue(text.contains("CHAR(36)"))
        assertTrue(text.contains("DEFAULT (UUID())"))
        assertFalse(text.contains("uuid_generate"))
    }

    @Test
    fun testMariaDbUuidV7Fallback() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.MARIADB,
            SchemaGenerator.UUIDType.UUID_V7
        )

        val schemaDir = File(dir, schemaDirName)
        val tableFile = schemaDir.findFileEndingWith("__create_sample_table.sql")
        val text = tableFile.readText()

        assertTrue(text.contains("DEFAULT (UUID())"))
        assertFalse(text.contains("uuid_generate"))
    }

    @Test
    fun testEntityDiscoveryWorks() {
        val gen = SchemaGenerator(entityPackage, tmpDir)
        val method = gen.javaClass.getDeclaredMethod("findEntities").apply { isAccessible = true }
        val entities = method.invoke(gen) as List<*>

        assertTrue(entities.any { (it as Class<*>).simpleName == "SampleEntity" })
        assertFalse(entities.any { (it as Class<*>).simpleName.contains("Test") })
    }

    @Test
    fun testFkOnDeleteCascadeAndSetNull() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val sql = schemaDir.findFileEndingWith("__add_foreign_keys.sql").readText()

        assertTrue(sql.contains("ON DELETE CASCADE"))
        assertTrue(sql.contains("ON DELETE SET NULL"))
    }

    @Test
    fun testIndexGeneration() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val sql = schemaDir.findFileEndingWith("__add_indexes.sql").readText()

        assertTrue(sql.contains("idx_sample_reference_id"))
        assertTrue(sql.contains("idx_sample_email"))
        assertTrue(sql.contains("CREATE INDEX"))
    }

    @Test
    fun testEmbeddedFields() {
        val schemaDir = File(
            runGenerator(
                SchemaGenerator.DatabaseType.POSTGRES,
                SchemaGenerator.UUIDType.UUID_V4
            ),
            schemaDirName
        )

        val text = schemaDir.findFileEndingWith("__create_sample_table.sql").readText()

        assertTrue(text.contains("address_street"))
        assertTrue(text.contains("address_city"))
    }

    @Test
    fun testTimestampDefaultApplied() {
        val schemaDir = File(
            runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4),
            schemaDirName
        )

        val text = schemaDir.findFileEndingWith("__create_sample_table.sql").readText()

        assertTrue(text.contains("created_at") && text.contains("DEFAULT CURRENT_TIMESTAMP"))
        assertTrue(text.contains("updated_at") && text.contains("DEFAULT CURRENT_TIMESTAMP"))
    }

    @Test
    fun testEnumAndJsonTypes() {
        val schemaDir = File(
            runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4),
            schemaDirName
        )

        val text = schemaDir.findFileEndingWith("__create_sample_table.sql").readText()

        assertTrue(
            Regex("""\b\w*_status\s+VARCHAR\(50\)""").containsMatchIn(text),
            "Enum mapping missing"
        )

        assertTrue(text.contains("JSONB"))
    }

    @Test
    fun testMigrationFileOrder() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val files = schemaDir.listFiles()?.sortedBy { it.name } ?: emptyList()

        assertTrue(files.isNotEmpty())
        assertTrue(files.first().name.startsWith("V"))
        assertTrue(files.last().name.startsWith("V"))
    }
}

package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.entity.SampleEntity
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertFalse
import kotlin.test.assertTrue

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
        val generator = SchemaGenerator(entityPackage, outDir, db, uuid)
        generator.generate()
        return outDir
        //.also {
        //File(it, schemaDirName).listFiles()?.forEach { file -> println("\n#####\n${file.readText()}\n#####\n") }
        //}
    }

    @Test
    fun testPostgresUuidV7Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V7
        )

        val schemaDir = File(dir, schemaDirName)
        assertTrue(schemaDir.exists())

        val setupFile = File(schemaDir, "V0__setup_uuid_extension.sql")
        assertTrue(setupFile.exists(), "Setup file missing for UUID v7")

        val content = setupFile.readText()
        assertTrue(content.contains("pgcrypto"))
        assertTrue(content.contains("uuid_generate_v7"))

        val tableFile = File(schemaDir, "V1__create_sample_table.sql")
        val text = tableFile.readText()
        assertTrue(text.contains("DEFAULT uuid_generate_v7()"))
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
        val setupFile = File(schemaDir, "V0__setup_uuid_extension.sql")
        assertFalse(setupFile.exists(), "UUIDv4 must not generate V0")

        val text = File(schemaDir, "V1__create_sample_table.sql").readText()
        assertTrue(text.contains("DEFAULT uuid_generate_v4()"))
        assertFalse(text.contains("uuid_generate_v7()"))
    }

    @Test
    fun testMariaDbUuidV4Generation() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.MARIADB,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val text = File(schemaDir, "V1__create_sample_table.sql").readText()

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
        val text = File(schemaDir, "V1__create_sample_table.sql").readText()

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

        val sql = File(File(dir, schemaDirName), "V3__add_foreign_keys.sql").readText()

        assertTrue(sql.contains("ON DELETE CASCADE"), "Expected a CASCADE")
        assertTrue(sql.contains("ON DELETE SET NULL"), "Expected a SET NULL")
    }

    @Test
    fun testIndexGeneration() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val sql = File(File(dir, schemaDirName), "V4__add_indexes.sql").readText()

        assertTrue(sql.contains("idx_sample_reference_id"))
        assertTrue(sql.contains("idx_sample_email"))
        assertTrue(sql.contains("CREATE INDEX"))
    }

    @Test
    fun testEmbeddedFields() {
        val text = File(
            File(
                runGenerator(
                    SchemaGenerator.DatabaseType.POSTGRES,
                    SchemaGenerator.UUIDType.UUID_V4
                ),
                schemaDirName
            ),
            "V1__create_sample_table.sql"
        ).readText()

        assertTrue(text.contains("address_street"))
        assertTrue(text.contains("address_city"))
    }

    @Test
    fun testTimestampDefaultApplied() {
        val text = File(
            File(
                runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4),
                schemaDirName
            ),
            "V1__create_sample_table.sql"
        ).readText()

        assertTrue(text.contains("created_at") && text.contains("DEFAULT CURRENT_TIMESTAMP"))
        assertTrue(text.contains("updated_at") && text.contains("DEFAULT CURRENT_TIMESTAMP"))
    }

    @Test
    fun testEnumAndJsonTypes() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val tableFile = File(schemaDir, "V1__create_sample_table.sql")

        val text = tableFile.readText()

        // ENUM: find any column whose name ends with "_status"
        assertTrue(
            Regex("""\b\w*_status\s+VARCHAR\(50\)""").containsMatchIn(text),
            "Enum mapping missing: expected *_status VARCHAR(50)"
        )

        assertTrue(text.contains("JSONB"), "JsonNode → JSONB missing")
    }

    @Test
    fun testMigrationFileOrder() {
        val dir = runGenerator(
            SchemaGenerator.DatabaseType.POSTGRES,
            SchemaGenerator.UUIDType.UUID_V4
        )

        val schemaDir = File(dir, schemaDirName)
        val files = schemaDir.listFiles()?.sortedBy { it.name }?.map { it.name } ?: emptyList()

        // V0 must exist only if UUID_V7
        val expectsV0 = false
        if (expectsV0) {
            assertTrue(files.first().startsWith("V0"))
        } else {
            assertFalse(files.first().startsWith("V0"))
        }

        assertTrue(files[0].startsWith("V1"))
        assertTrue(files.last().startsWith("V"), "File ordering broken")
    }
}

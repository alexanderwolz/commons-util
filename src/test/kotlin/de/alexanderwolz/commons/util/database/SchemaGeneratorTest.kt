package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.entity.bar.AnotherEntity
import de.alexanderwolz.commons.util.database.entity.fu.SampleEntity
import de.alexanderwolz.commons.util.database.entity.ReferenceEntity
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.*

class SchemaGeneratorTest {

    @TempDir
    private lateinit var tmpDir: File

    private val entityPackage = javaClass.packageName + ".entity"


    @Test
    fun testPostgresUuidV7Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V7)

        val setup = dir.partitionDirFor(ReferenceEntity::class.java)
            .requireFileEndingWith("__setup_uuid_extension.sql")
        assertTrue(setup.readText().contains("uuid_generate_v7"))

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(sampleSql.contains("DEFAULT public.uuid_generate_v7()"))
        assertTrue(sampleSql.contains("created_at") && sampleSql.contains("DEFAULT CURRENT_TIMESTAMP"))
    }

    @Test
    fun testPostgresUuidV4Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        // No setup file anywhere
        val anySetup = dir.walkTopDown().any { it.name.contains("__setup_uuid_extension") }
        assertFalse(anySetup, "UUID V4 must not generate setup file")

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(sampleSql.contains("DEFAULT public.uuid_generate_v4()"))
        assertFalse(sampleSql.contains("uuid_generate_v7()"))
    }

    @Test
    fun testMariaDbUuidV4Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.MARIADB, SchemaGenerator.UUIDType.UUID_V4)

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(sampleSql.contains("CHAR(36)"))
        assertTrue(sampleSql.contains("DEFAULT (UUID())"))
        assertFalse(sampleSql.contains("uuid_generate"))
    }

    @Test
    fun testMariaDbUuidV7Fallback() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.MARIADB, SchemaGenerator.UUIDType.UUID_V7)

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(sampleSql.contains("DEFAULT (UUID())"))
        assertFalse(sampleSql.contains("uuid_generate"))
    }

    @Test
    fun testEntityDiscoveryWorks() {
        val gen = SchemaGenerator(entityPackage, tmpDir)
        val entities = gen.findEntities()

        assertTrue(entities.any { it.simpleName == "SampleEntity" })
        assertTrue(entities.any { it.simpleName == "AnotherEntity" })
        assertFalse(entities.any { it.simpleName.contains("Test") })
    }

    @Test
    fun testFkOnDeleteCascadeAndSetNull() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        val fkSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__add_foreign_keys.sql")
            .readText()

        assertTrue(fkSql.contains("ON DELETE CASCADE"))
        assertTrue(fkSql.contains("ON DELETE SET NULL"))
    }

    @Test
    fun testIndexGeneration() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        val idxSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__add_indexes.sql")
            .readText()

        assertTrue(idxSql.contains("idx_sample_reference_id"))
        assertTrue(idxSql.contains("idx_sample_email"))
        assertTrue(idxSql.contains("CREATE INDEX"))
    }

    @Test
    fun testEmbeddedFields() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(sampleSql.contains("address_street"))
        assertTrue(sampleSql.contains("address_city"))
    }

    @Test
    fun testTimestampDefaultApplied() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(sampleSql.contains("created_at") && sampleSql.contains("DEFAULT CURRENT_TIMESTAMP"))
        assertTrue(sampleSql.contains("updated_at") && sampleSql.contains("DEFAULT CURRENT_TIMESTAMP"))
    }

    @Test
    fun testEnumAndJsonTypes() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        val sampleSql = dir.partitionDirFor(SampleEntity::class.java)
            .requireFileEndingWith("__create_sample_table.sql")
            .readText()

        assertTrue(
            Regex("""\b\w*_status\s+VARCHAR\(50\)""").containsMatchIn(sampleSql)
        )

        assertTrue(sampleSql.contains("JSONB"))
    }

    @Test
    fun testMigrationFileOrder() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)

        val allFiles = dir.walkTopDown().filter { it.isFile }.map { it.name }.sorted().toList()

        assertTrue(allFiles.isNotEmpty())
        assertTrue(allFiles.first().startsWith("V"))
        assertTrue(allFiles.last().startsWith("V"))
    }

    @Test
    fun testEntitiesAreGrouped() {
        val strategy = PackageNamePartitionStrategy()

        val grouped = SchemaGenerator(
            basePackage = entityPackage,
            outDir = tmpDir,
            databaseType = SchemaGenerator.DatabaseType.POSTGRES,
            uuidType = SchemaGenerator.UUIDType.UUID_V4,
            partitionStrategy = strategy
        ).groupByPartition(listOf(SampleEntity::class.java, AnotherEntity::class.java))

        assertTrue(grouped.containsKey("fu"))
        assertTrue(grouped.containsKey("bar"))

        assertTrue(grouped["fu"]!!.contains(SampleEntity::class.java))
        assertTrue(grouped["bar"]!!.contains(AnotherEntity::class.java))
    }


    private fun File.partitionDirFor(entity: Class<*>): File {
        val folder = entity.packageName.split(".").last()
        return File(this, folder)
    }

    private fun File.requireFileEndingWith(suffix: String): File {
        val match = this.listFiles()?.firstOrNull { it.name.endsWith(suffix) }
        return match ?: error(
            "No file found ending with '$suffix'\n" +
                    "In directory: $this\n" +
                    "Files: ${this.listFiles()?.joinToString()}"
        )
    }

    private fun runGenerator(
        db: SchemaGenerator.DatabaseType,
        uuid: SchemaGenerator.UUIDType
    ): File {
        val outDir = File(tmpDir, "migration_${db}_${uuid}")
        SchemaGenerator(entityPackage, outDir, db, uuid).generate()
        return outDir
    }
}

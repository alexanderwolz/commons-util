package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.entity.bar.AnotherEntity
import de.alexanderwolz.commons.util.database.entity.fu.SampleEntity
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SchemaGeneratorTest {

    @TempDir
    private lateinit var tmpDir: File

    private val entityPackage = javaClass.packageName + ".entity"

    @Test
    fun testPostgresUuidV7Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V7)

        val folder = dir.folderFor(SampleEntity::class.java)

        val setup = folder.findBySuffix("__setup_uuid_extension.sql")
        val create = folder.findBySuffix("__create_sample_table.sql")

        assertTrue(setup.readText().contains("uuid_generate_v7"))
        assertTrue(create.readText().startsWith("-- HASH:"))
        assertTrue(create.readText().contains("DEFAULT public.uuid_generate_v7()"))
    }

    @Test
    fun testPostgresUuidV4Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        // Kein Setup bei V4
        assertEquals(folder.listFiles()?.none { it.name.contains("__setup_uuid_extension") }, true)

        val create = folder.findBySuffix("__create_sample_table.sql")
        assertTrue(create.readText().contains("uuid_generate_v4"))
        assertTrue(create.readText().startsWith("-- HASH:"))
    }

    @Test
    fun testMariaDbUuidV4Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.MARIADB, SchemaGenerator.UUIDType.UUID_V4)

        val folder = dir.folderFor(SampleEntity::class.java)
        val create = folder.findBySuffix("__create_sample_table.sql")

        val text = create.readText()
        assertTrue(text.contains("CHAR(36)"))
        assertTrue(text.contains("DEFAULT (UUID())"))
    }

    @Test
    fun testMariaDbUuidV7Fallback() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.MARIADB, SchemaGenerator.UUIDType.UUID_V7)

        val folder = dir.folderFor(SampleEntity::class.java)
        val create = folder.findBySuffix("__create_sample_table.sql")

        assertTrue(create.readText().contains("DEFAULT (UUID())")) // fallback
    }

    @Test
    fun testEntityDiscovery() {
        val gen = SchemaGenerator(entityPackage, tmpDir)
        val entities = gen.testFindEntities()

        assertTrue(entities.any { it.simpleName == "SampleEntity" })
        assertTrue(entities.any { it.simpleName == "AnotherEntity" })
    }

    @Test
    fun testForeignKeyGeneration() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val fk = folder.findBySuffix("__add_foreign_keys.sql").readText()

        assertTrue(fk.contains("ON DELETE CASCADE") || fk.contains("ON DELETE SET NULL"))
    }

    @Test
    fun testIndexGeneration() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val idx = folder.findBySuffix("__add_indexes.sql").readText()

        assertTrue(idx.contains("CREATE INDEX"))
    }

    @Test
    fun testEmbeddedFields() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val create = folder.findBySuffix("__create_sample_table.sql").readText()

        assertTrue(create.contains("address_street"))
        assertTrue(create.contains("address_city"))
    }

    @Test
    fun testHashHeaderExists() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val file = folder.findBySuffix("__create_sample_table.sql")
        val firstLine = file.readLines().first()

        assertTrue(firstLine.startsWith("-- HASH:"))
        assertTrue(firstLine.length > 15)
    }

    @Test
    fun testGroupingByPackage() {
        val strategy = PackageNamePartitionStrategy()

        val grouped = SchemaGenerator(entityPackage, tmpDir, partitionStrategy = strategy)
            .groupByPartition(listOf(SampleEntity::class.java, AnotherEntity::class.java))

        assertTrue(grouped.containsKey("fu"))
        assertTrue(grouped.containsKey("bar"))
    }


    private fun runGenerator(
        db: SchemaGenerator.DatabaseType,
        uuid: SchemaGenerator.UUIDType
    ): File {
        val outDir = File(tmpDir, "migration_${db}_${uuid}")
        SchemaGenerator(entityPackage, outDir, db, uuid).generate()
        return outDir
    }

    private fun File.folderFor(entity: Class<*>): File {
        val folder = File(this, entity.packageName.split(".").last())
        require(folder.exists()) { "Folder for entity not found: ${folder.absolutePath}" }
        return folder
    }

    private fun File.findBySuffix(suffix: String): File {
        val candidates = listFiles()?.filter { it.name.contains(suffix) }.orEmpty()
        require(candidates.isNotEmpty()) {
            "No file ending with '$suffix' in: ${listFiles()?.toList()}"
        }
        return candidates.first()
    }
}

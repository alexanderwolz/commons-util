package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.entity.bar.AnotherEntity
import de.alexanderwolz.commons.util.database.entity.fu.SampleEntity
import de.alexanderwolz.commons.util.database.migration.DatabaseMigrationUtils
import de.alexanderwolz.commons.util.database.provider.DefaultSchemaProvider
import jakarta.persistence.Table
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SchemaGeneratorTest {

    @TempDir
    private lateinit var tmpDir: File

    private val entityPackage = EntityScannerTest::class.java.packageName + ".entity"

    @Test
    fun testPostgresUuidV7Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V7)

        val setup = dir.findBySuffix("_setup_uuid_extension.sql")

        val folder = dir.folderFor(SampleEntity::class.java)
        val create = folder.findBySuffix("_create_sample_table.sql")

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

        val create = folder.findBySuffix("_create_sample_table.sql")
        assertTrue(create.readText().contains("uuid_generate_v4"))
        assertTrue(create.readText().startsWith("-- HASH:"))
    }

    @Test
    fun testMariaDbUuidV4Generation() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.MARIADB, SchemaGenerator.UUIDType.UUID_V4)

        val folder = dir.folderFor(SampleEntity::class.java)
        val create = folder.findBySuffix("_create_sample_table.sql")

        val text = create.readText()
        assertTrue(text.contains("CHAR(36)"))
        assertTrue(text.contains("DEFAULT (UUID())"))
    }

    @Test
    fun testMariaDbUuidV7Fallback() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.MARIADB, SchemaGenerator.UUIDType.UUID_V7)

        val folder = dir.folderFor(SampleEntity::class.java)
        val create = folder.findBySuffix("_create_sample_table.sql")

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

        val fk = folder.findBySuffix("_add_foreign_keys.sql").readText()

        assertTrue(fk.contains("ON DELETE CASCADE") || fk.contains("ON DELETE SET NULL"))
    }

    @Test
    fun testIndexGeneration() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val idx = folder.findBySuffix("_add_indexes.sql").readText()

        assertTrue(idx.contains("CREATE INDEX"))
    }

    @Test
    fun testEmbeddedFields() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val create = folder.findBySuffix("_create_sample_table.sql").readText()

        assertTrue(create.contains("address_street"))
        assertTrue(create.contains("address_city"))
    }

    @Test
    fun testHashHeaderExists() {
        val dir = runGenerator(SchemaGenerator.DatabaseType.POSTGRES, SchemaGenerator.UUIDType.UUID_V4)
        val folder = dir.folderFor(SampleEntity::class.java)

        val file = folder.findBySuffix("_create_sample_table.sql")
        val firstLine = file.readLines().first()

        assertTrue(firstLine.startsWith("-- HASH:"))
        assertTrue(firstLine.length > 15)
    }

    @Test
    fun testGroupingByPackage() {
        val strategy = object : DefaultSchemaProvider() {
            override fun getFolderFor(entityClass: Class<*>, root: File) = entityClass.packageName.split(".").last()
            override fun getSetupFolder(root: File): String = "setup"
        }

        val grouped = DatabaseMigrationUtils.groupByPartition(
            listOf(SampleEntity::class.java, AnotherEntity::class.java),
            strategy, tmpDir
        )

        assertTrue(grouped.containsKey("fu"))
        assertTrue(grouped.containsKey("bar"))
    }


    private fun runGenerator(
        db: SchemaGenerator.DatabaseType, uuid: SchemaGenerator.UUIDType
    ): File {
        val outDir = File(tmpDir, "migration_${db}_${uuid}")
        SchemaGenerator(entityPackage, outDir, db, uuid).generate()
        return outDir
    }

    private fun File.folderFor(entity: Class<*>): File {
        val table = entity.annotations
            .find { it.annotationClass == Table::class }
            .let { it as? Table }
            ?.name
            ?: entity.simpleName.lowercase()

        val sql = this.walkTopDown()
            .filter { it.isFile && it.extension == "sql" }
            .firstOrNull { it.name.contains(table) }
            ?: error("No SQL file found for entity ${entity.simpleName} in $this")

        return sql.parentFile
    }

    fun File.findBySuffix(suffix: String): File {
        if (this.name.contains(suffix)) return this
        this.listFiles { it.name.contains(suffix) }?.firstOrNull()?.let {
            return it
        }
        return this.listFiles()?.firstOrNull { it.name.contains(suffix) }
            ?: error("No file containing '$suffix' in: ${this.listFiles()?.toList()} ($this)")
    }

}

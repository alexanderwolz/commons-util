package de.alexanderwolz.commons.util.database

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SchemaGeneratorMigrationTest {

    @TempDir
    lateinit var tmpDir: File

    private fun newGenerator(outDir: File = tmpDir) = SchemaGenerator(
        basePackage = javaClass.packageName + ".entity",
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

        println(f1.joinToString())
        println(f2.joinToString())

        assertEquals(2, f2.size, "changed content must create a new migration")

        val sorted = f2.sortedBy { it.name }
        val fileV2 = sorted.last()

        assertTrue(fileV2.readLines().first().startsWith("-- HASH:"))
        assertTrue(fileV2.readText().contains("name VARCHAR(255)"))
        assertTrue(fileV1 != fileV2.name, "filenames must differ for changed content")
    }
}

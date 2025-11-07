package de.alexanderwolz.commons.util.database

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertTrue

class SchemaGeneratorTest {

    @TempDir
    private lateinit var tmpDir: File

    @Test
    fun testSchemaGeneration() {
        val basePackage = SchemaGenerator::class.java.packageName
        val outDir = File(tmpDir, "migration")
        val generator = SchemaGenerator(basePackage, outDir)
        generator.generate()

        val expectedTableName = "sample"
        val migrationFile = File(tmpDir, "migration/database/V1__create_${expectedTableName}_table.sql")
        assertTrue { tmpDir.listFiles()?.isNotEmpty() == true }
        assertTrue { migrationFile.exists() }
        println(migrationFile.readText())
    }

}
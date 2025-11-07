package de.alexanderwolz.commons.util.database

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
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

        assertTrue { tmpDir.listFiles()?.isNotEmpty() == true }

        val expectedTableName = "sample"
        val migrationFile = File(tmpDir, "migration/database/V1__create_${expectedTableName}_table.sql")
        assertTrue { migrationFile.exists() }
        //println(migrationFile.readText())
        val migrationFileLines = migrationFile.readText().lines()
        val expectedLines = expectedContent.lines()

        assertEquals(13, migrationFileLines.size)
        for (i in 0..<migrationFileLines.size) {
            val content = migrationFileLines[i]
            if (content.startsWith("-- Generated:")) {
                continue
            }
            assertEquals(expectedLines[i], content)
        }
    }

    private val expectedContent = "-- create_sample_table\n" +
            "-- Entity: SampleEntity\n" +
            "-- Generated: 2025-11-07T20:24:02.642021\n" +
            "\n" +
            "CREATE TABLE sample (\n" +
            "    id                                       VARCHAR(255)         PRIMARY KEY,\n" +
            "    username                                 VARCHAR(100)         UNIQUE,\n" +
            "    given_name                               VARCHAR(100),\n" +
            "    family_name                              VARCHAR(100),\n" +
            "    email                                    VARCHAR(255),\n" +
            "    snapshot                                 JSONB,\n" +
            "    pdf_bytes                                BYTEA\n" +
            ");"

}
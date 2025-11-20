package de.alexanderwolz.commons.util.database.provider

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

open class DefaultSchemaProvider : SchemaProvider {

    private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

    override fun getFolderFor(entityClass: Class<*>, root: File) = ""

    override fun getSetupFolder(root: File) = ""

    override fun getFileName(timestamp: LocalDateTime, sortNumber: String, baseName: String): String {
        val version = formatter.format(timestamp) + sortNumber // e.g. 202511201733580001
        return "V${version}__${baseName}.sql"
    }

    override fun getFileNameRegex(timestamp: LocalDateTime, sortNumber: String, baseName: String): Regex {
        return Regex("""V\d{14}${sortNumber}__${baseName}\.sql""")
    }

}
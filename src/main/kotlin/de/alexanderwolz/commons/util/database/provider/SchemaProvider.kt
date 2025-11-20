package de.alexanderwolz.commons.util.database.provider

import java.io.File
import java.time.LocalDateTime

interface SchemaProvider {

    fun getFolderFor(entityClass: Class<*>, root: File): String?

    fun getSetupFolder(root: File): String?

    fun getFileName(timestamp: LocalDateTime, sortNumber: String, baseName: String): String

    fun getFileNameRegex(timestamp: LocalDateTime, sortNumber: String, baseName: String): Regex
}

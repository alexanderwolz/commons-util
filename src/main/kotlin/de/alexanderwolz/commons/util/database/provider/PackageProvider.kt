package de.alexanderwolz.commons.util.database.provider

import java.io.File

interface PackageProvider {
    fun getFolderFor(entityClass: Class<*>, root: File): String?
    fun getCommonFolder(root: File): String?
}

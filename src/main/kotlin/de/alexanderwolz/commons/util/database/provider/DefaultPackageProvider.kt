package de.alexanderwolz.commons.util.database.provider

import java.io.File

class DefaultPackageProvider : PackageProvider {
    override fun getFolderFor(entityClass: Class<*>, root: File) = ""
    override fun getCommonFolder(root: File) = ""
}
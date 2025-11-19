package de.alexanderwolz.commons.util.database

interface PartitionStrategy {
    fun folderFor(entityClass: Class<*>): String
}

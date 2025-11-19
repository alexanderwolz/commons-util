package de.alexanderwolz.commons.util.database

class PackageNamePartitionStrategy : PartitionStrategy {

    override fun folderFor(entityClass: Class<*>): String {
        //returns last folder of package name
        return entityClass.packageName.split(".").last().lowercase()
    }

}
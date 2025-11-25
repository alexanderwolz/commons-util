package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.log.Logger
import jakarta.persistence.Entity
import java.io.File

class EntityScanner(private val basePackage: String) {

    private val logger = Logger(javaClass)

    fun findEntities(): List<Class<*>> {
        val result = mutableListOf<Class<*>>()
        val path = basePackage.replace('.', '/')
        val loader = Thread.currentThread().contextClassLoader

        val urls = loader.getResources(path).toList()

        urls.forEach { url ->
            val dir = File(url.toURI())
            if (dir.exists() && dir.isDirectory) {
                scanDirectory(dir, result)
            }
        }

        return result
    }

    private fun scanDirectory(dir: File, out: MutableList<Class<*>>) {
        dir.walkTopDown().forEach { file ->
            if (!file.name.endsWith(".class")) return@forEach
            if (file.name.contains("$")) return@forEach // skip inner classes

            val abs = file.absolutePath
            val pkgPath = basePackage.replace('.', File.separatorChar)
            val start = abs.indexOf(pkgPath)
            if (start == -1) return@forEach

            val classPath = abs
                .substring(start)
                .removeSuffix(".class")
                .replace(File.separatorChar, '.')

            try {
                val clazz = Class.forName(classPath)
                if (clazz.isAnnotationPresent(Entity::class.java)) {
                    out.add(clazz)
                    logger.info { "Found entity: ${clazz.name}" }
                }
            } catch (_: Exception) {
            }
        }
    }
}
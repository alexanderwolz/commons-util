package de.alexanderwolz.commons.util.jpa

import de.alexanderwolz.commons.util.jpa.entity.NonEntity
import de.alexanderwolz.commons.util.jpa.entity.ReferenceEntity
import de.alexanderwolz.commons.util.jpa.entity.bar.AnotherEntity
import de.alexanderwolz.commons.util.jpa.entity.fu.SampleEntity
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class EntityScannerTest {

    @Test
    fun testFindEntities() {
        val scanner = EntityScanner(javaClass.packageName)

        val entities = scanner.findEntities()
        Assertions.assertEquals(3, entities.size)
        Assertions.assertTrue(entities.contains(ReferenceEntity::class.java))
        Assertions.assertTrue(entities.contains(SampleEntity::class.java))
        Assertions.assertTrue(entities.contains(AnotherEntity::class.java))
        Assertions.assertFalse(entities.contains(NonEntity::class.java))
    }
}
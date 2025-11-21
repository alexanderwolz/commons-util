package de.alexanderwolz.commons.util.database

import de.alexanderwolz.commons.util.database.migration.SqlFileSchemaExtractor
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class SqlFileSchemaExtractorTest {

    @TempDir
    lateinit var tmpDir: File

    // ==================== Basic Parsing Tests ====================

    @Test
    fun testParseSimpleCreateTable() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_users.sql")
        sqlFile.writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(2, schema!!.columns.size)

        val idCol = schema.columns[0]
        assertEquals("id", idCol.name)
        assertEquals("BIGSERIAL", idCol.type)
        assertTrue(idCol.isPrimaryKey)
        assertFalse(idCol.nullable)

        val nameCol = schema.columns[1]
        assertEquals("name", nameCol.name)
        assertEquals("VARCHAR(255)", nameCol.type)
        assertFalse(nameCol.nullable)
        assertFalse(nameCol.unique)
    }

    @Test
    fun testParseTableWithAllColumnTypes() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_test_table.sql")
        sqlFile.writeText(
            """
            CREATE TABLE test_table (
                id            BIGSERIAL       PRIMARY KEY,
                name          VARCHAR(255)    NOT NULL,
                email         VARCHAR(255)    NOT NULL UNIQUE,
                age           INTEGER,
                salary        DECIMAL(10,2)   NOT NULL,
                is_active     BOOLEAN         DEFAULT true,
                birth_date    DATE,
                created_at    TIMESTAMP       DEFAULT NOW(),
                updated_at    TIMESTAMP,
                profile_data  TEXT,
                settings      JSONB,
                avatar        BYTEA
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "test_table")

        assertNotNull(schema)
        assertEquals(12, schema!!.columns.size)

        // Test specific columns
        val emailCol = schema.columns.find { it.name == "email" }
        assertNotNull(emailCol)
        assertTrue(emailCol!!.unique)
        assertFalse(emailCol.nullable)

        val ageCol = schema.columns.find { it.name == "age" }
        assertNotNull(ageCol)
        assertTrue(ageCol!!.nullable)

        val salaryCol = schema.columns.find { it.name == "salary" }
        assertNotNull(salaryCol)
        assertEquals("DECIMAL(10,2)", salaryCol!!.type)

        val isActiveCol = schema.columns.find { it.name == "is_active" }
        assertNotNull(isActiveCol)
        assertEquals("true", isActiveCol!!.defaultValue)

        val createdAtCol = schema.columns.find { it.name == "created_at" }
        assertNotNull(createdAtCol)
        assertEquals("NOW()", createdAtCol!!.defaultValue)
    }

    @Test
    fun testParseTableWithCompositeKey() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_user_roles.sql")
        sqlFile.writeText(
            """
            CREATE TABLE user_roles (
                user_id   BIGINT NOT NULL,
                role_id   BIGINT NOT NULL,
                granted_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, role_id)
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "user_roles")

        assertNotNull(schema)
        assertEquals(3, schema!!.columns.size)

        // Columns should not be marked as individual primary keys in this case
        val userIdCol = schema.columns.find { it.name == "user_id" }
        val roleIdCol = schema.columns.find { it.name == "role_id" }

        assertNotNull(userIdCol)
        assertNotNull(roleIdCol)
        assertFalse(userIdCol!!.nullable)
        assertFalse(roleIdCol!!.nullable)
    }

    @Test
    fun testParseCreateTableIfNotExists() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_users.sql")
        sqlFile.writeText(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255)
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(2, schema!!.columns.size)
    }

    @Test
    fun testParseTableWithComments() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_users.sql")
        sqlFile.writeText(
            """
            -- User authentication table
            CREATE TABLE users (
                -- Primary identifier
                id           BIGSERIAL    PRIMARY KEY,
                -- User login name
                username     VARCHAR(50)  NOT NULL UNIQUE,
                -- Email for notifications
                email        VARCHAR(255) NOT NULL,
                -- Account status
                active       BOOLEAN      DEFAULT true
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(4, schema!!.columns.size)

        schema.columns.forEach { col ->
            println("Parsed column: ${col.name} ${col.type}")
        }
    }

    // ==================== Index Parsing Tests ====================

    @Test
    fun testParseSimpleIndex() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL
            );
        """.trimIndent()
        )

        File(schemaDir, "indexes.sql").writeText(
            """
            CREATE INDEX idx_users_email ON users (email);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(1, schema!!.indexes.size)

        val index = schema.indexes[0]
        assertEquals("idx_users_email", index.name)
        assertEquals(listOf("email"), index.columns)
        assertFalse(index.unique)
    }

    @Test
    fun testParseUniqueIndex() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                email VARCHAR(255)
            );
        """.trimIndent()
        )

        File(schemaDir, "indexes.sql").writeText(
            """
            CREATE UNIQUE INDEX idx_users_email ON users (email);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(1, schema!!.indexes.size)

        val index = schema.indexes[0]
        assertTrue(index.unique)
    }

    @Test
    fun testParseCompositeIndex() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_orders.sql").writeText(
            """
            CREATE TABLE orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """.trimIndent()
        )

        File(schemaDir, "indexes.sql").writeText(
            """
            CREATE INDEX idx_orders_user_created ON orders (user_id, created_at);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "orders")

        assertNotNull(schema)
        assertEquals(1, schema!!.indexes.size)

        val index = schema.indexes[0]
        assertEquals(listOf("user_id", "created_at"), index.columns)
    }

    @Test
    fun testParseMultipleIndexes() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                username VARCHAR(50),
                email VARCHAR(255),
                created_at TIMESTAMP
            );
        """.trimIndent()
        )

        File(schemaDir, "indexes.sql").writeText(
            """
            CREATE UNIQUE INDEX idx_users_username ON users (username);
            CREATE UNIQUE INDEX idx_users_email ON users (email);
            CREATE INDEX idx_users_created_at ON users (created_at);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(3, schema!!.indexes.size)

        val uniqueIndexes = schema.indexes.filter { it.unique }
        assertEquals(2, uniqueIndexes.size)
    }

    // ==================== Foreign Key Parsing Tests ====================

    @Test
    fun testParseSingleForeignKey() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_orders.sql").writeText(
            """
            CREATE TABLE orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL
            );
        """.trimIndent()
        )

        File(schemaDir, "foreign_keys.sql").writeText(
            """
            ALTER TABLE orders
                ADD CONSTRAINT fk_orders_user_id
                FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE CASCADE;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "orders")

        assertNotNull(schema)
        assertEquals(1, schema!!.foreignKeys.size)

        val fk = schema.foreignKeys[0]
        assertEquals("user_id", fk.columnName)
        assertEquals("users", fk.referencedTable)
        assertEquals("id", fk.referencedColumn)
        assertEquals("CASCADE", fk.onDelete)
    }

    @Test
    fun testParseMultipleForeignKeys() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_order_items.sql").writeText(
            """
            CREATE TABLE order_items (
                id BIGSERIAL PRIMARY KEY,
                order_id BIGINT NOT NULL,
                product_id BIGINT NOT NULL
            );
        """.trimIndent()
        )

        File(schemaDir, "foreign_keys.sql").writeText(
            """
            ALTER TABLE order_items
                ADD CONSTRAINT fk_order_items_order_id
                FOREIGN KEY (order_id)
                REFERENCES orders(id)
                ON DELETE CASCADE;
                
            ALTER TABLE order_items
                ADD CONSTRAINT fk_order_items_product_id
                FOREIGN KEY (product_id)
                REFERENCES products(id)
                ON DELETE RESTRICT;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "order_items")

        assertNotNull(schema)
        assertEquals(2, schema!!.foreignKeys.size)

        val orderFk = schema.foreignKeys.find { it.columnName == "order_id" }
        val productFk = schema.foreignKeys.find { it.columnName == "product_id" }

        assertNotNull(orderFk)
        assertNotNull(productFk)
        assertEquals("CASCADE", orderFk!!.onDelete)
        assertEquals("RESTRICT", productFk!!.onDelete)
    }

    @Test
    fun testParseForeignKeyWithSetNull() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_posts.sql").writeText(
            """
            CREATE TABLE posts (
                id BIGSERIAL PRIMARY KEY,
                author_id BIGINT
            );
        """.trimIndent()
        )

        File(schemaDir, "foreign_keys.sql").writeText(
            """
            ALTER TABLE posts
                ADD CONSTRAINT fk_posts_author_id
                FOREIGN KEY (author_id)
                REFERENCES users(id)
                ON DELETE SET NULL;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "posts")

        assertNotNull(schema)
        assertEquals(1, schema!!.foreignKeys.size)
        assertEquals("SET NULL", schema.foreignKeys[0].onDelete)
    }

    @Test
    fun testParseForeignKeyWithNoAction() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_audit_logs.sql").writeText(
            """
            CREATE TABLE audit_logs (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL
            );
        """.trimIndent()
        )

        File(schemaDir, "foreign_keys.sql").writeText(
            """
            ALTER TABLE audit_logs
                ADD CONSTRAINT fk_audit_logs_user_id
                FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE NO ACTION;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "audit_logs")

        assertNotNull(schema)
        assertEquals(1, schema!!.foreignKeys.size)
        assertEquals("NO ACTION", schema.foreignKeys[0].onDelete)
    }

    // ==================== Multi-Schema Tests ====================

    @Test
    fun testParseTablesFromMultipleSchemas() {
        val publicDir = File(tmpDir, "public").apply { mkdirs() }
        val domainDir = File(tmpDir, "domain").apply { mkdirs() }

        File(publicDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                username VARCHAR(50)
            );
        """.trimIndent()
        )

        File(domainDir, "create_products.sql").writeText(
            """
            CREATE TABLE products (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255)
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)

        val usersSchema = extractor.loadTableSchema("public", "users")
        val productsSchema = extractor.loadTableSchema("domain", "products")

        assertNotNull(usersSchema)
        assertNotNull(productsSchema)
        assertEquals(2, usersSchema!!.columns.size)
        assertEquals(2, productsSchema!!.columns.size)
    }

    @Test
    fun testGetExistingTablesFromMultipleSchemas() {
        val publicDir = File(tmpDir, "public").apply { mkdirs() }
        val domainDir = File(tmpDir, "domain").apply { mkdirs() }

        File(publicDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        File(publicDir, "create_sessions.sql").writeText(
            """
            CREATE TABLE sessions (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        File(domainDir, "create_products.sql").writeText(
            """
            CREATE TABLE products (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val existingTables = extractor.getExistingTables()

        assertEquals(3, existingTables.size)
        assertTrue(existingTables.contains("users"))
        assertTrue(existingTables.contains("sessions"))
        assertTrue(existingTables.contains("products"))
    }

    // ==================== Edge Cases ====================

    @Test
    fun testParseTableWithTrailingCommas() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_users.sql")
        sqlFile.writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255),
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(2, schema!!.columns.size)
    }

    @Test
    fun testParseEmptyTable() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_empty.sql")
        sqlFile.writeText(
            """
            CREATE TABLE empty_table (
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "empty_table")

        assertNotNull(schema)
        assertTrue(schema!!.columns.isEmpty())
    }

    @Test
    fun testLoadNonExistentTable() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "nonexistent")

        assertNull(schema)
    }

    @Test
    fun testLoadFromNonExistentSchema() {
        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("nonexistent_schema", "users")

        assertNull(schema)
    }

    @Test
    fun testGetExistingTablesFromEmptyDirectory() {
        val extractor = SqlFileSchemaExtractor(tmpDir)
        val existingTables = extractor.getExistingTables()

        assertTrue(existingTables.isEmpty())
    }

    @Test
    fun testParseTableWithLongDefaultValue() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_config.sql")
        sqlFile.writeText(
            """
            CREATE TABLE config (
                id BIGSERIAL PRIMARY KEY,
                settings JSONB DEFAULT '{"theme": "dark", "notifications": true}'::jsonb
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "config")

        assertNotNull(schema)
        val settingsCol = schema!!.columns.find { it.name == "settings" }
        assertNotNull(settingsCol)
        assertNotNull(settingsCol!!.defaultValue)
    }

    @Test
    fun testParseCaseInsensitiveKeywords() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        val sqlFile = File(schemaDir, "create_users.sql")
        sqlFile.writeText(
            """
            create table users (
                id bigserial primary key,
                name varchar(255) not null,
                email varchar(255) unique
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(3, schema!!.columns.size)
    }

    // ==================== Latest File Selection Tests ====================

    @Test
    fun testSelectsLatestCreateTableFile() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        // Older version
        File(schemaDir, "V20241121000000__1000_create_users_table.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY
            );
        """.trimIndent()
        )

        // Newer version (should be used)
        File(schemaDir, "V20241121000001__1000_create_users_table.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(2, schema!!.columns.size, "Should use the latest version")
    }

    @Test
    fun testIgnoresNonSqlFiles() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_users.txt").writeText(
            """
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
        """.trimIndent()
        )

        File(schemaDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(255)
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "users")

        assertNotNull(schema)
        assertEquals(2, schema!!.columns.size, "Should only read .sql files")
    }

    // ==================== Integration Tests ====================

    @Test
    fun testCompleteTableSchemaWithAllFeatures() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        // CREATE TABLE
        File(schemaDir, "V20241121000000__1000_create_orders_table.sql").writeText(
            """
            CREATE TABLE orders (
                id              BIGSERIAL       PRIMARY KEY,
                order_number    VARCHAR(50)     NOT NULL UNIQUE,
                user_id         BIGINT          NOT NULL,
                total_amount    DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
                status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
                notes           TEXT,
                created_at      TIMESTAMP       DEFAULT NOW(),
                updated_at      TIMESTAMP
            );
        """.trimIndent()
        )

        // INDEXES
        File(schemaDir, "V20241121000001__0100_indexes.sql").writeText(
            """
            CREATE UNIQUE INDEX idx_orders_order_number ON orders (order_number);
            CREATE INDEX idx_orders_user_id ON orders (user_id);
            CREATE INDEX idx_orders_status ON orders (status);
            CREATE INDEX idx_orders_created_at ON orders (created_at);
        """.trimIndent()
        )

        // FOREIGN KEYS
        File(schemaDir, "V20241121000002__0200_foreign_keys.sql").writeText(
            """
            ALTER TABLE orders
                ADD CONSTRAINT fk_orders_user_id
                FOREIGN KEY (user_id)
                REFERENCES users(id)
                ON DELETE CASCADE;
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)
        val schema = extractor.loadTableSchema("public", "orders")

        assertNotNull(schema)

        // Verify columns
        assertEquals(8, schema!!.columns.size)

        val orderNumberCol = schema.columns.find { it.name == "order_number" }
        assertNotNull(orderNumberCol)
        assertTrue(orderNumberCol!!.unique)
        assertFalse(orderNumberCol.nullable)

        val totalAmountCol = schema.columns.find { it.name == "total_amount" }
        assertNotNull(totalAmountCol)
        assertEquals("0.00", totalAmountCol!!.defaultValue)

        // Verify indexes
        assertEquals(4, schema.indexes.size)
        val uniqueIndexes = schema.indexes.filter { it.unique }
        assertEquals(1, uniqueIndexes.size)

        // Verify foreign keys
        assertEquals(1, schema.foreignKeys.size)
        val fk = schema.foreignKeys[0]
        assertEquals("user_id", fk.columnName)
        assertEquals("users", fk.referencedTable)
        assertEquals("CASCADE", fk.onDelete)
    }

    @Test
    fun testTableSchemaConsistencyAcrossMultipleReads() {
        val schemaDir = File(tmpDir, "public").apply { mkdirs() }

        File(schemaDir, "create_users.sql").writeText(
            """
            CREATE TABLE users (
                id BIGSERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL
            );
        """.trimIndent()
        )

        val extractor = SqlFileSchemaExtractor(tmpDir)

        val schema1 = extractor.loadTableSchema("public", "users")
        val schema2 = extractor.loadTableSchema("public", "users")

        assertNotNull(schema1)
        assertNotNull(schema2)

        assertEquals(schema1!!.columns.size, schema2!!.columns.size)
        schema1.columns.forEachIndexed { index, col1 ->
            val col2 = schema2.columns[index]
            assertEquals(col1.name, col2.name)
            assertEquals(col1.type, col2.type)
            assertEquals(col1.nullable, col2.nullable)
            assertEquals(col1.unique, col2.unique)
            assertEquals(col1.isPrimaryKey, col2.isPrimaryKey)
        }
    }
}
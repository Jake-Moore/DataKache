package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.result.DefiniteResult;
import com.jakemoore.datakache.api.result.OptionalResult;
import com.jakemoore.datakache.api.result.Success;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TestDatabaseOperationsJava extends AbstractDataKacheJavaTest {

    @Test
    void readFromDatabaseAsync_existingKey_returnsSuccess() throws Exception {
        getCache().createAsync("dbReadKey", doc -> doc.withName("db")).get(5, TimeUnit.SECONDS);

        OptionalResult<TestGenericDoc> result = getCache().readFromDatabaseAsync("dbReadKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals("db", result.getOrNull().getName());
    }

    @Test
    void readFromDatabaseAsync_nonExistentKey_returnsEmpty() throws Exception {
        OptionalResult<TestGenericDoc> result = getCache().readFromDatabaseAsync("noSuchKey")
            .get(5, TimeUnit.SECONDS);

        assertTrue(result.isEmpty());
    }

    @Test
    void readAllFromDatabaseAsync_returnsListType() throws Exception {
        getCache().createAsync("listDoc1").get(5, TimeUnit.SECONDS);

        DefiniteResult<List<TestGenericDoc>> result = getCache().readAllFromDatabaseAsync()
            .get(5, TimeUnit.SECONDS);

        // Verifies the return type is List — not a Flow or other Kotlin type
        assertInstanceOf(Success.class, result);
        assertNotNull(result.getOrNull());
    }

    @Test
    void readAllFromDatabaseAsync_containsAllDocuments() throws Exception {
        getCache().createAsync("allDoc1", doc -> doc.withName("all1").withBalance(1.0)).get(5, TimeUnit.SECONDS);
        getCache().createAsync("allDoc2", doc -> doc.withName("all2").withBalance(2.0)).get(5, TimeUnit.SECONDS);
        getCache().createAsync("allDoc3", doc -> doc.withName("all3").withBalance(3.0)).get(5, TimeUnit.SECONDS);

        DefiniteResult<List<TestGenericDoc>> result = getCache().readAllFromDatabaseAsync()
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        List<TestGenericDoc> docs = result.getOrNull();
        assertNotNull(docs);
        assertEquals(3, docs.size());
    }

    @Test
    void readKeysFromDatabaseAsync_returnsListType() throws Exception {
        getCache().createAsync("keyDoc1").get(5, TimeUnit.SECONDS);

        DefiniteResult<List<String>> result = getCache().readKeysFromDatabaseAsync()
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertNotNull(result.getOrNull());
    }

    @Test
    void readKeysFromDatabaseAsync_containsAllKeys() throws Exception {
        getCache().createAsync("k1", doc -> doc.withName("key1").withBalance(10.0)).get(5, TimeUnit.SECONDS);
        getCache().createAsync("k2", doc -> doc.withName("key2").withBalance(20.0)).get(5, TimeUnit.SECONDS);
        getCache().createAsync("k3", doc -> doc.withName("key3").withBalance(30.0)).get(5, TimeUnit.SECONDS);

        DefiniteResult<List<String>> result = getCache().readKeysFromDatabaseAsync()
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        List<String> keys = result.getOrNull();
        assertNotNull(keys);
        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));
        assertTrue(keys.contains("k3"));
    }

    @Test
    void readSizeFromDatabaseAsync_returnsCorrectCount() throws Exception {
        getCache().createAsync("sizeDoc1", doc -> doc.withName("size1").withBalance(10.0)).get(5, TimeUnit.SECONDS);
        getCache().createAsync("sizeDoc2", doc -> doc.withName("size2").withBalance(20.0)).get(5, TimeUnit.SECONDS);

        DefiniteResult<Long> result = getCache().readSizeFromDatabaseAsync()
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals(2L, result.getOrNull());
    }

    @Test
    void hasKeyInDatabaseAsync_existingKey_returnsTrue() throws Exception {
        getCache().createAsync("hasKey").get(5, TimeUnit.SECONDS);

        DefiniteResult<Boolean> result = getCache().hasKeyInDatabaseAsync("hasKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertTrue(result.getOrNull());
    }

    @Test
    void hasKeyInDatabaseAsync_nonExistentKey_returnsFalse() throws Exception {
        DefiniteResult<Boolean> result = getCache().hasKeyInDatabaseAsync("absent")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertFalse(result.getOrNull());
    }
}

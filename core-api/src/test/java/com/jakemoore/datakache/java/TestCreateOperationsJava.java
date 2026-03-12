package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.result.DefiniteResult;
import com.jakemoore.datakache.api.result.Failure;
import com.jakemoore.datakache.api.result.Success;
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException;
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TestCreateOperationsJava extends AbstractDataKacheJavaTest {

    @Test
    void createAsync_withKey_returnsSuccess() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache().createAsync("testKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        TestGenericDoc doc = result.getOrNull();
        assertNotNull(doc);
        assertEquals("testKey", doc.getKey());
        assertNull(doc.getName());
        assertEquals(0.0, doc.getBalance());
        assertEquals(0L, doc.getVersion());
    }

    @Test
    void createAsync_withKeyAndInitializer_appliesLambda() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache()
            .createAsync("initKey", doc -> doc.withName("Test").withBalance(100.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        TestGenericDoc doc = result.getOrNull();
        assertNotNull(doc);
        assertEquals("initKey", doc.getKey());
        assertEquals("Test", doc.getName());
        assertEquals(100.0, doc.getBalance());
    }

    @Test
    void createAsync_duplicateKey_returnsFailure() throws Exception {
        getCache().createAsync("dupKey").get(5, TimeUnit.SECONDS);

        DefiniteResult<TestGenericDoc> second = getCache().createAsync("dupKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, second);
    }

    @Test
    void createAsync_duplicateKey_failureExceptionIsCorrectType() throws Exception {
        getCache().createAsync("dupKey2").get(5, TimeUnit.SECONDS);

        DefiniteResult<TestGenericDoc> second = getCache().createAsync("dupKey2")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, second);
        Throwable ex = second.exceptionOrNull();
        assertInstanceOf(ResultExceptionWrapper.class, ex);
        assertInstanceOf(DuplicateDocumentKeyException.class, ((ResultExceptionWrapper) ex).getException());
    }

    @Test
    void createRandomAsync_noArgs_returnsSuccessWithUuidKey() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache().createRandomAsync()
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        TestGenericDoc doc = result.getOrNull();
        assertNotNull(doc);
        assertDoesNotThrow(() -> UUID.fromString(doc.getKey()));
    }

    @Test
    void createRandomAsync_withInitializer_appliesLambda() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache()
            .createRandomAsync(doc -> doc.withBalance(50.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        TestGenericDoc doc = result.getOrNull();
        assertNotNull(doc);
        assertEquals(50.0, doc.getBalance());
    }
}

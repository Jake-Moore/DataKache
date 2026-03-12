package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.result.DefiniteResult;
import com.jakemoore.datakache.api.result.OptionalResult;
import com.jakemoore.datakache.api.result.Success;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TestDeleteOperationsJava extends AbstractDataKacheJavaTest {

    @Test
    void deleteAsync_byKey_existingDoc_returnsSuccessTrue() throws Exception {
        getCache().createAsync("deleteKey").get(5, TimeUnit.SECONDS);

        DefiniteResult<Boolean> result = getCache().deleteAsync("deleteKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertTrue(result.getOrNull());
    }

    @Test
    void deleteAsync_byKey_afterDelete_documentNotInCache() throws Exception {
        getCache().createAsync("deletedDoc").get(5, TimeUnit.SECONDS);
        getCache().deleteAsync("deletedDoc").get(5, TimeUnit.SECONDS);

        OptionalResult<TestGenericDoc> read = getCache().read("deletedDoc");
        assertTrue(read.isEmpty());
    }

    @Test
    void deleteAsync_byKey_nonExistentKey_returnsSuccessFalse() throws Exception {
        DefiniteResult<Boolean> result = getCache().deleteAsync("neverExisted")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertFalse(result.getOrNull());
    }

    @Test
    void deleteAsync_byDoc_existingDoc_returnsSuccessTrue() throws Exception {
        TestGenericDoc created = getCache().createAsync("byDocDelete")
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(created);

        DefiniteResult<Boolean> result = getCache().deleteAsync(created)
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertTrue(result.getOrNull());
    }
}

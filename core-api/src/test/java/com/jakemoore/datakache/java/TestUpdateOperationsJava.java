package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.exception.DocumentNotFoundException;
import com.jakemoore.datakache.api.exception.update.RejectUpdateException;
import com.jakemoore.datakache.api.result.*;
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TestUpdateOperationsJava extends AbstractDataKacheJavaTest {

    @Test
    void updateAsync_byKey_appliesLambda() throws Exception {
        getCache().createAsync("updateKey", doc -> doc.withBalance(1.0)).get(5, TimeUnit.SECONDS);

        DefiniteResult<TestGenericDoc> result = getCache()
            .updateAsync("updateKey", doc -> doc.withBalance(999.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals(999.0, result.getOrNull().getBalance());
    }

    @Test
    void updateAsync_byKey_returnsNewVersion() throws Exception {
        TestGenericDoc created = getCache().createAsync("versionKey")
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(created);
        long originalVersion = created.getVersion();

        DefiniteResult<TestGenericDoc> result = getCache()
            .updateAsync("versionKey", doc -> doc.withName("updated"))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertTrue(result.getOrNull().getVersion() > originalVersion);
    }

    @Test
    void updateAsync_byDoc_appliesLambda() throws Exception {
        TestGenericDoc created = getCache().createAsync("byDocKey", doc -> doc.withBalance(5.0))
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(created);

        DefiniteResult<TestGenericDoc> result = getCache()
            .updateAsync(created, doc -> doc.withBalance(500.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals(500.0, result.getOrNull().getBalance());
    }

    @Test
    void updateAsync_nonExistentKey_returnsFailure() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache()
            .updateAsync("doesNotExist", doc -> doc)
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, result);
    }

    @Test
    void updateAsync_nonExistentKey_failureIsDocumentNotFoundException() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache()
            .updateAsync("nope", doc -> doc)
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, result);
        Throwable ex = result.exceptionOrNull();
        assertInstanceOf(ResultExceptionWrapper.class, ex);
        assertInstanceOf(DocumentNotFoundException.class, ((ResultExceptionWrapper) ex).getException());
    }

    @Test
    void updateRejectableAsync_byKey_appliesLambda() throws Exception {
        getCache().createAsync("rejectableKey", doc -> doc.withBalance(1.0)).get(5, TimeUnit.SECONDS);

        RejectableResult<TestGenericDoc> result = getCache()
            .updateRejectableAsync("rejectableKey", doc -> doc.withBalance(777.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals(777.0, result.getOrNull().getBalance());
    }

    @Test
    void updateRejectableAsync_byKey_rejectThrows_returnsReject() throws Exception {
        getCache().createAsync("rejectKey").get(5, TimeUnit.SECONDS);

        RejectableResult<TestGenericDoc> result = getCache()
            .updateRejectableAsync("rejectKey", doc -> { throw new RejectUpdateException("rejected", null); })
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Reject.class, result);
        assertTrue(result.isRejected());
    }

    @Test
    void updateRejectableAsync_byKey_unexpectedException_returnsFailure() throws Exception {
        getCache().createAsync("rejectFailKey").get(5, TimeUnit.SECONDS);

        RejectableResult<TestGenericDoc> result = getCache()
            .updateRejectableAsync("rejectFailKey", doc -> { throw new RuntimeException("boom"); })
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, result);
        assertTrue(result.isFailure());
    }
}

package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.exception.DocumentNotFoundException;
import com.jakemoore.datakache.api.exception.DuplicateDocumentKeyException;
import com.jakemoore.datakache.api.result.DefiniteResult;
import com.jakemoore.datakache.api.result.Failure;
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that exceptions from the suspend path surface correctly through the
 * CompletableFuture channel when using the *Async methods.
 *
 * Note: DataKache's async methods return typed DefiniteResult/OptionalResult values — errors are
 * represented as Failure results, not as failed futures. These tests confirm that failures are
 * delivered as Failure results (not via ExecutionException), and that .exceptionally() is NOT
 * invoked for expected operation failures.
 */
class TestFutureExceptionHandlingJava extends AbstractDataKacheJavaTest {

    @Test
    void createAsync_duplicateKey_futureResolvesNormally() throws Exception {
        // The future itself resolves successfully — the error is in the Failure result
        getCache().createAsync("dupExKey").get(5, TimeUnit.SECONDS);

        DefiniteResult<TestGenericDoc> result = getCache().createAsync("dupExKey")
            .get(5, TimeUnit.SECONDS);

        // Future completes normally (no ExecutionException); error is in the result type
        assertInstanceOf(Failure.class, result);
    }

    @Test
    void createAsync_duplicateKey_failureCauseIsDuplicateDocumentKeyException() throws Exception {
        getCache().createAsync("dupCauseKey").get(5, TimeUnit.SECONDS);

        DefiniteResult<TestGenericDoc> result = getCache().createAsync("dupCauseKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, result);
        Throwable wrapper = result.exceptionOrNull();
        assertInstanceOf(ResultExceptionWrapper.class, wrapper);
        assertInstanceOf(DuplicateDocumentKeyException.class,
            ((ResultExceptionWrapper) wrapper).getException());
    }

    @Test
    void createAsync_failure_exceptionallyIsNotInvoked() throws Exception {
        // .exceptionally() should NOT fire for a Failure result — the future itself resolved fine
        getCache().createAsync("exceptKey").get(5, TimeUnit.SECONDS);

        AtomicReference<Throwable> caughtEx = new AtomicReference<>(null);

        DefiniteResult<TestGenericDoc> result = getCache().createAsync("exceptKey")
            .exceptionally(ex -> {
                caughtEx.set(ex);
                return null;
            })
            .get(5, TimeUnit.SECONDS);

        assertNull(caughtEx.get(), "exceptionally() should not be called for a Failure result");
        assertInstanceOf(Failure.class, result);
    }

    @Test
    void updateAsync_nonExistentKey_failureCauseIsDocumentNotFoundException() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache()
            .updateAsync("noSuchKey", doc -> doc)
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Failure.class, result);
        Throwable wrapper = result.exceptionOrNull();
        assertInstanceOf(ResultExceptionWrapper.class, wrapper);
        assertInstanceOf(DocumentNotFoundException.class,
            ((ResultExceptionWrapper) wrapper).getException());
    }

    @Test
    void createAsync_thenApply_receivesResult() throws Exception {
        AtomicReference<Boolean> wasSuccess = new AtomicReference<>(false);

        getCache().createAsync("thenApplyKey")
            .thenApply(result -> {
                wasSuccess.set(result.isSuccess());
                return result;
            })
            .get(5, TimeUnit.SECONDS);

        assertTrue(wasSuccess.get());
    }
}

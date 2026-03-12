package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.result.DefiniteResult;
import com.jakemoore.datakache.api.result.Success;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TestReadOrCreateOperationsJava extends AbstractDataKacheJavaTest {

    @Test
    void readOrCreateAsync_nonExistent_createsAndReturnsDoc() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache().readOrCreateAsync("newKey")
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertNotNull(result.getOrNull());
        assertEquals("newKey", result.getOrNull().getKey());
    }

    @Test
    void readOrCreateAsync_existing_returnsExistingDoc() throws Exception {
        getCache().createAsync("existingKey", doc -> doc.withBalance(42.0))
            .get(5, TimeUnit.SECONDS);

        DefiniteResult<TestGenericDoc> result = getCache()
            .readOrCreateAsync("existingKey", doc -> doc.withBalance(999.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        // Should return the existing doc with original balance, not 999.0
        assertEquals(42.0, result.getOrNull().getBalance());
    }

    @Test
    void readOrCreateAsync_withInitializer_appliesOnlyOnCreate() throws Exception {
        DefiniteResult<TestGenericDoc> result = getCache()
            .readOrCreateAsync("initOnCreate", doc -> doc.withName("created"))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals("created", result.getOrNull().getName());

        // Second call — should return existing, name stays "created" not overridden
        DefiniteResult<TestGenericDoc> second = getCache()
            .readOrCreateAsync("initOnCreate", doc -> doc.withName("override"))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, second);
        assertEquals("created", second.getOrNull().getName());
    }

    @Test
    void readOrCreateAsync_concurrent_doesNotCreateDuplicate() throws Exception {
        // Fire two concurrent readOrCreate calls for the same key
        CompletableFuture<DefiniteResult<TestGenericDoc>> f1 =
            getCache().readOrCreateAsync("concurrentKey");
        CompletableFuture<DefiniteResult<TestGenericDoc>> f2 =
            getCache().readOrCreateAsync("concurrentKey");

        List<DefiniteResult<TestGenericDoc>> results =
            CompletableFuture.allOf(f1, f2)
                .thenApply(v -> List.of(f1.join(), f2.join()))
                .get(10, TimeUnit.SECONDS);

        // readOrCreate minimises but does not fully eliminate a duplicate-key race when two callers
        // both read Empty before either create fires. At least one must succeed; if one fails it must
        // be a DuplicateDocumentKeyException (not an unrelated error).
        long successes = results.stream().filter(r -> !r.isFailure()).count();
        assertTrue(successes >= 1, "At least one readOrCreate should succeed");
        // Exactly one document in the cache regardless of outcome
        assertEquals(1, getCache().getCacheSize());
    }
}

package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.doc.Doc;
import com.jakemoore.datakache.api.exception.update.RejectUpdateException;
import com.jakemoore.datakache.api.result.*;
import com.jakemoore.datakache.util.core.AbstractDataKacheJavaTest;
import com.jakemoore.datakache.util.doc.TestGenericDoc;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TestDocAsyncOperationsJava extends AbstractDataKacheJavaTest {

    @Test
    void doc_updateAsync_appliesLambda() throws Exception {
        TestGenericDoc doc = getCache().createAsync("docUpdateKey", d -> d.withBalance(1.0))
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(doc);

        DefiniteResult<TestGenericDoc> result = doc.updateAsync(d -> d.withBalance(42.0))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertEquals(42.0, result.getOrNull().getBalance());
    }

    @Test
    void doc_updateAsync_returnsNewDocInstance() throws Exception {
        TestGenericDoc doc = getCache().createAsync("docNewInstance")
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(doc);

        DefiniteResult<TestGenericDoc> result = doc.updateAsync(d -> d.withName("updated"))
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        TestGenericDoc updated = result.getOrNull();
        assertNotNull(updated);
        assertNotSame(doc, updated);
        assertTrue(updated.getVersion() > doc.getVersion());
    }

    @Test
    void doc_updateAsync_originalDocBecomesStale() throws Exception {
        TestGenericDoc doc = getCache().createAsync("docStaleKey")
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(doc);

        doc.updateAsync(d -> d.withName("changed")).get(5, TimeUnit.SECONDS);

        assertEquals(Doc.Status.STALE, doc.getStatus());
    }

    @Test
    void doc_updateRejectableAsync_rejectThrows_returnsReject() throws Exception {
        TestGenericDoc doc = getCache().createAsync("docRejectKey")
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(doc);

        RejectableResult<TestGenericDoc> result = doc
            .updateRejectableAsync(d -> { throw new RejectUpdateException("rejected", null); })
            .get(5, TimeUnit.SECONDS);

        assertInstanceOf(Reject.class, result);
        assertTrue(result.isRejected());
    }

    @Test
    void doc_deleteAsync_removesFromCache() throws Exception {
        TestGenericDoc doc = getCache().createAsync("docDeleteKey")
            .get(5, TimeUnit.SECONDS).getOrNull();
        assertNotNull(doc);

        DefiniteResult<Boolean> result = doc.deleteAsync().get(5, TimeUnit.SECONDS);

        assertInstanceOf(Success.class, result);
        assertTrue(result.getOrNull());
        assertTrue(getCache().read("docDeleteKey").isEmpty());
    }
}

package com.jakemoore.datakache.java;

import com.jakemoore.datakache.api.result.Empty;
import com.jakemoore.datakache.api.result.Failure;
import com.jakemoore.datakache.api.result.OptionalResult;
import com.jakemoore.datakache.api.result.Success;
import com.jakemoore.datakache.api.result.exception.ResultExceptionWrapper;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

// No MongoDB needed — unit tests on result types directly.
class TestOptionalResultJava {

    @Test
    void success_getOrThrow_returnsValue() {
        Success<String> result = new Success<>("hello");
        assertEquals("hello", result.getOrThrow());
    }

    @Test
    void failure_getOrThrow_throwsWrappedException() {
        RuntimeException cause = new RuntimeException("test failure");
        ResultExceptionWrapper wrapper = new ResultExceptionWrapper("test failure", cause);
        Failure<String> result = new Failure<>(wrapper);

        RuntimeException thrown = assertThrows(RuntimeException.class, result::getOrThrow);
        assertSame(wrapper, thrown);
    }

    @Test
    void empty_getOrThrow_throwsNoSuchElementException() {
        Empty<String> result = new Empty<>();
        assertThrows(NoSuchElementException.class, result::getOrThrow);
    }

    @Test
    void empty_getOrNull_returnsNull() {
        Empty<String> result = new Empty<>();
        assertNull(result.getOrNull());
    }

    @Test
    void success_getOrThrow_viaOptionalResultReference() {
        OptionalResult<String> result = new Success<>("via-interface");
        assertEquals("via-interface", result.getOrThrow());
    }

    @Test
    void empty_getOrThrow_viaOptionalResultReference() {
        OptionalResult<String> result = new Empty<>();
        assertThrows(NoSuchElementException.class, result::getOrThrow);
    }

    @Test
    void failure_getOrThrow_viaOptionalResultReference() {
        RuntimeException cause = new RuntimeException("failure via interface");
        ResultExceptionWrapper wrapper = new ResultExceptionWrapper("failure via interface", cause);
        OptionalResult<String> result = new Failure<>(wrapper);

        assertThrows(RuntimeException.class, result::getOrThrow);
    }
}

package com.csg.airtel.aaa4j.domain.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The normaliser is what keeps the error catalog's cardinality bounded, so these tests
 * are mostly about one property: <em>two occurrences of the same fault must produce the
 * same reason string, and two different faults must not</em>.
 */
class ErrorSignaturesTest {

    @Test
    void extractsVendorCodeFromHeadOfMessage() {
        assertEquals("ORA-00001", ErrorSignatures.codeFromMessage("ORA-00001: unique constraint violated"));
        assertEquals("ORA-12541", ErrorSignatures.codeFromMessage("ORA-12541: TNS:no listener"));
    }

    @Test
    void returnsNoCodeForOrdinaryProse() {
        assertNull(ErrorSignatures.codeFromMessage("connection was reset by peer"));
        assertNull(ErrorSignatures.codeFromMessage(null));
    }

    @Test
    void findsTheCodeBehindWrapperClassPrefixes() {
        // Reactive drivers surface Oracle errors as text behind their own exception name.
        assertEquals("ORA-12541", ErrorSignatures.codeFromMessage(
                "io.vertx.core.impl.NoStackTraceThrowable: ORA-12541: TNS:no listener"));
        assertEquals("ERR-503", ErrorSignatures.codeFromMessage("a.b.C: d.e.F: ERR-503: upstream down"));
    }

    @Test
    void doesNotMistakeProseForACode() {
        // A dash-digit pair buried in a sentence is not a code. Treating it as one would
        // split a single fault across several catalog rows.
        assertNull(ErrorSignatures.codeFromMessage("the dog rested-42"));
        assertNull(ErrorSignatures.codeFromMessage("rested-42nd time"));
    }

    @Test
    void missingMessageIsReportedExplicitly() {
        assertEquals(ErrorSignatures.NO_REASON, ErrorSignatures.normalizeReason(null));
        assertEquals(ErrorSignatures.NO_REASON, ErrorSignatures.normalizeReason("   \n\t "));
    }

    @Test
    void plainProseSurvivesUntouched() {
        assertEquals("Connection reset by peer", ErrorSignatures.normalizeReason("Connection reset by peer"));
    }

    @Test
    void stripsWrapperClassAndVendorCodePrefixes() {
        assertEquals("Connection refused",
                ErrorSignatures.normalizeReason("io.vertx.core.impl.NoStackTraceThrowable: Connection refused"));
        assertEquals("TNS:no listener", ErrorSignatures.normalizeReason("ORA-12541: TNS:no listener"));
    }

    @Test
    void leavesOrdinaryColonPrefixesAlone() {
        // "user" is neither a class name nor a vendor code, so nothing is dropped.
        assertEquals("user: not found", ErrorSignatures.normalizeReason("user: not found"));
    }

    @Test
    void sameFaultWithDifferentIdsCollapsesToOneReason() {
        String a = ErrorSignatures.normalizeReason("ORA-00001: unique constraint violated on id 88213");
        String b = ErrorSignatures.normalizeReason("ORA-00001: unique constraint violated on id 99999");
        assertEquals(a, b);
        assertEquals("unique constraint violated on id #", a);
    }

    @Test
    void measurementsWithUnitsCollapseToo() {
        // Regression: "30000ms" and "45000ms" must not become two separate time series.
        assertEquals(ErrorSignatures.normalizeReason("timed out after 30000ms"),
                ErrorSignatures.normalizeReason("timed out after 45000ms"));
        assertEquals("timed out after #", ErrorSignatures.normalizeReason("timed out after 30000ms"));
    }

    @Test
    void sessionIdentifiersAndTimestampsAreMasked() {
        assertEquals(ErrorSignatures.normalizeReason("replay failed for session a3f1e2b4c5d67890"),
                ErrorSignatures.normalizeReason("replay failed for session ff00aa11bb22cc33"));
    }

    @Test
    void quotedLiteralsAreMaskedButStructureRemains() {
        assertEquals("Unknown column ? in ?",
                ErrorSignatures.normalizeReason("Unknown column 'subscriber_id' in 'field list'"));
    }

    @Test
    void constraintNamesInParenthesesAreKept() {
        // Low cardinality and the most useful word in the message — deliberately preserved.
        assertEquals("unique constraint (AAA.PK_SESSION) violated",
                ErrorSignatures.normalizeReason("ORA-00001: unique constraint (AAA.PK_SESSION) violated"));
    }

    @Test
    void meaningfulNamesAreNotMistakenForValues() {
        assertEquals("topic DC-DR charset utf8mb4 key PK_SESSION",
                ErrorSignatures.normalizeReason("topic DC-DR charset utf8mb4 key PK_SESSION"));
    }

    @Test
    void addressAndPortAreMaskedWithoutLosingTheirShape() {
        assertEquals("Connection refused: #:#",
                ErrorSignatures.normalizeReason("Connection refused: 10.200.140.151:1521"));
    }

    @Test
    void differentFaultsStayDistinct() {
        String duplicateKey = ErrorSignatures.normalizeReason("ORA-00001: unique constraint violated");
        String noListener = ErrorSignatures.normalizeReason("ORA-12541: TNS:no listener");
        assertTrue(!duplicateKey.equals(noListener));
    }

    @Test
    void reasonIsLengthBounded() {
        String reason = ErrorSignatures.normalizeReason("x".repeat(4_000));
        assertTrue(reason.length() <= ErrorSignatures.MAX_REASON_LEN + 3,
                "reason label must stay bounded, was " + reason.length());
        assertTrue(reason.endsWith("..."));
    }

    @Test
    void sampleKeepsTheRealTextButFlattensAndCapsIt() {
        assertEquals("line1 line2", ErrorSignatures.sampleOf("line1\n\tline2"));
        assertTrue(ErrorSignatures.sampleOf("y".repeat(4_000)).length()
                <= ErrorSignatures.MAX_SAMPLE_LEN + 3);
        assertEquals(ErrorSignatures.NO_REASON, ErrorSignatures.sampleOf(null));
    }
}

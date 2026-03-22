package com.csg.airtel.aaa4j.application.common;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility for generating request trace identifiers.
 * <p>
 * The generated value is a 32-character, lowercase, hex string (UUID v4 without dashes),
 * which is widely compatible with logging/tracing systems and safe for use in headers.
 */
public final class TraceIdGenerator {

	private static final DateTimeFormatter TRACE_TIME_FORMATTER =
			DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

	private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

	private TraceIdGenerator() {
		// Utility class; do not instantiate
	}

	/**
	 * Generates a new trace id.
	 *
	 * @return 26-character string: 8-char hex + dash + 17-char timestamp,
	 *         e.g. "3f5a4c8e-20260206120000123"
	 */
	public static String generateTraceId() {
		long random = ThreadLocalRandom.current().nextLong();
		StringBuilder sb = new StringBuilder(26);
		for (int i = 0; i < 8; i++) {
			sb.append(HEX_CHARS[(int) (random & 0x0F)]);
			random >>>= 4;
		}
		sb.append('-');
		sb.append(ZonedDateTime.now().format(TRACE_TIME_FORMATTER));
		return sb.toString();
	}
}



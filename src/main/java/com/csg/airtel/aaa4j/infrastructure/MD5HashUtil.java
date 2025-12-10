package com.csg.airtel.aaa4j.infrastructure;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for MD5 hashing operations.
 *
 * This utility is specifically designed for hashing CHAP passwords
 * for CSV export purposes.
 */
@ApplicationScoped
public class MD5HashUtil {

    private static final Logger log = Logger.getLogger(MD5HashUtil.class);
    private static final String CHAP_PREFIX = "CHAP-";

    /**
     * Convert a string to its MD5 hash representation.
     *
     * @param input The string to hash
     * @return The MD5 hash as a lowercase hexadecimal string
     */
    public String toMD5(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            log.errorf(e, "MD5 algorithm not available");
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Hash a password if it's a CHAP password, otherwise return as-is.
     *
     * CHAP passwords start with "CHAP:" prefix. The value after the prefix
     * is hashed using MD5, and the result is returned with the "CHAP:" prefix
     * followed by the MD5 hash.
     *
     * @param password The password to process
     * @return The password with CHAP value hashed, or original password for non-CHAP
     */
    public String hashIfChap(String password) {
        if (password == null || password.isEmpty()) {
            return password;
        }

        if (password.startsWith(CHAP_PREFIX)) {
            // Extract the CHAP value (everything after "CHAP:")
            String chapValue = password.substring(CHAP_PREFIX.length());
            // Hash only the CHAP value and return with prefix
            String hashedValue = toMD5(chapValue);
            return CHAP_PREFIX + hashedValue;
        }

        // Return MAC and PAP passwords unchanged
        return password;
    }

    /**
     * Check if a password is a CHAP password.
     *
     * @param password The password to check
     * @return true if the password is a CHAP password
     */
    public boolean isChapPassword(String password) {
        return password != null && password.startsWith(CHAP_PREFIX);
    }

    /**
     * Convert byte array to hexadecimal string.
     *
     * @param bytes The byte array to convert
     * @return The hexadecimal representation as a lowercase string
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}

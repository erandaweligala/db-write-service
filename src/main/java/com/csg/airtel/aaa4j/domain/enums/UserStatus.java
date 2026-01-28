package com.csg.airtel.aaa4j.domain.enums;

public enum UserStatus {
    ACTIVE(1),
    SUSPENDED(2),
    INACTIVE(3);

    private final int code;

    UserStatus(int code) { this.code = code; }

    public int getCode() { return code; }

    public static UserStatus fromCode(int code) {
        for (UserStatus s : values()) {
            if (s.code == code) return s;
        }
        throw new IllegalArgumentException("Invalid status code: " + code);
    }
}

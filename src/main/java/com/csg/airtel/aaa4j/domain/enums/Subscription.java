package com.csg.airtel.aaa4j.domain.enums;

public enum Subscription {
    PREPAID(0),
    POSTPAID(1),
    HYBRID(2);

    private final int code;

    Subscription(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static Subscription fromCode(int code) {
        for (Subscription mode : values()) {
            if (mode.code == code) return mode;
        }
        throw new IllegalArgumentException("Invalid paid mode code: " + code);
    }
}


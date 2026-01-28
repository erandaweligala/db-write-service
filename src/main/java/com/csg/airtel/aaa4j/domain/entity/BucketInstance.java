package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "BUCKET_INSTANCE")
public class BucketInstance {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "bucket_instance_seq")
    @SequenceGenerator(name = "bucket_instance_seq", sequenceName = "BUCKET_INSTANCE_SEQ", allocationSize = 1)
    private Long id;

    @Column(name = "BUCKET_ID", nullable = false, length = 64)
    private String bucketId;

    @Column(name = "SERVICE_ID", length = 64)
    private Long serviceId;

    @Column(name = "BUCKET_TYPE")
    private String bucketType;

    @Column(name = "RULE", nullable = false, length = 64)
    private String rule;

    @Column(name = "PRIORITY", nullable = false, length = 64)
    private Long priority;

    @Column(name = "INITIAL_BALANCE", nullable = false, length = 64)
    private Long initialBalance;

    @Column(name = "CURRENT_BALANCE", nullable = false, length = 64)
    private Long currentBalance;

    @Column(name = "USAGE", nullable = false, length = 64)
    private Long usage;

    @Column(name = "CARRY_FORWARD", nullable = false)
    private Boolean carryForward;

    @Column(name = "MAX_CARRY_FORWARD")
    private Long maxCarryForward;

    @Column(name = "TOTAL_CARRY_FORWARD")
    private Long totalCarryForward;

    @Column(name = "CARRY_FORWARD_VALIDITY")
    private Integer carryForwardValidity;

    @Column(name = "TIME_WINDOW", nullable = false, length = 64)
    private String timeWindow;

    @Column(name = "CONSUMPTION_LIMIT")
    private Long consumptionLimit;

    @Column(name = "CONSUMPTION_LIMIT_WINDOW")
    private String consumptionLimitWindow;

    @Column(name = "EXPIRATION")
    private LocalDateTime expiration;

    @Column(name = "UPDATED_AT")
    @UpdateTimestamp
    private LocalDateTime updatedAt;

    @Column(name="IS_UNLIMITED")
    private Boolean isUnlimited;
}

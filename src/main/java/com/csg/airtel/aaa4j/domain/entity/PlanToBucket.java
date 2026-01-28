package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "PLAN_TO_BUCKET",
        uniqueConstraints = {
                @UniqueConstraint(name = "UK_PLAN_BUCKET", columnNames = {"PLAN_ID", "BUCKET_ID"})
        },
        indexes = {
                @Index(name = "IDX_PLAN_ID", columnList = "PLAN_ID"),
                @Index(name = "IDX_BUCKET_ID", columnList = "BUCKET_ID")
        })
public class PlanToBucket {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "plan_to_bucket_seq")
    @SequenceGenerator(
            name = "plan_to_bucket_seq",
            sequenceName = "PLAN_TO_BUCKET_SEQ",
            allocationSize = 1
    )
    @Column(name = "ID")
    private Long id;

    @Column(name = "PLAN_ID", nullable = false, length = 100)
    private String planId;

    @Column(name = "BUCKET_ID", nullable = false, length = 100)
    private String bucketId;

    @Column(name = "IS_UNLIMITED")
    private Boolean isUnlimited = false;

    @Column(name = "INITIAL_QUOTA")
    private Long initialQuota;

    @Column(name = "CARRY_FORWARD")
    private Boolean carryForward = false;

    @Column(name = "MAX_CARRY_FORWARD")
    private Long maxCarryForward;

    @Column(name = "TOTAL_CARRY_FORWARD")
    private Long totalCarryForward;

    @Column(name="CARRY_FORWARD_VALIDITY", length = 64)
    private Integer carryForwardValidity;

    @Column(name = "CONSUMPTION_LIMIT")
    private Long consumptionLimit;

    @Column(name = "CONSUMPTION_LIMIT_WINDOW", length = 64)
    private String consumptionLimitWindow;

    @Column(name = "CREATED_AT", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "UPDATED_AT", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    public void prePersist() {
        this.carryForward = this.carryForward != null && this.carryForward;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}

package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "PLAN")
public class Plan {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "plan_seq")
    @SequenceGenerator(
            name = "plan_seq",
            sequenceName = "PLAN_SEQ",
            allocationSize = 1
    )
    @Column(name = "ID")
    private Long id;

    @Column(name = "PLAN_ID", length = 64, nullable = false)
    private String planId;

    @Column(name = "PLAN_NAME", length = 64, nullable=false)
    private String planName;

    @Column(name = "PLAN_TYPE", length = 64, nullable = false)
    private String planType;

    @Column(name = "RECURRING_FLAG", nullable = false)
    private Boolean recurringFlag;

    @Column(name = "RECURRING_PERIOD", length = 64)
    private String recurringPeriod;

    @Column(name = "STATUS", length = 64, nullable = false)
    private String status;

    @Column(name = "CONNECTION_TYPE", length = 64, nullable = false)
    private String connectionType;

    @Column(name = "CREATED_AT", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "UPDATED_AT", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "QUOTA_PRORATION_FLAG", nullable = false)
    private Boolean quotaProrationFlag;
}

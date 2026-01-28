package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

@Entity
@Table(name = "SERVICE_INSTANCE")
public class ServiceInstance {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "service_instance_seq")
    @SequenceGenerator(name = "service_instance_seq", sequenceName = "SERVICE_INSTANCE_SEQ", allocationSize = 1)
    private Long id;

    @Column(name = "PLAN_ID", length = 64, nullable = false)
    private String planId;

    @Column(name = "PLAN_NAME", length = 64, nullable = false)
    private String planName;

    @Column(name = "PLAN_TYPE", length = 64, nullable = false)
    private String planType;

    @JdbcTypeCode(SqlTypes.INTEGER)
    @Column(name = "RECURRING_FLAG",nullable = false)
    private Boolean recurringFlag;

    @Column(name = "USERNAME", length = 64, nullable = false)
    private String username;

    @Column(name = "CYCLE_START_DATE")
    private LocalDateTime serviceCycleStartDate;

    @Column(name = "CYCLE_END_DATE")
    private LocalDateTime serviceCycleEndDate;

    @Column(name = "NEXT_CYCLE_START_DATE")
    private LocalDateTime nextCycleStartDate;

    @Column(name = "SERVICE_START_DATE",nullable = false)
    private LocalDateTime serviceStartDate;

    @Column(name = "EXPIRY_DATE")
    private LocalDateTime expiryDate;

    @Column(name = "STATUS", length = 64, nullable = false)
    private String status;

    @Column(name = "CREATED_AT", nullable = false)
    @CreationTimestamp
    private LocalDateTime createdAt;

    @Column(name = "UPDATED_AT", nullable = false)
    @UpdateTimestamp
    private LocalDateTime updatedAt;

    @Column(name = "REQUEST_ID",nullable = false)
    private String requestId;

    @JdbcTypeCode(SqlTypes.INTEGER)
    @Column(name = "IS_GROUP")
    private Boolean isGroup;
    @Column(name="CYCLE_DATE")
    private Integer cycleDate;
    @Column(name="BILLING")
    private String billing;
}

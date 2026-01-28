package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "CHILD_TEMPLATE_TABLE",
        indexes = {
                @Index(name = "IDX_SUPER_TEMPLATE_ID", columnList = "SUPER_TEMPLATE_ID"),
                @Index(name = "IDX_MESSAGE_TYPE", columnList = "MESSAGE_TYPE")
        })
public class ChildTemplate {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "child_template_seq")
    @SequenceGenerator(
            name = "child_template_seq",
            sequenceName = "CHILD_TEMPLATE_SEQ",
            allocationSize = 1
    )
    @Column(name = "ID")
    private Long id;

    @Column(name = "MESSAGE_TYPE", nullable = false, length = 20)
    private String messageType;

    @Column(name = "DAYS_TO_EXPIRE")
    private Integer daysToExpire;

    @Column(name = "QUOTA_PERCENTAGE")
    private Integer quotaPercentage;

    @Column(name = "MESSAGE_CONTENT", nullable = false, columnDefinition = "CLOB")
    private String messageContent;

    @Column(name = "SUPER_TEMPLATE_ID", nullable = false)
    private Long superTemplateId;

    @Column(name = "CREATED_AT", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "UPDATED_AT", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}

package com.csg.airtel.aaa4j.domain.entity;

import com.csg.airtel.aaa4j.domain.enums.TemplateStatus;
import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "SUPER_TEMPLATE_TABLE",
        uniqueConstraints = {
                @UniqueConstraint(name = "UK_TEMPLATE_NAME", columnNames = "TEMPLATE_NAME")
        },
        indexes = {
                @Index(name = "IDX_TEMPLATE_NAME", columnList = "TEMPLATE_NAME"),
                @Index(name = "IDX_STATUS", columnList = "STATUS")
        })
public class SuperTemplate {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "super_template_seq")
    @SequenceGenerator(
            name = "super_template_seq",
            sequenceName = "SUPER_TEMPLATE_SEQ",
            allocationSize = 1
    )
    @Column(name = "ID")
    private Long id;

    @Column(name = "TEMPLATE_NAME", nullable = false, unique = true, length = 255)
    private String templateName;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private TemplateStatus status;

    @Column(name = "IS_DEFAULT", nullable = false)
    private Boolean isDefault = false;

    @Column(name = "CREATED_BY", nullable = false, length = 100)
    private String createdBy;

    @Column(name = "CREATED_AT", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "UPDATED_BY", length = 100)
    private String updatedBy;

    @Column(name = "UPDATED_AT", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    public void prePersist() {
        this.isDefault = this.isDefault != null && this.isDefault;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}

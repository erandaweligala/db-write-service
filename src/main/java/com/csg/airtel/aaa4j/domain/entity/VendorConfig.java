package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "VENDOR_CONFIG_TABLE")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VendorConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "vendor_config_seq")
    @SequenceGenerator(
            name = "vendor_config_seq",
            sequenceName = "VENDOR_CONFIG_SEQ",
            allocationSize = 1
    )
    private Long id;

    @Column(name = "VENDOR_ID", nullable = false)
    private String vendorId;

    @Column(name = "VENDOR_NAME", length = 255, nullable = false)
    private String vendorName;

    @Column(name = "ATTRIBUTE_NAME", length = 255, nullable = false)
    private String attributeName;

    @Column(name = "ATTRIBUTE_ID", nullable = false)
    private String attributeId;

    @Column(name = "VALUE_PATH", length = 500)
    private String valuePath;

    @Column(name = "ENTITY", length = 255)
    private String entity;

    @Column(name = "DATA_TYPE", length = 100)
    private String dataType;

    @Column(name = "PARAMETER", length = 255)
    private String parameter;

    @Column(name = "IS_ACTIVE", nullable = false)
    private Boolean isActive = true;

    @Column(name = "ATTRIBUTE_PREFIX", length = 100)
    private String attributePrefix;

    @Column(name = "CREATED_DATE", updatable = false)
    private LocalDateTime createdDate;

    @Column(name = "CREATED_BY", length = 100)
    private String createdBy;

    @Column(name = "LAST_UPDATED_DATE")
    private LocalDateTime lastUpdatedDate;

    @Column(name = "LAST_UPDATED_BY", length = 100)
    private String lastUpdatedBy;

    @PrePersist
    protected void onCreate() {
        createdDate = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        lastUpdatedDate = LocalDateTime.now();
    }
}
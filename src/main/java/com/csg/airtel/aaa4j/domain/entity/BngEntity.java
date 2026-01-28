package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "bng")
public class BngEntity {
    @Id
    @Column(name = "bng_id", nullable = false, unique = true)
    private String bngId;  // No auto-generation, must be provided

    @Column(name = "bng_name", nullable = false, unique = true)
    private String bngName;

    @Column(name = "bng_ip", nullable = false)
    private String bngIp;

    @Column(name = "bng_type_vendor", nullable = false)
    private String bngTypeVendor;

    @Column(name = "model_version", nullable = false)
    private String modelVersion;

    @Column(name = "nas_ip_address", nullable = false)
    private String nasIpAddress;

    @Column(name = "nas_identifier", nullable = false)
    private String nasIdentifier;

    @Column(name = "coa_ip", nullable = false)
    private String coaIp;

    @Column(name = "coa_port", nullable = false)
    private Integer coaPort;

    @Column(name = "shared_secret", nullable = false)
    private String sharedSecret;

    @Column(name = "location", nullable = false)
    private String location;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name="created_by", nullable = false)
    private String createdBy;

    @Column(name="updated_by")
    private String updatedBy;

    @Column(name = "created_date", nullable = false, updatable = false)
    private LocalDateTime createdDate;

    @Column(name = "updated_date")
    private LocalDateTime updatedDate;

    @PrePersist
    protected void onCreate() {
        createdDate = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedDate = LocalDateTime.now();
    }
}

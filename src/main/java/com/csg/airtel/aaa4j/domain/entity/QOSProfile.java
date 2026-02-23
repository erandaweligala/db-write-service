package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "QOS_PROFILE")
public class QOSProfile {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "qos_profile_seq")
    @SequenceGenerator(
            name = "qos_profile_seq",
            sequenceName = "QOS_PROFILE_SEQ",
            allocationSize = 1
    )
    private Long id;

    @Column(name = "BNG_CODE", length = 255, nullable = false)
    private String bngCode;

    @Column(name = "QOS_PROFILE_NAME", length = 255, nullable = false)
    private String qosProfileName;

    @Column(name = "UPLINK_SPEED", length = 255, nullable = false)
    private String upLink;

    @Column(name = "DOWNLINK_SPEED", length = 255, nullable = false)
    private String downLink;

    @Column(name = "IS_DEFAULT", nullable = false)
    private Boolean isDefault;

    @Column(name = "CREATED_DATE", updatable = false)
    private LocalDateTime createdDate;

    @Column(name = "UPDATED_DATE")
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
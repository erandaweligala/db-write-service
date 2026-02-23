package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "BUCKET")
public class Bucket {
    @Id
    @Column(name = "BUCKET_ID", length = 64, nullable = false)
    private String bucketId;

    @Column(name = "BUCKET_NAME", length = 64, nullable = false)
    private String bucketName;

    @Column(name = "BUCKET_TYPE", length = 64, nullable = false)
    private String bucketType;

    @Column(name = "QOS_ID",nullable = false)
    private Long qosId;

    @Column(name = "PRIORITY",nullable = false)
    private Long priority;

    @Column(name = "TIME_WINDOW")
    private String timeWindow;

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

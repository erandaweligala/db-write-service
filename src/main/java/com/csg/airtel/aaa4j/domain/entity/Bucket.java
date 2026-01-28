package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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
}

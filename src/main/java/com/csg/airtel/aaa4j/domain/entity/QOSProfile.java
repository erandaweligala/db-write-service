package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

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

    @Column(name = "IS_DEFAULT",nullable = false)
    private Boolean isDefault;
}

package com.csg.airtel.aaa4j.domain.entity;

import com.csg.airtel.aaa4j.domain.enums.Subscription;
import com.csg.airtel.aaa4j.domain.enums.UserStatus;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.Random;

@Entity
@Table(name = "AAA_USER")
public class UserEntity {
    @Id
    @Column(name = "USER_ID", nullable = false, unique = true, length = 50)
    private String userId;

    @Column(name = "USER_NAME", nullable = false, unique = true)
    private String userName;

    @Column(name = "PASSWORD")
    private String password;

    @Column(name = "ENCRYPTION_METHOD")
    private Integer encryptionMethod;
    /*
        0 – Plain text
        1 – MD5
        2 – CSG/ADL proprietary
        Mandatory only if password is provided
     */

    @Column(name = "NAS_PORT_TYPE", nullable = false)
    private String nasPortType;

    @Column(name = "GROUP_ID")
    private String groupId;

    @Column(name = "BANDWIDTH")
    private String bandwidth;

    @Column(name = "VLAN_ID")
    private String vlanId;

    @Column(name = "CIRCUIT_ID")
    private String circuitId;

    @Column(name = "REMOTE_ID")
    private String remoteId;

    @Transient
    private String macAddress; // For maintaining API compatibility

    @Column(name = "IP_ALLOCATION")
    private String ipAllocation;

    @Column(name = "IP_POOL_NAME")
    private String ipPoolName;

    @Column(name = "IPV4")
    private String ipv4;

    @Column(name = "IPV6")
    private String ipv6;

    @Column(name = "BILLING")
    private String billing;

    @Column(name = "CYCLE_DATE")
    private Integer cycleDate;

    @Column(name = "CONTACT_NAME")
    private String contactName;

    @Column(name = "CONTACT_EMAIL")
    private String contactEmail;

    @Column(name = "CONTACT_NUMBER")
    private String contactNumber;

    @Column(name = "CONCURRENCY")
    private Integer concurrency;

    @Column(name = "BILLING_ACCOUNT_REF")
    private String billingAccountRef;

    @Column(name = "SESSION_TIMEOUT")
    private String sessionTimeout;

    @Enumerated(EnumType.STRING)
    @Column(name = "subscription")
    private Subscription subscription;

    @Enumerated(EnumType.STRING)
    @Column(name = "STATUS", nullable = false)
    private UserStatus status;
    // Change from Integer to UserStatus
    /*
        1 – Active
        2 – Suspended
        3 – Inactive
     */

    @Column(name = "REQUEST_ID", nullable = false, unique = true)
    private String requestId;

    @Column(name = "TEMPLATE_ID")
    private Long templateId;

    @Transient // Not stored in DB, populated from join
    private String templateName;

    @Column(name = "CREATED_DATE", nullable = false, updatable = false)
    @CreationTimestamp
    private LocalDateTime createdDate;

    @Column(name = "UPDATED_DATE")
    private LocalDateTime updatedDate;

    @PrePersist
    public void generateUserId() {
        if (this.userId == null || this.userId.isEmpty()) {
            this.userId = generateIncrementalRandomId();
        }
    }

    private static final Random RANDOM = new Random();

    private String generateIncrementalRandomId() {
        long timestamp = System.currentTimeMillis();
        int random = RANDOM.nextInt(10_000);
        return String.format("USR%d%04d", timestamp, random);
    }
}

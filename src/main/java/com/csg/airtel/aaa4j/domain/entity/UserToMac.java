package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "AAA_USER_MAC_ADDRESS")
public class UserToMac {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "mac_seq")
    @SequenceGenerator(name = "mac_seq", sequenceName = "AAA_USER_MAC_SEQ", allocationSize = 1)
    @Column(name = "ID")
    private Long id;

    @Column(name = "USER_NAME", nullable = false)
    private String userName;

    @Column(name = "MAC_ADDRESS", nullable = false)
    private String macAddress; // Normalized format (no separators, lowercase)

    @Column(name = "ORIGINAL_MAC_ADDRESS", nullable = false)
    private String originalMacAddress; // User-provided format

    @Column(name = "CREATED_DATE", nullable = false, updatable = false)
    @CreationTimestamp
    private LocalDateTime createdDate;

    @Column(name = "UPDATED_DATE")
    private LocalDateTime updatedDate;
}

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

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "USER_NAME", referencedColumnName = "USER_NAME", nullable = false)
    private UserEntity user;

    @Column(name = "MAC_ADDRESS", nullable = false)
    private String macAddress; // Normalized format (no separators, lowercase)

    @Column(name = "ORIGINAL_MAC_ADDRESS", nullable = false)
    private String originalMacAddress; // User-provided format

    @Column(name = "CREATED_DATE", nullable = false, updatable = false)
    @CreationTimestamp
    private LocalDateTime createdDate;

    @Column(name = "UPDATED_DATE")
    private LocalDateTime updatedDate;

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UserEntity getUser() {
        return user;
    }

    public void setUser(UserEntity user) {
        this.user = user;
    }

    // Convenience method to get userName from the associated user
    public String getUserName() {
        return user != null ? user.getUserName() : null;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public String getOriginalMacAddress() {
        return originalMacAddress;
    }

    public void setOriginalMacAddress(String originalMacAddress) {
        this.originalMacAddress = originalMacAddress;
    }

    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDateTime createdDate) {
        this.createdDate = createdDate;
    }

    public LocalDateTime getUpdatedDate() {
        return updatedDate;
    }

    public void setUpdatedDate(LocalDateTime updatedDate) {
        this.updatedDate = updatedDate;
    }
}

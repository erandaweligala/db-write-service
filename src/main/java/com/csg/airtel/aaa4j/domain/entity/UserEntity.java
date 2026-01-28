package com.csg.airtel.aaa4j.domain.entity;

import com.csg.airtel.aaa4j.domain.enums.Subscription;
import com.csg.airtel.aaa4j.domain.enums.UserStatus;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.List;
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

    @Column(name = "IDLE_TIMEOUT")
    private String idleTimeout;

    @Column(name = "CUSTOM_TIMEOUT")
    private String customTimeout;

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

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<UserToMac> macAddresses;

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

    // Getters and Setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getEncryptionMethod() {
        return encryptionMethod;
    }

    public void setEncryptionMethod(Integer encryptionMethod) {
        this.encryptionMethod = encryptionMethod;
    }

    public String getNasPortType() {
        return nasPortType;
    }

    public void setNasPortType(String nasPortType) {
        this.nasPortType = nasPortType;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(String bandwidth) {
        this.bandwidth = bandwidth;
    }

    public String getVlanId() {
        return vlanId;
    }

    public void setVlanId(String vlanId) {
        this.vlanId = vlanId;
    }

    public String getCircuitId() {
        return circuitId;
    }

    public void setCircuitId(String circuitId) {
        this.circuitId = circuitId;
    }

    public String getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(String remoteId) {
        this.remoteId = remoteId;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public String getIpAllocation() {
        return ipAllocation;
    }

    public void setIpAllocation(String ipAllocation) {
        this.ipAllocation = ipAllocation;
    }

    public String getIpPoolName() {
        return ipPoolName;
    }

    public void setIpPoolName(String ipPoolName) {
        this.ipPoolName = ipPoolName;
    }

    public String getIpv4() {
        return ipv4;
    }

    public void setIpv4(String ipv4) {
        this.ipv4 = ipv4;
    }

    public String getIpv6() {
        return ipv6;
    }

    public void setIpv6(String ipv6) {
        this.ipv6 = ipv6;
    }

    public String getBilling() {
        return billing;
    }

    public void setBilling(String billing) {
        this.billing = billing;
    }

    public Integer getCycleDate() {
        return cycleDate;
    }

    public void setCycleDate(Integer cycleDate) {
        this.cycleDate = cycleDate;
    }

    public String getContactName() {
        return contactName;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public String getContactEmail() {
        return contactEmail;
    }

    public void setContactEmail(String contactEmail) {
        this.contactEmail = contactEmail;
    }

    public String getContactNumber() {
        return contactNumber;
    }

    public void setContactNumber(String contactNumber) {
        this.contactNumber = contactNumber;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public String getBillingAccountRef() {
        return billingAccountRef;
    }

    public void setBillingAccountRef(String billingAccountRef) {
        this.billingAccountRef = billingAccountRef;
    }

    public String getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(String sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(String idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public String getCustomTimeout() {
        return customTimeout;
    }

    public void setCustomTimeout(String customTimeout) {
        this.customTimeout = customTimeout;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }

    public UserStatus getStatus() {
        return status;
    }

    public void setStatus(UserStatus status) {
        this.status = status;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Long getTemplateId() {
        return templateId;
    }

    public void setTemplateId(Long templateId) {
        this.templateId = templateId;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
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

    public List<UserToMac> getMacAddresses() {
        return macAddresses;
    }

    public void setMacAddresses(List<UserToMac> macAddresses) {
        this.macAddresses = macAddresses;
    }
}

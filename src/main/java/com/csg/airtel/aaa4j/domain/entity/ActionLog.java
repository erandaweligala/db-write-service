package com.csg.airtel.aaa4j.domain.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "ACTION_LOG")
public class ActionLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "ADMIN_USER")
    private String adminUser;

    @Column(name = "USER_NAME")
    private String userName;

    @Column(name = "GROUP_ID")
    private String groupId;

    @Column(name = "DATE_TIME")
    private LocalDateTime dateTime;

    @Column(name = "REQUEST_ID")
    private String requestId;

    @Column(name = "ACTION")
    private String action;

    @Column(name = "RESULT_CODE")
    private String resultCode;

    @Column(name = "HTTP_STATUS")
    private String httpStatus;

    @Column(name = "DESCRIPTION")
    private String description;
}

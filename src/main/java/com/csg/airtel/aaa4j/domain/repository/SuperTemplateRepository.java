package com.csg.airtel.aaa4j.domain.repository;

import com.csg.airtel.aaa4j.domain.entity.SuperTemplate;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.PersistenceUnit;

@ApplicationScoped
@PersistenceUnit(unitName = "oracle")
public class SuperTemplateRepository implements PanacheRepository<SuperTemplate> {
}

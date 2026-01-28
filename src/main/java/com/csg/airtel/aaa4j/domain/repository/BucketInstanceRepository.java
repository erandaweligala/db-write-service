package com.csg.airtel.aaa4j.domain.repository;

import com.csg.airtel.aaa4j.domain.entity.BucketInstance;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.PersistenceUnit;

@ApplicationScoped
@PersistenceUnit(unitName = "oracle")
public class BucketInstanceRepository implements PanacheRepository<BucketInstance> {
}

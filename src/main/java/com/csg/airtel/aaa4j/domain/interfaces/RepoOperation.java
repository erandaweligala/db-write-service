package com.csg.airtel.aaa4j.domain.interfaces;

import io.quarkus.hibernate.orm.panache.PanacheRepository;

@FunctionalInterface
public interface RepoOperation<E,R> {
    R apply(PanacheRepository<E> repository);
}

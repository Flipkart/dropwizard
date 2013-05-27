package com.yammer.dropwizard.hibernate;

import java.util.Set;

import javax.persistence.Entity;

import com.fasterxml.jackson.datatype.hibernate4.Hibernate4Module;
import com.google.common.collect.ImmutableList;
import com.yammer.dropwizard.ConfiguredBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.ConfigurationStrategy;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import org.hibernate.SessionFactory;
import org.reflections.Reflections;

public abstract class HibernateBundle<T extends Configuration> implements ConfiguredBundle<T>, ConfigurationStrategy<T> {
    private SessionFactory sessionFactory;

    private final ImmutableList<Class<?>> entities;
    private final SessionFactoryFactory sessionFactoryFactory;
    
    protected HibernateBundle(String packagePrefix) {
        Reflections reflections = new Reflections(packagePrefix);
        Set<Class<?>> result = reflections.getTypesAnnotatedWith(Entity.class);
        this.entities = ImmutableList.<Class<?>>builder().addAll(result).build();
        this.sessionFactoryFactory = new SessionFactoryFactory();
    }

    protected HibernateBundle(Class<?> entity, Class<?>... entities) {
        this(ImmutableList.<Class<?>>builder().add(entity).add(entities).build(),
             new SessionFactoryFactory());
    }

    protected HibernateBundle(ImmutableList<Class<?>> entities,
                              SessionFactoryFactory sessionFactoryFactory) {
        this.entities = entities;
        this.sessionFactoryFactory = sessionFactoryFactory;
    }

    @Override
    public final void initialize(Bootstrap<?> bootstrap) {
        bootstrap.getObjectMapperFactory().registerModule(new Hibernate4Module());
    }

    @Override
    public final void run(T configuration, Environment environment) throws Exception {
        final DatabaseConfiguration dbConfig = getDatabaseConfiguration(configuration);
        this.sessionFactory = sessionFactoryFactory.build(environment, dbConfig, entities);
        environment.addProvider(new UnitOfWorkResourceMethodDispatchAdapter(sessionFactory));
        environment.addHealthCheck(new SessionFactoryHealthCheck("hibernate",
                                                                 sessionFactory,
                                                                 dbConfig.getValidationQuery()));
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}

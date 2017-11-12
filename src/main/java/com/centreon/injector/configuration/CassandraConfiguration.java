package com.centreon.injector.configuration;

import static com.centreon.injector.configuration.EnvParams.*;
import static java.lang.Integer.parseInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.env.Environment;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

@Configuration
public class CassandraConfiguration {

    static final private Logger LOGGER = LoggerFactory.getLogger(CassandraConfiguration.class);

    @Bean(destroyMethod = "close")
    public Cluster getCluster(@Autowired Environment env) {
        LOGGER.info("Initializing DSE cluster object");

        final QueryOptions queryOptions = new QueryOptions();
        queryOptions.setFetchSize(parseInt(env.getProperty(DSE_CONNECTION_READ_FETCH_SIZE, DSE_CONNECTION_READ_FETCH_SIZE_DEFAULT)));
        queryOptions.setConsistencyLevel(ConsistencyLevel.valueOf(env.getProperty(DSE_CONNECTION_DEFAULT_CONSISTENCY, DSE_CONNECTION_DEFAULT_CONSISTENCY_DEFAULT)));
        final PlainTextAuthProvider authProvider = new PlainTextAuthProvider(
                env.getProperty(DSE_USERNAME, DSE_USERNAME_DEFAULT),
                env.getProperty(DSE_PASSWORD, DSE_PASSWORD_DEFAULT)
        );


        final TokenAwarePolicy loadBalancingPolicy = new TokenAwarePolicy(
                DCAwareRoundRobinPolicy.builder()
                        .withLocalDc(env.getProperty(DSE_LOCAL_DC, DSE_LOCAL_DC_DEFAULT))
                        .build(),
                true);


        final Cluster cluster = Cluster.builder()
                .addContactPoint(env.getProperty(DSE_CONTACT_POINT, DSE_CONTACT_POINT_DEFAULT))
                .withClusterName(env.getProperty(DSE_CLUSTER_NAME, DSE_CLUSTER_NAME_DEFAULT))
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                .withQueryOptions(queryOptions)
                .withAuthProvider(authProvider)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build();

        return cluster;
    }

    @Bean(destroyMethod = "close")
    public Session getSession(@Autowired Environment env, @Autowired Cluster dseCluster) {
        LOGGER.info("Initializing DSE Session object ");

        final Session session = dseCluster
                .connect(env.getProperty(DSE_KEYSPACE_NAME, DSE_KEYSPACE_NAME_DEFAULT));
        return session;
    }

    @Bean
    public DSETopology getTopology(@Autowired Environment env) {
        return new DSETopology(
                env.getProperty(DSE_KEYSPACE_NAME, DSE_KEYSPACE_NAME_DEFAULT),
                env.getProperty(DSE_LOCAL_DC, DSE_LOCAL_DC_DEFAULT)
        );
    }


    public static class DSETopology {
        final public String keyspace;
        final public String local_DC;

        public DSETopology(String keyspace, String local_DC) {
            this.keyspace = keyspace;
            this.local_DC = local_DC;
        }
    }

}

package com.centreon.injector.repository;

import com.centreon.injector.configuration.DSEConfiguration;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

public abstract class AbstractTestCassandraRepository {

    protected static final Session SESSION = CassandraEmbeddedServerBuilder
            .builder()
            .withScript("cassandra/schema.cql")
            .buildNativeSession();

    protected static final DSEConfiguration.DSETopology TOPOLOGY = new DSEConfiguration.DSETopology("centreon", "dc1");

    protected static void truncate(String tableWithKeyspace) {
        SESSION.execute("TRUNCATE " + tableWithKeyspace);
    }
}

package com.centreon.injector.data_access;

import static org.assertj.core.api.Assertions.*;

import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import com.centreon.injector.configuration.DSEConfiguration;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class MetaDataQueriesTest {

    private static final Session SESSION = CassandraEmbeddedServerBuilder
            .builder()
            .withScript("cassandra/schema.cql")
            .buildNativeSession();

    private static final ScriptExecutor SCRIPT_EXECUTOR = new ScriptExecutor(SESSION);
    private static final DSEConfiguration.DSETopology TOPOLOGY = new DSEConfiguration.DSETopology("centreon", "dc1");


    @Test
    public void should_get_service_for_id_metric() throws Exception {
        //Given
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric3 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final UUID service1 = new UUID(0, idMetric1);
        final UUID service2 = new UUID(0, idMetric2);

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/MetaDataQueries/insert_rows.cql",
                ImmutableMap.of("service1", service1,
                        "service2", service2,
                        "idMetric1", idMetric1,
                        "idMetric2", idMetric2,
                        "idMetric3", idMetric3));

        //When
        final MetaDataQueries metaDataQueries = new MetaDataQueries(SESSION, TOPOLOGY);

        //Then
        assertThat(metaDataQueries.getServiceIdForIdMetric(idMetric1)).isEqualTo(service1);
        assertThat(metaDataQueries.getServiceIdForIdMetric(idMetric2)).isEqualTo(service1);
        assertThat(metaDataQueries.getServiceIdForIdMetric(idMetric3)).isEqualTo(service2);
    }

}
package com.centreon.injector.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.UUID;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;

import com.centreon.injector.configuration.DSEConfiguration;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

public class RRDQueriesTest extends AbstractTestCassandraRepository {

    private static final RRDQueries RRD_QUERIES = new RRDQueries(SESSION, TOPOLOGY);

    @After
    public void cleanUp() {
        truncate("centreon.rrd_aggregated");
    }

    @Test
    public void should_insert_into_rrd_aggregated_for_hour() throws Exception {
        //Given
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final UUID service = new UUID(0, idMetric);
        final long hour = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final long previousTimeValue = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final float value = RandomUtils.nextFloat(0f, Float.MAX_VALUE);

        //When
        RRD_QUERIES.insertIntoRrdAggregatedForHour(service, hour, idMetric, previousTimeValue, value)
                .getUninterruptibly();

        //Then
        final Row found = SESSION.execute("SELECT * FROM centreon.rrd_aggregated WHERE " +
                "service = " + service.toString() +
                " AND aggregation_unit = 'HOUR'" +
                " AND time_value = " + hour).one();
        assertThat(found).isNotNull();
        assertThat(found.getLong("previous_time_value")).isEqualTo(previousTimeValue);
        assertThat(found.getFloat("min")).isEqualTo(value);
        assertThat(found.getFloat("max")).isEqualTo(value);
        assertThat(found.getFloat("sum")).isEqualTo(value);
        assertThat(found.getInt("count")).isEqualTo(1);
    }


}
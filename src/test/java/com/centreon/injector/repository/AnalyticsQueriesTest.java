package com.centreon.injector.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class AnalyticsQueriesTest extends AbstractTestCassandraRepository {

    private static final AnalyticsQueries ANALYTICS_QUERIES = new AnalyticsQueries(SESSION, TOPOLOGY);

    @After
    public void cleanUp() {
        truncate("centreon.analytics_aggregated");
    }

    @Test
    public void should_insert_into_analytics_aggregated() throws Exception {
        //Given
        final long hour = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final long cTime = new Date().getTime();
        final Float value = RandomUtils.nextFloat(0f, Float.MAX_VALUE);

        //When
        final ResultSetFuture future = ANALYTICS_QUERIES.insertIntoAnalyticsdAggregatedForHour(idMetric, hour, cTime, value);

        //Then
        future.getUninterruptibly();
        final Row found = SESSION.execute("SELECT * FROM centreon.analytics_aggregated WHERE " +
                " aggregation_unit='HOUR'" +
                " AND time_value = " + hour +
                " AND id_metric = " + idMetric +
                " AND previous_time_value = " + cTime).one();
        assertThat(found).isNotNull();
        assertThat(found.getLong("previous_time_value")).isEqualTo(cTime);
        assertThat(found.getFloat("min")).isEqualTo(value);
        assertThat(found.getFloat("max")).isEqualTo(value);
        assertThat(found.getFloat("sum")).isEqualTo(value);
        assertThat(found.getInt("count")).isEqualTo(1);
    }
}
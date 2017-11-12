package com.centreon.injector.data_access;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import com.centreon.injector.configuration.CassandraConfiguration.DSETopology;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

public class DatabinQueriesTest {

    private static final Session SESSION = CassandraEmbeddedServerBuilder
            .builder()
            .withScript("cassandra/schema.cql")
            .buildNativeSession();

    private static final DSETopology TOPOLOGY = new DSETopology("centreon", "dc1");
    private static final DatabinQueries DATABIN_QUERIES = new DatabinQueries(SESSION, TOPOLOGY);
    
    @Test
    public void should_insert_data_into_databin() throws Exception {
        //Given
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final long cTime = new Date().getTime();
        final Float value = RandomUtils.nextFloat(0f, Float.MAX_VALUE);
        final Integer status = 2;

        //When
        final ResultSetFuture future = DATABIN_QUERIES.insertIntoDatabin(idMetric, cTime, value, status);

        //Then
        future.getUninterruptibly();
        final Row found = SESSION.execute("SELECT * FROM centreon.databin WHERE id_metric = "
                + idMetric + " AND ctime = " + cTime).one();
        assertThat(found).isNotNull();
        assertThat(found.getLong("ctime")).isEqualTo(cTime);
        assertThat(found.getFloat("value")).isEqualTo(value);
        assertThat(found.getByte("status")).isEqualTo((byte)2);
    }

    @Test
    public void should_insert_into_databin_by_hour() throws Exception {
        //Given
        final long hour = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final long cTime = new Date().getTime();
        final Float value = RandomUtils.nextFloat(0f, Float.MAX_VALUE);
        final Integer status = 2;

        //When
        final ResultSetFuture future = DATABIN_QUERIES.insertIntoDatabinByHour(hour, idMetric, cTime, value, status);

        //Then
        future.getUninterruptibly();
        final Row found = SESSION.execute("SELECT * FROM centreon.databin_by_hour WHERE " +
                "hour = " + hour +
                " AND id_metric = " + idMetric +
                " AND ctime = " + cTime).one();
        assertThat(found).isNotNull();
        assertThat(found.getLong("ctime")).isEqualTo(cTime);
        assertThat(found.getFloat("value")).isEqualTo(value);
        assertThat(found.getByte("status")).isEqualTo((byte)2);
    }

}
package com.centreon.injector.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import com.centreon.injector.configuration.DSEConfiguration.DSETopology;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

public class DatabinQueriesTest extends AbstractTestCassandraRepository {

    private static final DatabinQueries DATABIN_QUERIES = new DatabinQueries(SESSION, TOPOLOGY);

    @After
    public void cleanUp() {
        truncate("centreon.databin");
    }

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


}
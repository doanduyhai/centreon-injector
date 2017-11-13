package com.centreon.injector.service;

import static com.centreon.injector.configuration.EnvParams.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;

import org.junit.Test;
import org.springframework.core.env.Environment;

import com.centreon.injector.configuration.DSEConfiguration;
import com.centreon.injector.repository.AnalyticsQueries;
import com.centreon.injector.repository.DatabinQueries;
import com.centreon.injector.repository.MetaDataQueries;
import com.centreon.injector.repository.RRDQueries;
import com.centreon.injector.error_handling.ErrorFileLogger;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

public class InjectionServiceTest {

    private static final Session SESSION = CassandraEmbeddedServerBuilder
            .builder()
            .withScript("cassandra/schema.cql")
            .withScript("cassandra/InjectionService/insert_meta_data.cql")
            .buildNativeSession();

    private static final DSEConfiguration.DSETopology TOPOLOGY = new DSEConfiguration.DSETopology("centreon", "dc1");
    private static final MetaDataQueries METADATA_QUERIES = new MetaDataQueries(SESSION, TOPOLOGY);
    private static final DatabinQueries DATABIN_QUERIES = new DatabinQueries(SESSION, TOPOLOGY);
    private static final RRDQueries RRD_QUERIES = new RRDQueries(SESSION, TOPOLOGY);
    private static final AnalyticsQueries ANALYTICS_QUERIES = new AnalyticsQueries(SESSION, TOPOLOGY);
    private static final ErrorFileLogger ERROR_FILE_LOGGER;
    static {
        try {
            ERROR_FILE_LOGGER = new ErrorFileLogger("/tmp/errors.txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Environment ENV = new FakeEnvironment() {

        @Override
        public String getProperty(String key, String defaultValue) {
            if (key.equals(ASYNC_BATCH_SIZE)) {
                return "1";
            } else if (key.equals(DATABIN_INPUT_FILE)) {
                return System.getProperty("user.dir")
                        + "/src/test/resources/cassandra/InjectionService/sample_databin.csv";
            } else if (key.equals(ASYNC_BATCH_SLEEP_IN_MS)) {
                return "1";
            } else if (key.equals(INSERT_PROGRES_DISPLAY_MULTIPLIER)) {
                return "1";
            } else {
                return "";
            } 
        }
    };

    /**
     *
     CREATE TABLE IF NOT EXISTS centreon.databin (
     id_metric int,
     ctime bigint,
     value float,
     status tinyint,
     PRIMARY KEY((id_metric), ctime)
     );

     CREATE TABLE IF NOT EXISTS centreon.databin_by_hour (
     hour bigint,
     id_metric int,
     ctime bigint,
     value float,
     status tinyint,
     PRIMARY KEY((hour, id_metric), ctime)
     );

     CREATE TABLE IF NOT EXISTS centreon.rrd_aggregated(
     service uuid,
     aggregation_unit text,
     time_value bigint,
     id_metric int,
     previous_time_value bigint,
     min float,
     max float,
     sum float,
     count int,
     PRIMARY KEY ((service,aggregation_unit,time_value),id_metric, previous_time_value));

     */
    @Test
    public void should_inject_from_file() throws Exception {
        //Given
        final InjectionService service = new InjectionService(ENV, DATABIN_QUERIES,
                METADATA_QUERIES, RRD_QUERIES, ANALYTICS_QUERIES, ERROR_FILE_LOGGER);
        //When
        service.injectDatabin();

        //Then
        final Row idMetric_90969 = SESSION.execute("SELECT * FROM centreon.databin WHERE id_metric = " + 90969).one();
        assertThat(idMetric_90969).isNotNull();
        assertThat(idMetric_90969.getLong("ctime")).isEqualTo(1427252950000L);
        assertThat(idMetric_90969.getFloat("value")).isEqualTo(42f);
        assertThat(idMetric_90969.getByte("status")).isEqualTo((byte)2);

        final Row idMetric_90969_by_hour = SESSION.execute("SELECT * FROM centreon.analytics_aggregated WHERE " +
                " id_metric = " + 90969 +
                " AND aggregation_unit='HOUR' " +
                " AND time_value = 2015032503").one();
        assertThat(idMetric_90969_by_hour).isNotNull();
        assertThat(idMetric_90969_by_hour.getLong("previous_time_value")).isEqualTo(1427252950000L);
        assertThat(idMetric_90969_by_hour.getFloat("min")).isEqualTo(42f);
        assertThat(idMetric_90969_by_hour.getFloat("max")).isEqualTo(42f);
        assertThat(idMetric_90969_by_hour.getFloat("sum")).isEqualTo(42f);
        assertThat(idMetric_90969_by_hour.getInt("count")).isEqualTo(1);

        final Row idMetric_95798 = SESSION.execute("SELECT * FROM centreon.rrd_aggregated WHERE " +
                " service=123e4567-e89b-12d3-a456-426655440000" +
                " AND aggregation_unit='HOUR' " +
                " AND time_value=2016021002").one();
        assertThat(idMetric_95798).isNotNull();
        assertThat(idMetric_95798.getLong("previous_time_value")).isEqualTo(20160210024028L);
        assertThat(idMetric_95798.getFloat("min")).isEqualTo(45f);
        assertThat(idMetric_95798.getFloat("max")).isEqualTo(45f);
        assertThat(idMetric_95798.getFloat("sum")).isEqualTo(45f);
        assertThat(idMetric_95798.getInt("count")).isEqualTo(1);



    }
}
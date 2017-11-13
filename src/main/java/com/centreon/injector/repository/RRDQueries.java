package com.centreon.injector.repository;

import static java.lang.String.format;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.centreon.injector.configuration.DSEConfiguration.DSETopology;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Session;

/**
 *
 *  Repository class to insert data into:
 *
 *   CREATE TABLE IF NOT EXISTS centreon.rrd_aggregated(
 *       service uuid,
 *       aggregation_unit text, //HOUR, DAY, WEEK, MONTH
 *       time_value bigint, //HOUR=yyyyMMddHH, DAY=yyyyMMdd, WEEK=yyyyMMdd(first day of week), MONTH=yyyyMM
 *       id_metric int,
 *       previous_time_value bigint, //epoch, HOUR=yyyyMMddHH, DAY=yyyyMMdd, WEEK=yyyyMMdd(first day of week), MONTH=yyyyMM
 *       min float,
 *       max float,
 *       sum float,
 *       count int,
 *       PRIMARY KEY ((service,aggregation_unit,time_value),id_metric, previous_time_value)
 *   );
 **/
@Repository
public class RRDQueries {
    static private final Logger LOGGER = LoggerFactory.getLogger(RRDQueries.class);

    public final String INSERT_INTO_RRD_AGGREGATED = "INSERT INTO %s." +
            "rrd_aggregated(service, aggregation_unit, time_value, id_metric, previous_time_value, min, max, sum, count) " +
            "VALUES(:service, 'HOUR', :hour, :id_metric, :previous_time_value, :min, :max, :sum, :count)";

    private final Session session;
    private final PreparedStatement INSERT_INTO_RRD_AGGREGATED_PS;

    public RRDQueries(@Autowired Session session, @Autowired DSETopology dseTopology) {
        LOGGER.info("Start preparing queries");
        this.session = session;
        this.INSERT_INTO_RRD_AGGREGATED_PS = this.session.prepare(new SimpleStatement(
                format(INSERT_INTO_RRD_AGGREGATED, dseTopology.keyspace)));
    }

    /**
     * <br/>
     * Insert data into centreon.rrd_aggregated table with the aggregation_unit = 'HOUR'.
     *
     */
    public ResultSetFuture insertIntoRrdAggregatedForHour(UUID service, long hour, int idMetric, long previousTimeValue, float value) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Insert row {} {} {} {} {}  into rrd_aggregated", service, hour, idMetric, previousTimeValue, value);
        }
        final BoundStatement bs = INSERT_INTO_RRD_AGGREGATED_PS.bind();
        bs.setUUID("service", service);
        bs.setLong("hour", hour);
        bs.setInt("id_metric", idMetric);
        bs.setLong("previous_time_value", previousTimeValue);
        bs.setFloat("min", value);
        bs.setFloat("max", value);
        bs.setFloat("sum", value);
        bs.setInt("count", 1);
        bs.setIdempotent(true);

        return this.session.executeAsync(bs);
    }
}

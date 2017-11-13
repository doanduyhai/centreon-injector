package com.centreon.injector.repository;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.centreon.injector.configuration.DSEConfiguration.DSETopology;
import com.datastax.driver.core.*;

/**
 *
 *  Repository class to insert data into:
 *
 *   CREATE TABLE IF NOT EXISTS centreon.analytics_aggregated(
 *       id_metric int,
 *       aggregation_unit text, //HOUR, DAY, WEEK, MONTH
 *       time_value bigint, //HOUR=yyyyMMddHH, DAY=yyyyMMdd, WEEK=yyyyMMdd(first day of week), MONTH=yyyyMM
 *       previous_time_value bigint, //epoch, HOUR=yyyyMMddHH, DAY=yyyyMMdd, WEEK=yyyyMMdd(first day of week), MONTH=yyyyMM
 *       min float,
 *       max float,
 *       sum float,
 *       count int,
 *       PRIMARY KEY ((id_metric,aggregation_unit,time_value), previous_time_value)
 *   );
 **/
@Repository
public class AnalyticsQueries {
    static private final Logger LOGGER = LoggerFactory.getLogger(AnalyticsQueries.class);

    public final String INSERT_INTO_ANALYTICS_AGGREGATED = "INSERT INTO %s." +
            "analytics_aggregated(id_metric, aggregation_unit, time_value, previous_time_value, min, max, sum, count) " +
            "VALUES(:id_metric, 'HOUR', :hour, :previous_time_value, :min, :max, :sum, :count)";

    private final Session session;
    private final PreparedStatement INSERT_INTO_ANALYTICS_AGGREGATED_PS;

    public AnalyticsQueries(@Autowired Session session, @Autowired DSETopology dseTopology) {
        LOGGER.info("Start preparing queries");
        this.session = session;
        this.INSERT_INTO_ANALYTICS_AGGREGATED_PS = this.session.prepare(new SimpleStatement(
                format(INSERT_INTO_ANALYTICS_AGGREGATED, dseTopology.keyspace)));
    }

    /**
     * <br/>
     * Insert data into centreon.analytics_aggregated table with the aggregation_unit = 'HOUR'.
     *
     */
    public ResultSetFuture insertIntoAnalyticsdAggregatedForHour(int idMetric, long hour, long previousTimeValue, float value) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Insert row {} {} {} {} {}  into analytics_aggregated", idMetric, hour, previousTimeValue, value);
        }
        final BoundStatement bs = INSERT_INTO_ANALYTICS_AGGREGATED_PS.bind();
        bs.setInt("id_metric", idMetric);
        bs.setLong("hour", hour);
        bs.setLong("previous_time_value", previousTimeValue);
        bs.setFloat("min", value);
        bs.setFloat("max", value);
        bs.setFloat("sum", value);
        bs.setInt("count", 1);
        bs.setIdempotent(true);

        return this.session.executeAsync(bs);
    }
}

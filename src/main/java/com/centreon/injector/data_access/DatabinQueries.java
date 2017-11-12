package com.centreon.injector.data_access;

import static java.lang.String.format;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.centreon.injector.configuration.CassandraConfiguration.DSETopology;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Session;

/**
 *   CREATE TABLE IF NOT EXISTS centreon.databin (
 *       id_metric int,
 *       ctime bigint,
 *       value float,
 *       status tinyint,
 *       PRIMARY KEY((id_metric), ctime)
 *   );
 *
 *   CREATE TABLE IF NOT EXISTS centreon.databin_by_hour (
 *       hour bigint,
 *       id_metric int,
 *       ctime bigint,
 *       value float,
 *       status tinyint,
 *       PRIMARY KEY((hour, id_metric), ctime)
 *   );
 **/
@Repository
public class DatabinQueries {
    static private final Logger LOGGER = LoggerFactory.getLogger(DatabinQueries.class);

    public final String INSERT_INTO_DATABIN = "INSERT INTO %s." +
            "databin(id_metric, ctime, value, status) " +
            "VALUES(:id_metric, :ctime, :value, :status)";

    public final String INSERT_INTO_DATABIN_BY_HOUR = "INSERT INTO %s." +
            "databin_by_hour(hour, id_metric, ctime, value, status) " +
            "VALUES(:hour, :id_metric, :ctime, :value, :status)";

    private final Session session;
    private final PreparedStatement INSERT_INTO_DATABIN_PS;
    private final PreparedStatement INSERT_INTO_DATABIN_BY_HOUR_PS;

    public DatabinQueries(@Autowired Session session, @Autowired DSETopology dseTopology) {
        LOGGER.info("Start preparing queries");
        this.session = session;
        this.INSERT_INTO_DATABIN_PS = session.prepare(new SimpleStatement(
                format(INSERT_INTO_DATABIN, dseTopology.keyspace)));

        this.INSERT_INTO_DATABIN_BY_HOUR_PS = session.prepare(new SimpleStatement(
                format(INSERT_INTO_DATABIN_BY_HOUR, dseTopology.keyspace)));
    }

    public ResultSetFuture insertIntoDatabin(int idMetric, long cTimeAsEpoch, Float value, Integer status) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Inserting row {} {} {} {} into databin", idMetric, cTimeAsEpoch, value, status);
        }
        final BoundStatement bs = INSERT_INTO_DATABIN_PS.bind();
        bs.setInt("id_metric", idMetric);
        bs.setLong("ctime", cTimeAsEpoch);
        if (value == null) {
            bs.unset("value");
        } else {
            bs.setFloat("value", value);
        }
        if (status == null) {
            bs.unset("status");
        } else {
            byte statusByte = ((byte) ((int)status));
            bs.setByte("status", statusByte);
        }

        bs.setIdempotent(true);

        return session.executeAsync(bs);
    }

    public ResultSetFuture insertIntoDatabinByHour(long hour, int idMetric, long cTimeAsEpoch, Float value, Integer status) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Inserting row {} {} {} {} {} into databin_by_hour", hour, idMetric, cTimeAsEpoch, value, status);
        }
        final BoundStatement bs = INSERT_INTO_DATABIN_BY_HOUR_PS.bind();
        bs.setLong("hour", hour);
        bs.setInt("id_metric", idMetric);
        bs.setLong("ctime", cTimeAsEpoch);

        if (value == null) {
            bs.unset("value");
        } else {
            bs.setFloat("value", value);
        }
        if (status == null) {
            bs.unset("status");
        } else {
            byte statusByte = ((byte) ((int)status));
            bs.setByte("status", statusByte);
        }

        bs.setIdempotent(true);

        return session.executeAsync(bs);
    }

}

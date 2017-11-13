package com.centreon.injector.repository;

import static java.lang.String.format;

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
 *  Repository class to insert data into the following table:
 *
 *   CREATE TABLE IF NOT EXISTS centreon.databin (
 *       id_metric int,
 *       ctime bigint,
 *       value float,
 *       status tinyint,
 *       PRIMARY KEY((id_metric), ctime)
 *   );
 **/
@Repository
public class DatabinQueries {
    static private final Logger LOGGER = LoggerFactory.getLogger(DatabinQueries.class);

    public final String INSERT_INTO_DATABIN = "INSERT INTO %s." +
            "databin(id_metric, ctime, value, status) " +
            "VALUES(:id_metric, :ctime, :value, :status)";

    private final Session session;
    private final PreparedStatement INSERT_INTO_DATABIN_PS;

    public DatabinQueries(@Autowired Session session, @Autowired DSETopology dseTopology) {
        LOGGER.info("Start preparing queries");
        this.session = session;
        this.INSERT_INTO_DATABIN_PS = session.prepare(new SimpleStatement(
                format(INSERT_INTO_DATABIN, dseTopology.keyspace)));
    }

    /**
     * <br/>
     * Insert data into the centreon.databin table.
     * <br/>
     * <br/>
     * Please note that mandatory (non null) values are:
     *
     * <ul>
     *     <li>idMetric</li>
     *     <li>cTimeAsEpoch in millisecs</li>
     * </ul>
     *
     * Optional (nullable) values are:
     *
     * <ul>
     *     <li>value</li>
     *     <li>status</li>
     * </ul>
     *
     */
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

}

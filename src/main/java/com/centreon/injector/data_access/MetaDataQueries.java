package com.centreon.injector.data_access;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.centreon.injector.configuration.CassandraConfiguration.DSETopology;
import com.datastax.driver.core.*;


/**
 *   CREATE TABLE IF NOT EXISTS centreon.service_meta(
 *       service uuid,
 *       id_metric int,
 *       PRIMARY KEY((service),id_metric)
 *   );
 *
 **/
@Repository
public class MetaDataQueries {

    public final String SELECT_SERVICES_FOR_METRICS = "SELECT service,id_metric FROM %s.service_meta";

    static private final Logger LOGGER = LoggerFactory.getLogger(MetaDataQueries.class);

    private final Session session;
    private final PreparedStatement SELECT_SERVICES_FOR_METRICS_PS;
    private final IdMetricsByServiceId idMetricsByServiceId;

    public MetaDataQueries(@Autowired Session session, @Autowired DSETopology dseTopology) {
        LOGGER.info("Start preparing queries");
        this.session = session;

        this.SELECT_SERVICES_FOR_METRICS_PS = session.prepare(new SimpleStatement(
                format(SELECT_SERVICES_FOR_METRICS, dseTopology.keyspace)));
        this.idMetricsByServiceId = getMetricsByServiceMap();
    }

    /**
     *
     * Currently we load the whole map of service - id_metric into memory
     * For a customer having millions of services, this approach does NOT work
     * We should then use a caching layer like Redis to perform a fast lookup
     * of service id for each id_metric. In this case this method IMPL should be
     * updated to connect to Redis
     */
    public UUID getServiceIdForIdMetric(int idMetric) {
        return idMetricsByServiceId.get(idMetric);
    }

    private IdMetricsByServiceId getMetricsByServiceMap() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Get Metrics-Service map");
        }
        final IdMetricsByServiceId idMetricsByServiceId = new IdMetricsByServiceId();
        final Iterator<Row> iterator = this.session.execute(
                SELECT_SERVICES_FOR_METRICS_PS.bind()).iterator();

        while (iterator.hasNext()) {
            final Row row = iterator.next();
            idMetricsByServiceId.put(row.getInt("id_metric"), row.getUUID("service"));
        }

        return idMetricsByServiceId;
    }


    public class IdMetricsByServiceId extends HashMap<Integer, UUID> {}
}

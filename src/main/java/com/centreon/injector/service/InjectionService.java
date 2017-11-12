package com.centreon.injector.service;

import static com.centreon.injector.configuration.EnvParams.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.centreon.injector.configuration.EnvParams;
import com.centreon.injector.data_access.DatabinQueries;
import com.centreon.injector.data_access.MetaDataQueries;
import com.centreon.injector.data_access.RRDQueries;
import com.centreon.injector.error_handling.ErrorFileLogger;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.ListenableFuture;

@Service
public class InjectionService {

    static final private Logger LOGGER = LoggerFactory.getLogger(InjectionService.class);

    private final DatabinQueries databinQueries;
    private final MetaDataQueries metaDataQueries;
    private final RRDQueries rrdQueries;
    private final ErrorFileLogger errorFileLogger;

    private final int asyncBatchSize;
    private final int asyncBatchSleepInMillis;
    private final int insertProgressCount;
    private final String inputDatabinFile;
    private final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
    private final DateTimeFormatter secondFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private final ZoneId UTC_ZONE = ZoneId.ofOffset("UTC", ZoneOffset.UTC);

    public InjectionService(@Autowired Environment env,
                            @Autowired DatabinQueries databinQueries,
                            @Autowired MetaDataQueries metaDataQueries,
                            @Autowired RRDQueries rrdQueries,
                            @Autowired ErrorFileLogger errorFileLogger) {
        this.databinQueries = databinQueries;
        this.metaDataQueries = metaDataQueries;
        this.rrdQueries = rrdQueries;
        this.asyncBatchSize = Integer.parseInt(env.getProperty(ASYNC_BATCH_SIZE, ASYNC_BATCH_SIZE_DEFAULT));
        this.inputDatabinFile = env.getProperty(DATABIN_INPUT_FILE, DATABIN_INPUT_FILE_DEFAULT);
        this.asyncBatchSleepInMillis = Integer.parseInt(env.getProperty(ASYNC_BATCH_SLEEP_IN_MS, ASYNC_BATCH_SLEEP_IN_MS_DEFAULT));
        final int insertProgressDisplayMultiplier = Integer.parseInt(env.getProperty(INSERT_PROGRES_DISPLAY_MULTIPLIER, INSERT_PROGRES_DISPLAY_MULTIPLIER_DEFAULT));
        this.insertProgressCount = asyncBatchSize * insertProgressDisplayMultiplier;
        this.errorFileLogger = errorFileLogger;
    }

    /**
     * databin file format:
     *
     * id_metric|ctime|value|status
     */
    public void injectDatabin() throws InterruptedException {
        LOGGER.info("Start injecting data from databin file: {}", inputDatabinFile);

        String line;

        int counter=0;

        try(FileReader fr = new FileReader(inputDatabinFile);
            BufferedReader br = new BufferedReader(fr, 1000 * 8192)) {

            final Map<ListenableFuture<ResultSet>, String> futures = new HashMap<>(asyncBatchSize + 1);

            while ((line = br.readLine()) != null) {
                final String[] row = line.split("\\|");
                if (row.length != 4) {
                    LOGGER.error("Cannot parse line {}, there are more than 4 columns", line);
                } else {

                    final int idMetric = Integer.parseInt(row[0]);
                    final long cTime = Long.parseLong(row[1]);
                    final long cTimeAsEpoch = cTime * 1000;
                    final Date cTimeAsDate = new Date(cTimeAsEpoch);
                    final long hour = getHourFromDate(cTimeAsDate);
                    final long seconds = geSecondFromDate(cTimeAsDate);
                    final float value = Float.parseFloat(row[2]);
                    final int status = Integer.parseInt(row[3]);

                    final UUID service = metaDataQueries.getServiceIdForIdMetric(idMetric);

                    futures.put(databinQueries.insertIntoDatabin(idMetric, cTimeAsEpoch, value, status), line);
                    futures.put(databinQueries.insertIntoDatabinByHour(hour, idMetric, cTimeAsEpoch, value, status), line);
                    futures.put(rrdQueries.insertIntoRrdAggregatedForHour(service, hour, idMetric, seconds, value), line);

                    if (futures.size() >= asyncBatchSize) {
                        asyncInsert(futures);
                        futures.clear();
                        Thread.sleep(asyncBatchSleepInMillis);
                    }
                    counter += 3;

                    if (counter >= insertProgressCount) {
                        LOGGER.debug("Successful {} async inserts", insertProgressCount);
                        counter = 0;
                    }
                }
            }

            if (futures.size() >= 0) {
                asyncInsert(futures);
            }

        } catch (IOException e) {
            LOGGER.error(String.format("Exception when reading %s file",inputDatabinFile), e);
            throw new RuntimeException(e);
        }

        LOGGER.info("Finish injecting data SUCCESSFULLY from databin file: {}", inputDatabinFile);

    }

    private void asyncInsert(Map<ListenableFuture<ResultSet>, String> map) {
        for (Map.Entry<ListenableFuture<ResultSet>, String> entry: map.entrySet()) {
            try {
                entry.getKey().get(10, TimeUnit.SECONDS);
            } catch (InterruptedException| ExecutionException | TimeoutException e) {
                LOGGER.error("Cannot insert line because {}", e.getMessage());
                errorFileLogger.writeLine(entry.getValue());
            }
        }
    }

    protected long getHourFromDate(Date cTime) {
        final LocalDateTime localDateTime = cTime
                .toInstant()
                .atZone(UTC_ZONE)
                .toLocalDateTime();
        return Long.parseLong(localDateTime.format(hourFormatter));
    }

    protected long geSecondFromDate(Date cTime) {
        final LocalDateTime localDateTime = cTime
                .toInstant()
                .atZone(UTC_ZONE)
                .toLocalDateTime();
        return Long.parseLong(localDateTime.format(secondFormatter));
    }

}

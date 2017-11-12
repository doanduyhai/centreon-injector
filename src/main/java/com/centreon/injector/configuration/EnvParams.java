package com.centreon.injector.configuration;

/**
 *
 * Centralize all Spring configuration parameters here with their default values
 *
 */
public interface EnvParams {

    String DSE_CONTACT_POINT = "dse.contact_point";
    String DSE_CONTACT_POINT_DEFAULT = "127.0.0.1";

    String DSE_CLUSTER_NAME = "dse.cluster_name";
    String DSE_CLUSTER_NAME_DEFAULT = "Test Cluster";


    String DSE_LOCAL_DC = "dse.local_DC";
    String DSE_LOCAL_DC_DEFAULT = "dc1";

    String DSE_KEYSPACE_NAME = "dse.keyspace_name";
    String DSE_KEYSPACE_NAME_DEFAULT = "centreon";

    String DSE_USERNAME = "dse.username";
    String DSE_USERNAME_DEFAULT = "cassandra";

    String DSE_PASSWORD = "dse.pass";
    String DSE_PASSWORD_DEFAULT = "cassandra";

    String DSE_CONNECTION_READ_FETCH_SIZE = "dse.read_fetch_size";
    String DSE_CONNECTION_READ_FETCH_SIZE_DEFAULT = "10000";

    String DSE_CONNECTION_DEFAULT_CONSISTENCY = "dse.default_consistency";
    String DSE_CONNECTION_DEFAULT_CONSISTENCY_DEFAULT = "LOCAL_QUORUM";

    String DATABIN_INPUT_FILE = "dse.input_databin_file";
    String DATABIN_INPUT_FILE_DEFAULT = "/tmp/databin.csv";

    String INSERTION_ERROR_FILE = "dse.input_error_file";
    String INSERTION_ERROR_FILE_DEFAULT = "/tmp/errors.txt";

    String ASYNC_BATCH_SIZE = "dse.async_batch_size";
    String ASYNC_BATCH_SIZE_DEFAULT = "1000";

    String ASYNC_BATCH_SLEEP_IN_MS = "dse.async_batch_sleep_in_millis";
    String ASYNC_BATCH_SLEEP_IN_MS_DEFAULT = "10";

    String INSERT_PROGRES_DISPLAY_MULTIPLIER = "dse.insert_progress_display_multiplier";
    String INSERT_PROGRES_DISPLAY_MULTIPLIER_DEFAULT = "100";

}

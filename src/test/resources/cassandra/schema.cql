CREATE KEYSPACE IF NOT EXISTS centreon WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1};

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

CREATE TABLE IF NOT EXISTS centreon.service_meta(
    service uuid,
    id_metric int,
    PRIMARY KEY((service),id_metric)
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
    PRIMARY KEY ((service,aggregation_unit,time_value),id_metric, previous_time_value)
);
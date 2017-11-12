# Centreon injector

## I Build

To build the project you'll need:

* Java 8 latest release (preferably) installed
* Maven 3.x installed

Command to build: `mvn clean package`. It will generate a jar file in `./target/data-injector-package-<project_version>.jar` where `<project_version>` is the project version
 
## II Run
 
To run the project: 

```bash
java 
    -jar data-injector-package-<project_version>.jar 
    -Dlogback.configurationFile=file:///<path_to_logback.xml> 
    --spring.config.location=file:///<path_to_application.properties>
``` 

#### A `logback.xml` file:

Below is an example of a logback configuration file:

```xml
<configuration scan="true" scanPeriod="30 seconds">
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36}:%msg%n</pattern>
        </encoder>
    </appender>


    <logger name="com.centreon.injector" level="DEBUG" additivity="false">
        <appender-ref ref="STDOUT" />
    </logger>

    <root level="WARN">
        <appender-ref ref="STDOUT" />
    </root>
</configuration>
```

Please notice that you can update the log configuration at **runtime without restarting the program** because `scan="true"` and `scanPeriod="30 seconds"` 

You can also modify dynamically the logging level and/or add extra logger(s)

#### B `application.properties` file:

Below is the mandatory content of the Spring properties file:

```
#Cluster connection
dse.contact_point: <IP_address_of_one_Cassandra_seed_node>
dse.cluster_name: <cluster_name>
dse.keyspace_name: <keyspace_name>
dse.local_DC: <local_datacenter_name>

#Credentials
dse.username: <login>
dse.pass: <password>

#Connection config
dse.read_fetch_size: 10000
dse.default_consistency: LOCAL_QUORUM

#Injector config
dse.input_databin_file: /data/databin_raw.csv
dse.async_batch_size: 3000
dse.async_batch_sleep_in_millis: 15
dse.insert_progress_display_multiplier: 1000
dse.input_error_file: /home/ubuntu/data_injector/injection_errors.txt
```

* `dse.input_databin_file`: location of the input CSV data to be inserted into DSE
* `dse.input_error_file`: location of error file. Any un-processed or failed row from the `dse.input_databin_file` will be recorded into this file for later retrieval
* `dse.async_batch_size`: number of **concurrent** inserts into Cassandra, before sleeping to let the cluster handle the queries
* `dse.async_batch_sleep_in_millis`: number of millisecs to sleep between two `dse.async_batch_size`
* `dse.insert_progress_display_multiplier`: to display the insert progress, the program will output a log message every `dse.async_batch_size * dse.insert_progress_display_multiplier` rows successfully inserted into Cassandra

> To throttle the insertion, play with the parameters `dse.async_batch_size` and `dse.async_batch_sleep_in_millis`

The `dse.insert_progress_display_multiplier` parameter is only useful to monitor the insertion progress and has no impact on the insertion process itself


#### C Databin file format

The input databin file should have the following format: 

`id_metric|ctime_as_epoch_in_second|value|status`

The separator is the pipe (`|`) and the time is of epoch format but in seconds (and not milliseconds)

You can use Linux commands like `awk`, `sed`, `cut` and `paste` to reformat the input file to match those requirements





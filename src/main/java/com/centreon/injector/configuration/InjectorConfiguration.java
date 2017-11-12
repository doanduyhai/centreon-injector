package com.centreon.injector.configuration;

import static com.centreon.injector.configuration.EnvParams.INSERTION_ERROR_FILE;
import static com.centreon.injector.configuration.EnvParams.INSERTION_ERROR_FILE_DEFAULT;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.centreon.injector.error_handling.ErrorFileLogger;

@Configuration
public class InjectorConfiguration {

    static final private Logger LOGGER = LoggerFactory.getLogger(InjectorConfiguration.class);

    private final Environment env;

    public InjectorConfiguration(@Autowired Environment env) {
        this.env = env;
    }

    @Bean(destroyMethod = "close")
    public ErrorFileLogger getErrorFileLogger() {
        LOGGER.info("Initializing error file");

        String errorFile = env.getProperty(INSERTION_ERROR_FILE, INSERTION_ERROR_FILE_DEFAULT);
        try {
            return new ErrorFileLogger(errorFile);
        } catch (IOException e) {
            LOGGER.error(String.format("Cannot create error file %s", errorFile), e);
            throw new RuntimeException(e);
        }
    }
}

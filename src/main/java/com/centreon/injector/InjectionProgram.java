package com.centreon.injector;


import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import com.centreon.injector.service.InjectionService;

@SpringBootApplication
public class InjectionProgram {
    final private static Logger LOGGER = LoggerFactory.getLogger(InjectionProgram.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Initializing Spring context");
        final ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder()
                .banner(new InjectorBanner())
                .sources(InjectionProgram.class)
                .run(args);
        final InjectionService injectionService = applicationContext.getBean(InjectionService.class);

        LOGGER.info("Start injecting databin");
        injectionService.injectDatabin();

        applicationContext.close();
    }

    public static class InjectorBanner implements Banner {

        /**
         * Ascii art generator : <a href=http://patorjk.com/software/taag/#p=display&f=Doom&t=AsciiText>http://patorjk.com/software/taag/</a>
         */
        @Override
        public void printBanner(Environment environment, Class<?> sourceClass, PrintStream out) {
            StringBuilder builder = new StringBuilder();
            builder.append(" \n");
            builder.append(" _____               _                               _____           _              _                \n");
            builder.append("/  __ \\             | |                             |_   _|         (_)            | |               \n");
            builder.append("| /  \\/  ___  _ __  | |_  _ __   ___   ___   _ __     | |   _ __     _   ___   ___ | |_   ___   _ __ \n");
            builder.append("| |     / _ \\| '_ \\ | __|| '__| / _ \\ / _ \\ | '_ \\    | |  | '_ \\   | | / _ \\ / __|| __| / _ \\ | '__|\n");
            builder.append("| \\__/\\|  __/| | | || |_ | |   |  __/| (_) || | | |  _| |_ | | | |  | ||  __/| (__ | |_ | (_) || |   \n");
            builder.append(" \\____/ \\___||_| |_| \\__||_|    \\___| \\___/ |_| |_|  \\___/ |_| |_|  | | \\___| \\___| \\__| \\___/ |_|   \n");
            builder.append("                                                                   _/ |                              \n");
            builder.append("                                                                  |__/                               \n");
            builder.append(" \n");

            out.println(builder.toString());
        }
    }
}

package com.rbkmoney.shumway.replicator;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;

@Slf4j
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(classes = ReplicatorApplication.class)
@ContextConfiguration(initializers = ReplicatorApplicationContextUpTest.Initializer.class)
public class ReplicatorApplicationContextUpTest {

    @ClassRule
    public static PostgreSQLContainer postgres1 = new PostgreSQLContainer<>("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));
    @ClassRule
    public static PostgreSQLContainer postgres2 = new PostgreSQLContainer<>("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    @Test
    public void contextUp() {

    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @SneakyThrows
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("datasource-first.url=" + postgres1.getJdbcUrl(),
                            "datasource-first.username=" + postgres1.getUsername(),
                            "datasource-first.password=" + postgres1.getPassword(),
                            "datasource-second.url=" + postgres2.getJdbcUrl(),
                            "datasource-second.username=" + postgres2.getUsername(),
                            "datasource-second.password=" + postgres2.getPassword(),
                            "spring.flyway.url=" + postgres1.getJdbcUrl(),
                            "spring.flyway.user=" + postgres1.getUsername(),
                            "spring.flyway.password=" + postgres1.getPassword())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

}
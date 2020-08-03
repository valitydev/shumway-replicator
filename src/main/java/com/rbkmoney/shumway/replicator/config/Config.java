package com.rbkmoney.shumway.replicator.config;


import com.rbkmoney.damsel.shumaich.AccounterSrv;
import com.rbkmoney.shumway.replicator.dao.ProgressDAO;
import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicLong;

@Configuration
public class Config {

    @Bean
    public AccounterSrv.Iface shumaichClient(@Value("${shumaich.target.uri}") String uri) throws URISyntaxException {
        return new THSpawnClientBuilder().withAddress(new URI(uri)).withNetworkTimeout(5000).build(AccounterSrv.Iface.class);
    }

    @Bean
    public com.rbkmoney.damsel.accounter.AccounterSrv.Iface shumwayClient(@Value("${shumway.target.uri}") String uri) throws URISyntaxException {
        return new THSpawnClientBuilder().withAddress(new URI(uri)).withNetworkTimeout(5000).build(com.rbkmoney.damsel.accounter.AccounterSrv.Iface.class);
    }

    @Bean
    @Primary
    @ConfigurationProperties("datasource-first")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @Primary
    @ConfigurationProperties("datasource-first.hikari")
    public HikariDataSource dataSource(DataSourceProperties dataSourceProperties) {
        return dataSourceProperties.initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }

    @Bean
    @ConfigurationProperties("datasource-second")
    public DataSourceProperties shumwayDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("datasource-second.hikari")
    public HikariDataSource shumwayDataSource(DataSourceProperties shumwayDataSourceProperties) {
        return shumwayDataSourceProperties.initializeDataSourceBuilder()
                .type(HikariDataSource.class).build();
    }

    @Bean
    public ShumwayDAO shumwayDAO(DataSource shumwayDataSource) {
        return new ShumwayDAO(shumwayDataSource);
    }

    @Bean
    public ProgressDAO progressDAO(DataSource dataSource) {
        return new ProgressDAO(dataSource);
    }

    @Bean
    public Flyway flyway(DataSource dataSource) {
        final Flyway flyway = Flyway.configure().dataSource(dataSource).load();
        flyway.migrate();
        return flyway;
    }

    @Bean
    @DependsOn("flyway")
    public AtomicLong lastReplicatedPosting(ProgressDAO progressDAO) {
        return new AtomicLong(progressDAO.getProgress());
    }

}

package com.rbkmoney.shumway.replicator.config;


import com.rbkmoney.damsel.shumpune.AccounterSrv;
import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.shumway.replicator.service.ProgressService;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
    @ConfigurationProperties(prefix="spring.datasource-first")
    public DataSource primaryDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix="spring.datasource-second")
    public DataSource shumwayDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public ShumwayDAO dao(DataSource shumwayDataSource) {
        return new ShumwayDAO(shumwayDataSource);
    }

    @Bean
    public AtomicLong lastReplicatedPosting(ProgressService progressService) {
        return new AtomicLong(progressService.getProgress().getLatestPosting());
    }

}

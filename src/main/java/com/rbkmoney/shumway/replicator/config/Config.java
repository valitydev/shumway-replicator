package com.rbkmoney.shumway.replicator.config;

import com.rbkmoney.damsel.accounter.AccounterSrv;
import com.rbkmoney.damsel.shumpune.MigrationHelperSrv;
import com.rbkmoney.shumway.replicator.service.ReplicatorService;
import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by vpankrashkin on 16.05.18.
 */
@Configuration
public class Config {

    @Bean
    public com.rbkmoney.damsel.shumpune.AccounterSrv.Iface shumpuneClient(@Value("${shumpune.target.uri}") String uri) throws URISyntaxException {
        return new THSpawnClientBuilder().withAddress(new URI(uri)).withNetworkTimeout(5000).build(com.rbkmoney.damsel.shumpune.AccounterSrv.Iface.class);
    }

    @Bean
    public MigrationHelperSrv.Iface shumpuneMigrationClient(@Value("${shumpune.target.uri}") String uri) throws URISyntaxException {
        return new THSpawnClientBuilder().withAddress(new URI(uri)).withNetworkTimeout(5000).build(MigrationHelperSrv.Iface.class);
    }

    @Bean
    public AccounterSrv.Iface shumwayClient(@Value("${shumway.target.uri}") String uri) throws URISyntaxException {
        return new THSpawnClientBuilder().withAddress(new URI(uri)).withNetworkTimeout(5000).build(AccounterSrv.Iface.class);
    }

}

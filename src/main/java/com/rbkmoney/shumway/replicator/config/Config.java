package com.rbkmoney.shumway.replicator.config;


import com.rbkmoney.damsel.shumpune.AccounterSrv;
import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public ShumwayDAO dao(DataSource ds) {
        return new ShumwayDAO(ds);
    }

    @Bean
    public AtomicLong lastReplicatedPosting() {
        return new AtomicLong(0);
    }

}

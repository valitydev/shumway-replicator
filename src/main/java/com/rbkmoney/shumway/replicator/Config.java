package com.rbkmoney.shumway.replicator;


import com.rbkmoney.damsel.shumpune.AccounterSrv;
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
    public AccounterSrv.Iface shumwayClient(@Value("${shumaich.target.uri}") String uri) throws URISyntaxException {
        return new THSpawnClientBuilder().withAddress(new URI(uri)).withNetworkTimeout(5000).build(AccounterSrv.Iface.class);
    }

    @Bean
    public ShumwayDAO dao(DataSource ds) {
        return new ShumwayDAO(ds);
    }

    @Bean
    public Replicator replicator(ShumwayDAO dao, AccounterSrv.Iface client) {
        return new Replicator(dao, client);
    }
}

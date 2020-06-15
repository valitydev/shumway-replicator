package com.rbkmoney.shumway.replicator;

import com.rbkmoney.damsel.shumpune.AccounterSrv;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by vpankrashkin on 11.05.18.
 */
public class Replicator {
    private static final Logger log = LoggerFactory.getLogger(Replicator.class);
    static final long SEQ_CHECK_STALING = 1000 * 60 * 5;

    private final Thread postingReplicator;

    @Autowired
    public Replicator(ShumwayDAO dao, AccounterSrv.Iface client) {
        this.postingReplicator = new Thread(new PostingReplicator(dao, client, 0), "PostingReplicator");
    }

    @PostConstruct
    public void fire() {
        new Thread(() -> {
            log.info("Start replicator");
            try {
                postingReplicator.start();
            } catch (Throwable t) {
                log.error("Replicator error", t);
                throw new RuntimeException("Replicator error", t);
            } finally {
                log.info("Destroy replicator");
            }
        }).start();
    }

    @PreDestroy
    public void destroy() {
        postingReplicator.interrupt();
    }

    protected static  <T> T executeCommand(Callable<T> command, Object data, long stalingTime) throws Exception {
        while (true) {
            try {
                return command.call();
            } catch (WUndefinedResultException | WUnavailableResultException e) {
                log.warn("Temporary command error, retry", e);
                Thread.sleep(stalingTime);
            } catch (Exception e) {
                log.error("Failed to execute command with data: {}, retry", data);
                Thread.sleep(stalingTime);
            }
        }
    }

}

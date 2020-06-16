package com.rbkmoney.shumway.replicator.service.replication;

import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.shumway.replicator.domain.replication.Status;
import com.rbkmoney.shumway.replicator.domain.replication.StatusCheckResult;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReplicatorService {
    static final long SEQ_CHECK_STALING = 1000 * 60 * 5;
    private Thread postingReplicator;

    private final PostingReplicatorThread postingReplicatorThread;
    private final ShumwayDAO shumwayDAO;
    private final AtomicLong lastReplicatedPosting;

    private Status status = Status.NOT_STARTED;

    @PostConstruct
    public void initThreads() {
        postingReplicator = new Thread(postingReplicatorThread);
    }

    public void fire() {
        new Thread(() -> {
            log.info("Start replicator");
            try {
                status = Status.IN_PROGRESS;
                postingReplicator.start();
            } catch (Throwable t) {
                status = Status.ERROR;
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

    public StatusCheckResult status() {
        return StatusCheckResult.builder()
                .status(status)
                .postingPlanNumber(lastReplicatedPosting.get())
                .totalPostingPlanAmount(shumwayDAO.totalPostingsCount())
                .build();
    }
}

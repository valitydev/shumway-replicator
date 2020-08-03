package com.rbkmoney.shumway.replicator.service.replication;

import com.rbkmoney.damsel.shumpune.*;
import com.rbkmoney.shumway.replicator.dao.ProgressDAO;
import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.shumway.replicator.domain.PostingLog;
import com.rbkmoney.shumway.replicator.domain.PostingOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.rbkmoney.shumway.replicator.domain.PostingOperation.HOLD;
import static com.rbkmoney.shumway.replicator.service.replication.ReplicatorService.executeCommand;

@Slf4j
@Component
@RequiredArgsConstructor
public class PostingReplicatorThread implements Runnable {

    @Value("${replicator.batch.size:2500}")
    private Integer batchSize;

    @Value("${replicator.backoff.time.remote.ms:200}")
    private Integer remoteBackoffTime;

    @Value("${replicator.backoff.time.changes.ms:2000}")
    private Integer changesBackoffTime;


    private int windowSize = 1000;

    private final ShumwayDAO dao;
    private final AccounterSrv.Iface client;
    private final AtomicLong lastPostingReplicatedId;
    private final ProgressDAO progressDAO;

    Map<String, ReplicationPoint> lastPlanPoints = new HashMap<>();
    TreeMap<Long, ReplicationPoint> points = new TreeMap<>();
    Map<String, Clock> clocks = new HashMap<>();

    private static class ReplicationPoint {
        final PostingOperation operation;
        final String planId;
        final long firstPostingId;
        List<Posting> postings = new ArrayList<>();
        List<PostingBatch> batches = new ArrayList<>();
        Long lastBatchId = null;

        public ReplicationPoint(String planId, PostingOperation operation, long firstPostingId) {
            this.planId = planId;
            this.operation = operation;
            this.firstPostingId = firstPostingId;
        }

        @Override
        public String toString() {
            return "ReplicationPoint{" +
                    "operation=" + operation +
                    ", planId='" + planId + '\'' +
                    ", firstPostingId=" + firstPostingId +
                    ", postings=" + postings +
                    ", batches=" + batches +
                    '}';
        }
    }

    private ReplicationPoint nextLogPoint(PostingLog postingLog) {
        lastPlanPoints.remove(postingLog.getPlanId());
        return logPoint(postingLog);
    }

    private ReplicationPoint logPoint(PostingLog postingLog) {
        ReplicationPoint lastPlanPoint = lastPlanPoints.computeIfAbsent(postingLog.getPlanId(), p -> new ReplicationPoint(p, postingLog.getOperation(), postingLog.getId()));
        points.putIfAbsent(lastPlanPoint.firstPostingId, lastPlanPoint);
        return lastPlanPoint;
    }

    private boolean addBatch(ReplicationPoint point) {
        if (!point.postings.isEmpty()) {
            point.batches.add(new PostingBatch(point.lastBatchId, new ArrayList<>(point.postings)));
            point.postings.clear();
            return true;
        }
        return false;
    }

    @Override
    public void run() {
        log.info("Start posting replicator from id: {}", lastPostingReplicatedId);
        try {
            boolean prevNoData = false;

            while (!Thread.currentThread().isInterrupted()) {
                log.info("Get postings from id: {}", lastPostingReplicatedId);
                List<PostingLog> postingLogs = executeCommand(() -> dao.getPostingLogs(lastPostingReplicatedId.get(), batchSize), lastPostingReplicatedId, remoteBackoffTime);
                if (postingLogs.isEmpty()) {
                    if (prevNoData && !lastPlanPoints.isEmpty()) {
                        flushToBounds(0);
                    }
                    log.info("Awaiting new postings on: {}", lastPostingReplicatedId);
                    Thread.sleep(changesBackoffTime);

                    if (!prevNoData) {
                        prevNoData = true;
                    }
                } else {
                    if (!validatePostingSequence(postingLogs)) {
                        log.warn("Sequence not validated, awaiting for continuous range on: {}", lastPostingReplicatedId);
                        Thread.sleep(changesBackoffTime);
                        continue;
                    }

                    log.info("Extracted {} new postings [{}, {}]", postingLogs.size(), postingLogs.get(0).getId(), postingLogs.get(postingLogs.size() - 1).getId());
                    for (PostingLog postingLog : postingLogs) {
                        log.debug("Processing log record: {}", postingLog);
                        ReplicationPoint lastPlanPoint = lastPlanPoints.get(postingLog.getPlanId());

                        Optional<ReplicationPoint> optionalLastPoint = Optional.ofNullable(lastPlanPoint);
                        boolean sameBatch = isSameBatch(postingLog, optionalLastPoint.map(p -> p.lastBatchId).orElse(null));
                        if (isSameOperation(postingLog.getOperation(), optionalLastPoint.map(p -> p.operation).orElse(null))) {
                            if (!sameBatch) {
                                addBatch(lastPlanPoint);
                                if (lastPlanPoint.operation == HOLD) {
                                    lastPlanPoint = nextLogPoint(postingLog);
                                } else {
                                    lastPlanPoint = logPoint(postingLog);
                                }
                            } else {
                                lastPlanPoint = logPoint(postingLog);
                            }
                        } else {
                            lastPlanPoint = nextLogPoint(postingLog);
                        }

                        prevNoData = false;
                        lastPlanPoint.postings.add(convertToProto(postingLog));
                        lastPlanPoint.lastBatchId = postingLog.getBatchId();
                        lastPostingReplicatedId.set(postingLog.getId());
                        progressDAO.saveProgess(lastPostingReplicatedId.get());
                    }
                    int flushed = flushToBounds(windowSize);
                    if (flushed > 0) {
                        log.info("Flushed {} points", flushed);
                    }
                }
            }
        } catch (InterruptedException e) {
            log.warn("Posting replicator interrupted");
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            log.error("Posting replicator error", t);
            throw new RuntimeException("Posting replicator error", t);
        } finally {
            log.info("Stop posting replicator on: {}", lastPostingReplicatedId);
        }
    }

    private boolean isSameOperation(PostingOperation logOp, PostingOperation pointOp) {
        return pointOp == null || logOp == pointOp;
    }

    private int flushToBounds(int windowSize) throws Exception {
        int diff = points.size() - windowSize;
        while (diff-- > 0) {
            flushFirst();
        }
        return diff;
    }

    private void flushFirst() throws Exception {
        ReplicationPoint flushPoint = points.remove(points.firstEntry().getKey());
        ReplicationPoint lastPlanPoint = lastPlanPoints.get(flushPoint.planId);
        if (lastPlanPoint.firstPostingId == flushPoint.firstPostingId) {
            lastPlanPoints.remove(flushPoint.planId);
        }
        addBatch(flushPoint);
        try {
            switch (flushPoint.operation) {
                case HOLD:
                    processHold(flushPoint);
                    break;
                case COMMIT:
                    processCommit(flushPoint);
                    break;
                case ROLLBACK:
                    processRollback(flushPoint);
                    break;
            }
        } catch (Exception e) {
            log.error("Flush point: {}", flushPoint);
            throw e;
        }
    }

    boolean validatePostingSequence(List<PostingLog> postingLogs) {
        long border = postingLogs.get(postingLogs.size() - 1).getId();
        long distance = border - lastPostingReplicatedId.get();
        if (distance != postingLogs.size()) {
            log.warn("Gaps in posting sequence range: [{}, {}], distance: {}", lastPostingReplicatedId, border, distance);
            Instant lastCreationTime = postingLogs.get(postingLogs.size() - 1).getCreationTime();
            if (lastCreationTime.plusMillis(ReplicatorService.SEQ_CHECK_STALING).isBefore(Instant.now())) {
                log.warn("Last time in log pack:{} is old enough, seq check staled [continue]", lastCreationTime);
                return true;
            } else {
                log.warn("Last time in log pack: {} isn't old enough, seq check failed [await]", lastCreationTime);
                return false;
            }
        }
        return true;
    }

    void processHold(ReplicationPoint point) throws Exception {
        PostingPlanChange postingPlanChange = new PostingPlanChange(point.planId, new PostingBatch(point.lastBatchId, point.batches.get(0).getPostings()));
        log.info("Hold: {}", postingPlanChange);
        final Clock clock = executeCommand(() -> client.hold(postingPlanChange, null), postingPlanChange, remoteBackoffTime);
        clocks.put(planAndBatchId(point), clock);
    }

    void processCommit(ReplicationPoint point) throws Exception {
        PostingPlan postingPlan = new PostingPlan(point.planId, point.batches);
        log.info("Commit: {}", postingPlan);
        final Clock clock = clocks.remove(planAndBatchId(point));
        executeCommand(() -> client.commitPlan(postingPlan, clock), postingPlan, remoteBackoffTime);
    }

    void processRollback(ReplicationPoint point) throws Exception {
        PostingPlan postingPlan = new PostingPlan(point.planId, point.batches);
        log.info("Rollback: {}", postingPlan);
        final Clock clock = clocks.remove(planAndBatchId(point));
        executeCommand(() -> client.rollbackPlan(postingPlan, clock), postingPlan, remoteBackoffTime);
    }

    boolean isSameBatch(PostingLog postingLog, Long lastBatchId) {
        return lastBatchId == null || lastBatchId.equals(postingLog.getBatchId());
    }

    Posting convertToProto(PostingLog postingLog) {
        return new Posting(new Account(postingLog.getFromAccountId() + "", postingLog.getCurrSymCode()),
                new Account(postingLog.getToAccountId() + "", postingLog.getCurrSymCode()),
                postingLog.getAmount(),
                postingLog.getCurrSymCode(),
                postingLog.getDescription());
    }

    private String planAndBatchId(ReplicationPoint point) {
        return String.format("%s_%d", point.planId, point.lastBatchId);
    }
}

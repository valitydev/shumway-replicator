package com.rbkmoney.shumway.replicator;

import com.rbkmoney.damsel.accounter.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumway.replicator.domain.Account;
import com.rbkmoney.shumway.replicator.domain.PostingLog;
import com.rbkmoney.shumway.replicator.domain.PostingOperation;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static com.rbkmoney.shumway.replicator.domain.PostingOperation.HOLD;

/**
 * Created by vpankrashkin on 11.05.18.
 */
public class Replicator {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ShumwayDAO dao;
    private final AtomicLong lastReplicatedAccount = new AtomicLong(0);
    private final AtomicLong lastReplicatedPosting = new AtomicLong(0);
    private final Thread accountReplicator;
    private final Thread postingReplicator;

    @Autowired
    public Replicator(ShumwayDAO dao, AccounterSrv.Iface client) {
        this.dao = dao;
        this.accountReplicator = new Thread(new AccountReplicator(dao, client, lastReplicatedAccount), "AccountReplicator");
        this.postingReplicator = new Thread(new PostingReplicator(dao, client, lastReplicatedAccount, lastReplicatedPosting), "PostingReplicator");
    }

    @PostConstruct
    public void fire() {
        new Thread(() -> {
            log.info("Start replicator");
            try {
                accountReplicator.start();
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
        accountReplicator.interrupt();
    }

    public static class AccountReplicator implements Runnable {
        private static final int BATCH_SIZE = 1000;
        private static final int STALING_TIME = 3000;
        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final ShumwayDAO dao;
        private final AccounterSrv.Iface client;
        private final AtomicLong lastReplicatedId;

        public AccountReplicator(ShumwayDAO dao, AccounterSrv.Iface client, AtomicLong lastReplicatedId) {
            this.dao = dao;
            this.client = client;
            this.lastReplicatedId = lastReplicatedId;
        }

        @Override
        public void run() {
            log.info("Start account replicator from id: {}", lastReplicatedId.get());
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    List<Account> accounts = dao.getAccounts(lastReplicatedId.get(), BATCH_SIZE);
                    if (accounts.isEmpty()) {
                        log.info("Awaiting new accounts on: {}", lastReplicatedId.get());
                        Thread.sleep(STALING_TIME);
                    } else {
                        if (!validateAccountSequence(accounts)) {
                            log.warn("Sequence not validated, awaiting for continuous range on: {}", lastReplicatedId.get());
                            Thread.sleep(STALING_TIME);
                            continue;
                        }
                        log.info("Extracted {} new accounts [{}, {}]", accounts.size(), accounts.get(0).getId(), accounts.get(accounts.size() - 1).getId());
                        for (Account acc : accounts) {
                            log.debug("Saving account: {}", acc);
                            lastReplicatedId.set(checkId(acc, client.createAccount(convertToProto(acc))));
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Account replicator interrupted");
            } catch (Throwable t) {
                log.error("Account replicator error", t);
                throw new RuntimeException("Account replicator error", t);
            } finally {
                log.info("Stop account replicator on: {}", lastReplicatedId.get());
            }
        }

        boolean validateAccountSequence(List<Account> accounts) {
            long border = accounts.get(accounts.size() - 1).getId();
            long distance = border - lastReplicatedId.get();
            if (distance != accounts.size()) {
                log.warn("Gaps in account sequence range: [{}, {}], distance: {}", border, lastReplicatedId.get(), distance);
                return false;
            }
            return true;
        }

        private long checkId(Account acc, long newId) {
            if (newId != acc.getId()) {
                log.error("Replicated account id: {} and source account id: {} doesn't match, this is unrecoverable error");
                throw new IllegalStateException("Account id coherence broken");
            }
            return newId;
        }

        AccountPrototype convertToProto(Account acc) {
            AccountPrototype proto = new AccountPrototype(acc.getCurrSymCode());
            proto.setDescription(acc.getDescription());
            proto.setCreationTime(TypeUtil.temporalToString(acc.getCreationTime()));
            return proto;
        }
    }

    public static class PostingReplicator implements Runnable {

        private static final int BATCH_SIZE = 1000;
        private static final int STALING_TIME = 5000;
        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final ShumwayDAO dao;
        private final AccounterSrv.Iface client;
        private final AtomicLong lastAccountReplicatedId;
        private final AtomicLong lastPostingReplicatedId;
        PostingOperation prevState = HOLD;
        Optional<String> lastPlanId = Optional.empty();
        Optional<Long> lastBatchId = Optional.empty();
        List<Posting> postings = new ArrayList<>();
        List<PostingBatch> batches = new ArrayList<>();

        public PostingReplicator(ShumwayDAO dao, AccounterSrv.Iface client, AtomicLong lastAccountReplicatedId, AtomicLong lastPostingReplicatedId) {
            this.dao = dao;
            this.client = client;
            this.lastAccountReplicatedId = lastAccountReplicatedId;
            this.lastPostingReplicatedId = lastPostingReplicatedId;
        }

        @Override
        public void run() {
            log.info("Start posting replicator from id: {}", lastPostingReplicatedId.get());
            try {
                boolean prevNoData = false;

                while (!Thread.currentThread().isInterrupted()) {
                    log.info("Get postings from id: {}", lastPostingReplicatedId.get());
                    List<PostingLog> postingLogs = dao.getPostingLogs(lastPostingReplicatedId.get(), BATCH_SIZE);
                    if (postingLogs.isEmpty()) {
                        if (prevNoData && !postings.isEmpty()) {
                            applyPrevState(prevState);
                        }
                        log.info("Awaiting new postings on: {}", lastPostingReplicatedId.get());
                        Thread.sleep(STALING_TIME);

                        if (!prevNoData) {
                            prevNoData = true;
                        }
                    } else {
                        if (!validatePostingSequence(postingLogs)) {
                            log.warn("Sequence not validated, awaiting for continuous range on: {}", lastPostingReplicatedId.get());
                            Thread.sleep(STALING_TIME);
                            continue;
                        }
                        if (!validateAccountCoherence(postingLogs)) {
                            log.warn("Posting replication is moving faster than accounts one, awaiting on: {}", lastPostingReplicatedId.get());
                            Thread.sleep(STALING_TIME);
                            continue;
                        }


                        log.info("Extracted {} new postings [{}, {}]", postingLogs.size(), postingLogs.get(0).getId(), postingLogs.get(postingLogs.size() - 1).getId());
                        for (PostingLog postingLog : postingLogs) {
                            log.debug("Processing log record: {}", postingLog);
                            boolean samePlan = isSamePlan(postingLog, lastPlanId);
                            boolean sameBatch = isSameBatch(postingLog, lastBatchId);
                            switch (postingLog.getOperation()) {
                                case HOLD:
                                    switch (prevState) {
                                        case HOLD:
                                            if (!samePlan || !sameBatch) {
                                                batches.add(new PostingBatch(lastBatchId.get(), new ArrayList<>(postings)));
                                                processHoldAndReset(lastPlanId.get(), lastBatchId.get());
                                            }
                                            break;
                                        default:
                                            applyPrevState(prevState);
                                            break;
                                    }
                                    break;
                                case COMMIT:
                                    switch (prevState) {
                                        case COMMIT:
                                            if (!samePlan | !sameBatch) {
                                                batches.add(new PostingBatch(lastBatchId.get(), new ArrayList<>(postings)));
                                                postings.clear();
                                                if (!samePlan) {
                                                    processCommitAndReset(lastPlanId.get());
                                                }
                                            }
                                            break;
                                        default:
                                            applyPrevState(prevState);
                                            break;
                                    }
                                    break;
                                case ROLLBACK:
                                    switch (prevState) {
                                        case ROLLBACK:
                                            if (!samePlan | !sameBatch) {
                                                batches.add(new PostingBatch(lastBatchId.get(), new ArrayList<>(postings)));
                                                postings.clear();
                                                if (!samePlan) {
                                                    processRollbackAndReset(lastPlanId.get());
                                                }
                                            }
                                            break;
                                        default:
                                            applyPrevState(prevState);
                                            break;
                                    }
                                    break;
                            }
                            prevNoData = false;
                            prevState = postingLog.getOperation();
                            postings.add(convertToProto(postingLog));
                            lastPlanId = Optional.of(postingLog.getPlanId());
                            lastBatchId = Optional.of(postingLog.getBatchId());
                            lastPostingReplicatedId.set(postingLog.getId());
                        }
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Posting replicator interrupted");
            } catch (Throwable t) {
                log.error("Posting replicator error", t);
                throw new RuntimeException("Posting replicator error", t);
            } finally {
                log.info("Stop posting replicator on: {}", lastPostingReplicatedId.get());
            }
        }

        void applyPrevState(PostingOperation prevState) throws Exception {
            switch (prevState) {
                case HOLD:
                    batches.add(new PostingBatch(lastBatchId.get(), postings));
                    processHoldAndReset(lastPlanId.get(), lastBatchId.get());
                    break;
                case COMMIT:
                    batches.add(new PostingBatch(lastBatchId.get(), postings));
                    processCommitAndReset(lastPlanId.get());
                    break;
                case ROLLBACK:
                    batches.add(new PostingBatch(lastBatchId.get(), postings));
                    processRollbackAndReset(lastPlanId.get());
                    break;
            }
        }

        boolean validateAccountCoherence(List<PostingLog> postingLogs) {
            for (PostingLog postingLog : postingLogs) {
                long lastAccId = lastAccountReplicatedId.get();
                if (postingLog.getFromAccountId() > lastAccId || postingLog.getToAccountId() > lastAccId) {
                    log.warn("Posting contains account id more than replicated: {}, {}", lastAccId, postingLog);
                    return false;
                }
            }
            return true;
        }

        boolean validatePostingSequence(List<PostingLog> postingLogs) {
            long border = postingLogs.get(postingLogs.size() - 1).getId();
            long distance = border - lastPostingReplicatedId.get();
            if (distance != postingLogs.size()) {
                log.warn("Gaps in posting sequence range: [{}, {}], distance: {}", border, lastPostingReplicatedId.get(), distance);
                return false;
            }
            return true;
        }

        void processHoldAndReset(String lastPlanId, long lastBatchId) throws Exception {
            PostingPlanChange postingPlanChange = new PostingPlanChange(lastPlanId, new PostingBatch(lastBatchId, batches.get(0).getPostings()));
            log.info("Hold: {}", postingPlanChange);
            executeCommand(() -> client.hold(postingPlanChange), postingPlanChange);
            resetLists();
        }

        void processCommitAndReset(String lastPlanId) throws Exception {
            PostingPlan postingPlan = new PostingPlan(lastPlanId, batches);
            log.info("Commit: {}", postingPlan);
            executeCommand(() -> client.commitPlan(postingPlan), postingPlan);
            resetLists();
        }

        void processRollbackAndReset(String lastPlanId) throws Exception {
            PostingPlan postingPlan = new PostingPlan(lastPlanId, batches);
            log.info("Rollback: {}", postingPlan);
            executeCommand(() -> client.rollbackPlan(postingPlan), postingPlan);
            resetLists();
        }

        private void resetLists() {
            postings.clear();
            batches.clear();
        }

        private PostingPlanLog executeCommand(Callable<PostingPlanLog> command, Object data) throws Exception {
            while (true) {
                try {
                    return command.call();
                } catch (WUndefinedResultException | WUnavailableResultException e) {
                    log.warn("Temporary command error, retry", e);
                    Thread.sleep(STALING_TIME);
                    continue;
                } catch (Exception e) {
                    log.error("Failed to execute command with data: {}", data);
                    throw e;
                }
            }
        }


        boolean isSamePlan(PostingLog postingLog, Optional<String> lastPlanId) {
            return lastPlanId.map(p -> p.equals(postingLog.getPlanId())).orElse(true);
        }

        boolean isSameBatch(PostingLog postingLog, Optional<Long> lastBatchId) {
            return lastBatchId.map(p -> p.equals(postingLog.getBatchId())).orElse(true);//postingLog.getBatchId() == lastBatchId
        }

        Posting convertToProto(PostingLog postingLog) {
            return new Posting(postingLog.getFromAccountId(), postingLog.getToAccountId(), postingLog.getAmount(), postingLog.getCurrSymCode(), postingLog.getDescription());
        }
    }

}

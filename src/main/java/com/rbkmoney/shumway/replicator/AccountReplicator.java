package com.rbkmoney.shumway.replicator;

import com.rbkmoney.damsel.accounter.AccountPrototype;
import com.rbkmoney.damsel.accounter.AccounterSrv;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.shumway.replicator.domain.Account;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by vpankrashkin on 19.06.18.
 */
public class AccountReplicator implements Runnable {
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

package com.rbkmoney.shumway.replicator.service;

import com.rbkmoney.damsel.accounter.Account;
import com.rbkmoney.damsel.accounter.AccounterSrv;
import com.rbkmoney.damsel.shumpune.AccountNotFound;
import com.rbkmoney.damsel.shumpune.Balance;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.damsel.shumpune.LatestClock;
import com.rbkmoney.shumway.replicator.dao.ShumwayDAO;
import com.rbkmoney.shumway.replicator.domain.replication.Status;
import com.rbkmoney.shumway.replicator.domain.verification.AccountCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

@Slf4j
@Service
@RequiredArgsConstructor
public class VerificationService {

    private final com.rbkmoney.damsel.shumpune.AccounterSrv.Iface shumpuneClient;
    private final AccounterSrv.Iface shumwayClient;
    private final ShumwayDAO shumwayDAO;

    private Status status = Status.NOT_STARTED;
    private List<Long> invalidAccounts = new ArrayList<>();
    private Long totalAccountsCount;
    private final AtomicLong currentAcc = new AtomicLong(0);

    public void start() {
        new Thread(() -> {
            log.info("Verification started");
            status = Status.IN_PROGRESS;
            totalAccountsCount = shumwayDAO.totalAccountsCount();
            LongStream.range(0, totalAccountsCount)
//                    .parallel()
                    .forEach(i -> {
                        try {
                            currentAcc.set(i);
                            Balance balance = shumpuneClient.getBalanceByID(i, Clock.latest(new LatestClock()));
                            Account account = shumwayClient.getAccountByID(i);
                            if (!(balance.getOwnAmount() == account.getOwnAmount()
                                    && balance.getMinAvailableAmount() == account.getMinAvailableAmount()
                                    && balance.getMaxAvailableAmount() == account.getMaxAvailableAmount())) {
                                log.warn("Invalid account number: {}\n" +
                                        "shumpune own -  {} / min - {} / max - {}\n" +
                                        "shumway own - {} / min - {} / max - {}.", i, balance.getOwnAmount(),
                                        balance.getMinAvailableAmount(),
                                        balance.getMaxAvailableAmount(),
                                        account.getOwnAmount(),
                                        account.getMinAvailableAmount(),
                                        account.getMaxAvailableAmount());
                                invalidAccounts.add(i);
                            } else {
                                log.info("Account valid: {}", i);
                            }
                        } catch (Throwable t) {
                            log.error("Verification error, accNum: {}", i, t);
//                            throw new RuntimeException("Verification error", t);
                        }
                    });
            if (invalidAccounts.size() > 0) {
                status = Status.ERROR;
                log.error("Invalid accounts found: " + invalidAccounts);
            } else {
                status = Status.FINISHED;
            }
            log.info("Verification finished");
        }).start();
    }

    public AccountCheckResult status() {
        return AccountCheckResult.builder()
                .status(status)
                .accountNumber(currentAcc.get())
                .totalAccountAmount(totalAccountsCount)
                .invalidAccounts(invalidAccounts)
                .build();
    }
}

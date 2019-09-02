package com.rbkmoney.shumway.replicator.controller;

import com.rbkmoney.shumway.replicator.domain.replication.StatusCheckResult;
import com.rbkmoney.shumway.replicator.service.ReplicatorService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping("/replication")
@RequiredArgsConstructor
public class ReplicationController {

    private final ReplicatorService replicatorService;

    @GetMapping("/start")
    public void start() {
        replicatorService.fire();
    }

    @GetMapping("/status")
    public StatusCheckResult status() {
        return replicatorService.status();
    }

}

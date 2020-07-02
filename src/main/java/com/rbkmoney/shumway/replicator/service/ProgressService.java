package com.rbkmoney.shumway.replicator.service;

import com.rbkmoney.shumway.replicator.domain.db.Progress;
import com.rbkmoney.shumway.replicator.domain.db.repo.ProgressRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ProgressService {

    public static final String POSTING_ID = "posting_id";

    private final ProgressRepo progressRepo;

    @Transactional
    public void saveProgess(long replicationPostingsCount) {
        progressRepo.save(new Progress(POSTING_ID, replicationPostingsCount));
    }

    public Progress getProgress() {
        return progressRepo.getOne(POSTING_ID);
    }

}

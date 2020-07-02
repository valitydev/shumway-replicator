package com.rbkmoney.shumway.replicator.domain.db.repo;

import com.rbkmoney.shumway.replicator.domain.db.Progress;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProgressRepo extends JpaRepository<Progress, String> {
}

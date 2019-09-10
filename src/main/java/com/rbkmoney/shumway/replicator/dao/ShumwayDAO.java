package com.rbkmoney.shumway.replicator.dao;

import com.rbkmoney.shumway.replicator.domain.Account;
import com.rbkmoney.shumway.replicator.domain.PostingLog;
import com.rbkmoney.shumway.replicator.domain.PostingOperation;
import com.rbkmoney.shumway.replicator.exception.DAOException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.NestedRuntimeException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Created by vpankrashkin on 11.05.18.
 */
@Component
public class ShumwayDAO extends JdbcDaoSupport  {
    private static final String sqlAcc = "SELECT id, curr_sym_code, creation_time, description FROM shm.account WHERE id > ? order by id limit ?";
    private static final String sqlPst = "select id, plan_id, batch_id, from_account_id, to_account_id, creation_time, amount, curr_sym_code, operation, description from shm.posting_log where id > ? order by id limit ?";

    private final AccountMapper accountMapper = new AccountMapper();
    private final PostingLogMapper postingLogMapper = new PostingLogMapper();

    @Autowired
    public ShumwayDAO(DataSource ds) {
        setDataSource(ds);
    }

    public Long totalAccountsCount() throws DAOException {
        try {
            return getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM shm.account", Long.class);
        } catch (NestedRuntimeException e) {
            throw new DAOException(e);
        }
    }

    public Long totalPostingsCount() throws DAOException {
        try {
            return getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM shm.posting_log", Long.class);
        } catch (NestedRuntimeException e) {
            throw new DAOException(e);
        }
    }

    public List<Account> getAccounts(long fromId, int limit) throws DAOException {
        try {
            return getJdbcTemplate().query(sqlAcc, new Object[]{fromId, limit}, accountMapper);
        } catch (NestedRuntimeException e) {
            throw new DAOException(e);
        }
    }

    public List<PostingLog> getPostingLogs(long fromId, int limit) throws DAOException {
        try {
            return getJdbcTemplate().query(sqlPst, new Object[]{fromId, limit}, postingLogMapper);
        } catch (NestedRuntimeException e) {
            throw new DAOException(e);
        }
    }

    private static class AccountMapper implements RowMapper<Account> {
        @Override
        public Account mapRow(ResultSet rs, int i) throws SQLException {
            long id = rs.getLong(1);
            String currSymCode = rs.getString(2);
            Instant creationTime = rs.getObject(3, LocalDateTime.class).toInstant(ZoneOffset.UTC);
            String description = rs.getString(4);
            return new Account(id, creationTime, currSymCode, description);
        }
    }

    private static class PostingLogMapper implements RowMapper<PostingLog> {
        @Override
        public PostingLog mapRow(ResultSet rs, int rowNum) throws SQLException {
            long id = rs.getLong(1);
            String planId = rs.getString(2);
            long batchId = rs.getLong(3);
            long fromAccountId = rs.getLong(4);
            long toAccountId = rs.getLong(5);
            Instant creationTime = rs.getTimestamp(6).toInstant();
            long amount = rs.getLong(7);
            String currSymCode = rs.getString(8);
            PostingOperation operation = PostingOperation.getValueByKey(rs.getString(9));
            String description = rs.getString(10);
            return new PostingLog(id, planId, batchId, fromAccountId, toAccountId, amount, creationTime, operation, currSymCode, description);
        }
    }
}

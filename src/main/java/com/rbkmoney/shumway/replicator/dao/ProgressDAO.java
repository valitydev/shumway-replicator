package com.rbkmoney.shumway.replicator.dao;

import com.rbkmoney.shumway.replicator.exception.DAOException;
import org.springframework.core.NestedRuntimeException;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.PreparedStatement;

public class ProgressDAO extends JdbcDaoSupport {

    public ProgressDAO(DataSource dataSource) {
        setDataSource(dataSource);
    }

    @Transactional
    public void saveProgess(long replicationPostingsCount) {
        try {
            getJdbcTemplate().update(connection ->
            {
                final String sql = "update shmr.progress set latest_posting=? where id='posting_id'";
                final PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setLong(1, replicationPostingsCount);;
                return preparedStatement;
            });
        } catch (NestedRuntimeException e) {
            throw new DAOException(e);
        }
    }

    public long getProgress() {
        try {
            return getJdbcTemplate().queryForObject("select latest_posting from shmr.progress where id='posting_id'", Long.class);
        } catch (NestedRuntimeException e) {
            throw new DAOException(e);
        }
    }
}

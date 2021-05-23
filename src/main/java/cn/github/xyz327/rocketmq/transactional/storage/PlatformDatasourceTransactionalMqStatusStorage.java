package cn.github.xyz327.rocketmq.transactional.storage;

import cn.github.xyz327.rocketmq.transactional.TransactionMsgSendResult;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.spi.SessionImplementor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.ClassUtils;

import javax.persistence.EntityManager;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author xizhou
 * @since 2019/11/8 15:13
 */
@Slf4j
public class PlatformDatasourceTransactionalMqStatusStorage implements TransactionalMqStatusStorage {
    private final NamedParameterJdbcTemplate extJdbcTemplate;
    private final PlatformTransactionManager transactionManager;
    @Getter
    @Setter
    private String tableName = "trans_mq_status";
    private final String recordSql =
        "insert into trans_mq_status (msg_id, offset_msg_id, queue_offset, queue_id, topic, local_id, status, create_time) " +
        "values(?,?,?,?,?,?,?,?)";
    private final String updateSql = "update trans_mq_status set status = ? where msg_id=?";
    private final String queryByMsgIdSql = "select status from trans_mq_status where msg_id=?";
    private final String queryListByMsgIdSql = "select msg_id, local_id, status from trans_mq_status where msg_id in (:ids)";
    private final String deleteMsgSql = "delete from trans_mq_status where msg_id in (:ids) ";

    public static enum ConnectionProviderRegistry {
        /**
         * 单例
         */
        Instance;
        static {
            ClassLoader classLoader = PlatformDatasourceTransactionalMqStatusStorage.class.getClassLoader();
            if(ClassUtils.isPresent("org.springframework.orm.jpa.JpaTransactionManager", classLoader)) {
                ConnectionProviderRegistry.Instance.registryConnectionProvider(new JpaConnectionProvider());
            }
            if(ClassUtils.isPresent("org.springframework.jdbc.datasource.DataSourceTransactionManager", classLoader)) {
                ConnectionProviderRegistry.Instance.registryConnectionProvider(new JdbcConnectionProvider());
            }
        }
        private List<ConnectionProvider> connectionProviders = new ArrayList<>();
        public Connection getConnection(PlatformTransactionManager transactionManager){
            Connection connection = connectionProviders.stream()
                .filter(provider -> provider.supports(transactionManager))
                .findFirst()
                .orElse(NoneConnectionProvider.instance)
                .getConnection(transactionManager);
            if(connection == null){
                throw new UnsupportedOperationException("不支持的transactionManager:" + transactionManager.getClass().getName());
            }
            return connection;
        }
        public void registryConnectionProvider(ConnectionProvider connectionProvider){
            connectionProviders.add(connectionProvider);
        }
    }
    public static class NoneConnectionProvider implements ConnectionProvider{
        static ConnectionProvider instance = new NoneConnectionProvider();
        @Override
        public boolean supports(PlatformTransactionManager transactionManager) {
            return false;
        }

        @Override
        public Connection getConnection(PlatformTransactionManager transactionManager) {
            return null;
        }
    }
    public static interface ConnectionProvider {
        boolean supports(PlatformTransactionManager transactionManager);

        Connection getConnection(PlatformTransactionManager transactionManager);
    }

    public static class JpaConnectionProvider implements ConnectionProvider {
        @Override
        public boolean supports(PlatformTransactionManager transactionManager) {
            return transactionManager instanceof JpaTransactionManager;
        }

        @Override
        public Connection getConnection(PlatformTransactionManager transactionManager) {
            JpaTransactionManager jpaTransactionManager = (JpaTransactionManager) transactionManager;
            EntityManagerHolder emHolder =
                (EntityManagerHolder) TransactionSynchronizationManager.getResource(jpaTransactionManager.getEntityManagerFactory());
            EntityManager entityManager = emHolder.getEntityManager();
            return getConnectionFromEntityManager(entityManager);
        }


        private Connection getConnectionFromEntityManager(EntityManager entityManager) {
            SessionImplementor session = entityManager.unwrap(SessionImplementor.class);
            Connection connection = session.connection();
            try {
                if (connection.isClosed()) {
                    throw new IllegalStateException("获取connection必须在事务内执行");
                }
            } catch (SQLException e) {
                throw new IllegalStateException("获取数据链接失败", e);
            }
            return connection;
        }
    }

    public static class JdbcConnectionProvider implements ConnectionProvider {

        @Override
        public boolean supports(PlatformTransactionManager transactionManager) {
            return transactionManager instanceof DataSourceTransactionManager;
        }

        @Override
        public Connection getConnection(PlatformTransactionManager transactionManager) {
            if (transactionManager instanceof DataSourceTransactionManager) {
                DataSourceTransactionManager dataSourceTransactionManager = (DataSourceTransactionManager) transactionManager;
                ConnectionHolder conHolder =
                    (ConnectionHolder) TransactionSynchronizationManager.getResource(dataSourceTransactionManager.getDataSource());
                return conHolder.getConnection();
            }
            return null;
        }
    }

    public PlatformDatasourceTransactionalMqStatusStorage(DataSource dataSource, PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        extJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }
    
    @Override
    public String getName() {
        return "db";
    }
    
    @Override
    public void recordTransaction(String transactionId, Boolean status) throws RuntimeException {
    
    }
    
    @Override
    public void recordTransaction(List<String> transactionIds, Boolean status) throws RuntimeException {
    
    }
    
    @Override
    public void recordTransaction(List<TransactionMsgSendResult> sendResults, Boolean status, String localTransactionId) {
        try {
            if (localTransactionId == null) {
                localTransactionId = "";
            }
            Date now = new Date();
            String finalLocalTransactionId = localTransactionId;
            List<Object[]> dbArgs = sendResults.stream().map(sendResult -> {
                StorageInfo storageInfo = StorageInfo.formSendResult(sendResult, status);
                return new Object[] {storageInfo.getMsgId(), storageInfo.getOffsetMsgId(), storageInfo.getQueueOffset(),
                                     storageInfo.getQueueId(), sendResult.getTopic(), finalLocalTransactionId, status, now};
            }).collect(Collectors.toList());
            Connection connection = getLocalTransactionConnection();
            JdbcTemplate jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            
            jdbcTemplate.batchUpdate(recordSql, dbArgs);
        } catch (Exception e) {
            throw new IllegalStateException("记录本地事务状态异常: " + e.getMessage(), e);
        }
    }
    
    private Connection getLocalTransactionConnection() {
        return ConnectionProviderRegistry.Instance.getConnection(transactionManager);
    }
    
    @Override
    public Boolean getTransactionStatus(String transactionId) {
        try {
            return extJdbcTemplate.getJdbcOperations().queryForObject(queryByMsgIdSql, Boolean.class, transactionId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
    
    @Override
    public void removeTransactionStatus(String transactionId) {
    
    }
    
    @Override
    public void removeTransactionStatus(List<String> msgIds) {
        if (msgIds.size() > 0) {
            extJdbcTemplate.update(deleteMsgSql, Collections.singletonMap("ids", msgIds));
        }
    }
    
    @Override
    public void updateRecordTransaction(String transactionId, int reconsumeTimes) {
    
    }
    
    @Override
    public Map<String, Boolean> getTransactionStatus(List<String> transactionIds) {
        List<RecordInfo> recordInfos = extJdbcTemplate.query(queryListByMsgIdSql, Collections.singletonMap("ids", transactionIds),
                                                             BeanPropertyRowMapper.newInstance(RecordInfo.class));
        
        return recordInfos.stream().collect(Collectors.toMap(RecordInfo::getMsgId, RecordInfo::getStatus));
    }
    
    @Override
    public void updateTransaction(List<TransactionMsgSendResult> sendResults, boolean status, String localTransactionId) {
        List<Object[]> dbArgs = sendResults.stream().map(sendResult -> {
            return new Object[] {status, sendResult.getTransactionId()};
        }).collect(Collectors.toList());
        try {
            extJdbcTemplate.getJdbcOperations().batchUpdate(updateSql, dbArgs);
        } catch (DataAccessException e) {
            log.error("更新消息事务记录状态失败: status:{}, msgSendResult: [{}], localTransactionId:{} cause: {} ", status, localTransactionId,
                      sendResults, e.getMessage(), e);
        }
    }
    
    @Data
    public static class RecordInfo {
        private String id;
        private String msgId;
        private String localId;
        private Boolean status;
        private Date createTime;
    }
}

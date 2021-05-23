package cn.github.xyz327.rocketmq.transactional;

import cn.github.xyz327.rocketmq.transactional.storage.TransactionalMqStatusStorage;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.transaction.TransactionProperties;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 事务消息回调处理， spring事务状态监听
 * @author <a href="mailto:xyz327@outlook.com">xizhou</a>
 * @since 2020/10/22 4:30 下午
 */
@Slf4j
public class TransactionalMqListener implements TransactionSynchronization, InitializingBean {
    
    /**
     * 当commit过程中发生了 {@link TransactionException} 时,是否要执行回滚, 此开关默认关闭。<br/>
     * 这个使用 {@code spring.transaction.rollback-on-commit-failure} 属性做配置
     */
    private Boolean rollbackOnCommitFailure;
    
    @Autowired
    private TransactionalMqStatusStorage statusStorage;
    @Autowired
    private TransactionalRocketMqProducer transactionalRocketMqProducer;
    @Autowired(required = false)
    private TransactionProperties transactionProperties;
    
    @Override
    public void afterPropertiesSet() throws Exception {
        if (transactionProperties != null) {
            rollbackOnCommitFailure = transactionProperties.getRollbackOnCommitFailure();
        }
    }
    public void handleTransactionStart() {
        registerSynchronization();
    }

    private void registerSynchronization() {
        // 注册spring 的事务状态监听器
        TransactionSynchronizationManager.registerSynchronization(this);
        
    }
    
    @Override
    public void suspend() {
    
    }
    
    @Override
    public void resume() {
    }
    
    @Override
    public void flush() {
    
    }
    
    TransactionTemplate transactionTemplate;
    
    @Override
    @SuppressWarnings("unchecked")
    public void beforeCommit(boolean readOnly) {
        if (log.isDebugEnabled()) {
            log.debug("TransactionMQListenerImpl.beforeCommit: readOnly: {}", readOnly);
        }
        if (readOnly) {
            // 只读事务, 不会有数据修改.
            return;
        }
        
        List<TransactionMsgSendResult> sendResults =
            (List<TransactionMsgSendResult>)TransactionSynchronizationManager.getResource(TransactionalMessageConsts.TRANSACTION_HALF_MSGS);
        if (sendResults == null || sendResults.isEmpty()) {
            return;
        }
        List<String> topics = new ArrayList<>();
        List<String> msgIds = new ArrayList<>();
        
        for (TransactionMsgSendResult sendResult : sendResults) {
            topics.add(sendResult.getTopic());
            msgIds.add(sendResult.getTransactionId());
        }
        
        if (sendResults.size() > 1) {
            log.warn("[TRANS_MQ]请不要在一个本地事务内发多个事务消息 topic:[{}] , msgId:[{}]", topics, msgIds);
        }
    }
    
    @Override
    public void beforeCompletion() {
    }
    
    /**
     * 此时表示事务成功提交之后的回调
     */
    @Override
    public void afterCommit() {
    }
    
    /**
     * Completion status in case of proper commit
     * <p>
     * <p>
     * int STATUS_COMMITTED = 0;
     * <p>
     * Completion status in case of proper rollback
     * <p>
     * int STATUS_ROLLED_BACK = 1;
     * <p>
     * Completion status in case of heuristic mixed completion or system errors
     * <p>
     * int STATUS_UNKNOWN = 2;
     * <p>
     * <p>
     * <p>
     * 事务完成后的回调。此时事务有三种状态
     * commit: 表示事务提交成功
     * rollback: 表示事务回滚成功
     * unknown: 此时事务可能成功也可能失败,
     * 当执行 commit 操作过程中发生 {@link TransactionException}异常时 就会是unknown状态
     * 当执行rollback 操作过程中发生 {@link Error} 时 就会是unknown状态
     *
     * @param status
     */
    @Override
    @SuppressWarnings("unchecked")
    public void afterCompletion(int status) {
        
        String msgId = "";
        try {
            List<String> transactionIds = new ArrayList<>();
            if (!TransactionSynchronizationManager.hasResource(TransactionalMessageConsts.TRANSACTION_HALF_MSGS)) {
                return;
            }
            List<TransactionMsgSendResult> sendResults =
                (List<TransactionMsgSendResult>)TransactionSynchronizationManager.getResource(TransactionalMessageConsts.TRANSACTION_HALF_MSGS);
            if (sendResults == null) {
                return;
            }
            for (TransactionMsgSendResult sendResult : sendResults) {
                transactionIds.add(sendResult.getTransactionId());
            }
               log.info("事务执行状态 status: [{}], [{}], msgIds:[{}]", status,
                      //
                      (status == STATUS_COMMITTED ? "STATUS_COMMITTED" :
                           status == STATUS_ROLLED_BACK ? "STATUS_ROLLED_BACK" : "STATUS_UNKNOWN"), transactionIds);
            if (!sendResults.isEmpty()) {
                List<String> msgIds = new ArrayList<>();
                List<String> topics = new ArrayList<>();
                boolean needRemoveRecords = false;
                switch (status) {
                    case STATUS_COMMITTED:
                        
                        for (TransactionMsgSendResult sendResult : sendResults) {
                            msgId = sendResult.getTransactionId();
                            doCheckTransMsg(sendResult, true);
                            msgIds.add(msgId);
                            topics.add(sendResult.getTopic());
                        }

                        needRemoveRecords = true;
                        break;
                    case STATUS_ROLLED_BACK:
                        for (TransactionMsgSendResult sendResult : sendResults) {
                            msgId = sendResult.getTransactionId();
                            doCheckTransMsg(sendResult, false);
                        }
                        // 如果本地事务失败 事务会回滚，那么也不会存在记录
                        break;
                    case STATUS_UNKNOWN:
                    default:
                        // 此时 本地事务的状态不确定, 那就从状态管理器里面重新确认本地事务执行状态
                        log.warn("[TRANS_MQ]事务消息 本地事务状态[{}], 无法确认本地事务状态,从事务状态保存器[{}]中重新查询事务执行状态", "STATUS_UNKNOWN",
                                  statusStorage.getName());
                        Map<String, Boolean> statusMap = statusStorage.getTransactionStatus(transactionIds);
                        for (TransactionMsgSendResult sendResult : sendResults) {
                            msgId = sendResult.getTransactionId();
                            Boolean msgStatus = statusMap.get(msgId);
                            doCheckTransMsg(sendResult, msgStatus);
                            msgIds.add(msgId);
                            topics.add(sendResult.getTopic());
                        }
                        needRemoveRecords = true;
                        break;
                }
                if(needRemoveRecords){
                    try {
                        // 确认成功后删除保存的记录
                        statusStorage.removeTransactionStatus(msgIds);
                    } catch (Exception e) {
                        log.warn("[TRANS_MQ] 删除事务消息状态表记录出现异常.不会影响正常流程,msgIds:[{}], topics:[{}], 异常:[{}]", msgIds, topics, e.getMessage(), e);
                    }
                }
            }
            
        } catch (Exception e) {
            log.error("[TRANS_MQ][{}]事务消息确认异常:[{}]", msgId, e.getMessage(), e);
        } finally {
            // 清理上下文
            TransactionSynchronizationManager.unbindResourceIfPossible(TransactionalMessageConsts.TRANSACTION_HALF_MSGS);
        }
        
    }
    
    private void doCheckTransMsg(TransactionMsgSendResult sendResult, Boolean msgStatus) {
        LocalTransactionState transactionState = msgStatus ==
                                                 null ? LocalTransactionState.UNKNOW : msgStatus ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
        log.info("[TRANS_MQ]事务消息:[{}], topic:[{}] 执行状态检查,状态:[{}]", sendResult.getTransactionId(), sendResult.getTopic(), transactionState);
        transactionalRocketMqProducer.endHalfTransactionMsg(sendResult.getSendResult(), transactionState, null);
        
    }
}
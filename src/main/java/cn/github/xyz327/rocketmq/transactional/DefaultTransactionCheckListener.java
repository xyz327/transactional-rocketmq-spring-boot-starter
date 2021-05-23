package cn.github.xyz327.rocketmq.transactional;

import cn.github.xyz327.rocketmq.transactional.storage.TransactionalMqStatusStorage;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.NumberUtils;

/**
 * @author xizhou
 * @since 2019/3/4 15:26
 */
@Slf4j
public class DefaultTransactionCheckListener implements TransactionCheckListener, LocalTransactionExecuter, TransactionListener {
    private int maxCheckTime = 15;
    @Autowired
    private TransactionalMqStatusStorage statusStorage;

    
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        return LocalTransactionState.UNKNOW;
    }
    
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // 从消息中恢复crid
        String topic = msg.getTopic();
        String transactionId = msg.getTransactionId();
        Boolean transactionStatus = null;
        Exception err = null;
        String checkTimes = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (checkTimes != null) {
            checkTime = NumberUtils.parseNumber(checkTimes, Integer.class);
            if (checkTime >= maxCheckTime) {
                log.warn("[TRANS_MQ]事务消息[{}],topic:[{}]超过最大回查次数:[{}], 当前次数:[{}]", transactionId, topic, maxCheckTime, checkTime);
            }
        }
        try {
            transactionStatus = statusStorage.getTransactionStatus(transactionId);
            if (transactionStatus == null) {
                log.warn("[TRANS_MQ]事务消息:[{}] 的执行记录在[{}]中找不到记录, 可能是本地事务失败: 回滚事务消息", transactionId, statusStorage.getName());
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
            if (transactionStatus) {
                log.info("[TRANS_MQ] 回查事务消息[{}]状态为成功,确认事务消息", transactionId);
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            log.info("[TRANS_MQ] 回查事务消息[{}]状态失败,回滚事务消息", transactionId);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } catch (Exception e) {
            err = e;
        } finally {
            //@formatter:off
            log.info("[TRANS_MQ]回查事务消息 状态:[{}] topic:[{}], msgId：[{}], commitLog:[{}], preparedTransactionOffset:[{}],queueOffset:[{}],  " +
                      "properties: [{}], 半消息的 msgId:[{}]",
                      transactionStatus == null ? LocalTransactionState.UNKNOW : transactionStatus ? LocalTransactionState.COMMIT_MESSAGE
                          :LocalTransactionState.ROLLBACK_MESSAGE,
                      msg.getTopic(),
                      msg.getTransactionId(),
                      msg.getCommitLogOffset(),
                      msg.getPreparedTransactionOffset(),
                      msg.getQueueOffset(),
                      msg.getProperties(),
                      msg.getMsgId());
            if (err != null) {
                log.error("[TRANS_MQ]事务消息:[{}] 回查执行出现异常:[{}],等待重试回查 [{}]", transactionId, err.getMessage(), msg, err);
            } else if (transactionStatus != null && transactionStatus) {
                // 确认成功,删除redis的记录
                statusStorage.removeTransactionStatus(transactionId);
            }
            if (transactionStatus == null || !transactionStatus) {
                // 没有确认成功
                statusStorage.updateRecordTransaction(transactionId, checkTime);
            }
        }
        return LocalTransactionState.UNKNOW;
    }
    
    @Override
    public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
        return checkLocalTransaction(msg);
    }
    
    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
        return executeLocalTransaction(msg, arg);
    }
}

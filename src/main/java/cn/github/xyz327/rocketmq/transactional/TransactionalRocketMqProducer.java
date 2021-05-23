package cn.github.xyz327.rocketmq.transactional;

import cn.github.xyz327.rocketmq.transactional.storage.StorageInfo;
import cn.github.xyz327.rocketmq.transactional.storage.TransactionalMqStatusStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author <a href="mailto:xyz327@outlook.com">xizhou</a>
 * @since 2021/5/21 9:26 下午
 */
@Slf4j
@RequiredArgsConstructor
public class TransactionalRocketMqProducer implements InitializingBean {
    private final TransactionMQProducer transactionMQProducer;

    @Autowired
    private TransactionalMqStatusStorage transactionalMqStatusStorage;
    @Autowired
    private DefaultTransactionCheckListener defaultTransactionCheckListener;

    @Override
    public void afterPropertiesSet() throws Exception {
        transactionMQProducer.setTransactionListener(defaultTransactionCheckListener);
    }

    private void checkLocalTransaction() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            log.error("不存在本地事务, 不能发送事务消息!");
            throw new IllegalStateException("不存在本地事务, 不能发送事务消息!");
        }
    }

    @SuppressWarnings("unchecked")
    public TransactionMsgSendResult sendTransactionalMessage(Message msg) throws MQClientException {
        checkLocalTransaction();
        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.transactionMQProducer);

        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.transactionMQProducer.getProducerGroup());
        try {
            sendResult = this.transactionMQProducer.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        // 将发送结果绑定到事务上下文
        TransactionMsgSendResult transactionMsgSendResult =
                new TransactionMsgSendResult(transactionMQProducer.getProducerGroup(), msg.getTopic(), sendResult.getMsgId(), sendResult);
        List<TransactionMsgSendResult> sendResults;
        if (!TransactionSynchronizationManager.hasResource(TransactionalMessageConsts.TRANSACTION_HALF_MSGS)) {
            sendResults = new ArrayList<>();
            TransactionSynchronizationManager.bindResource(TransactionalMessageConsts.TRANSACTION_HALF_MSGS, sendResults);
        } else {
            // unchecked
            sendResults = (List<TransactionMsgSendResult>) TransactionSynchronizationManager.getResource(
                    TransactionalMessageConsts.TRANSACTION_HALF_MSGS);
        }
        transactionalMqStatusStorage.recordTransaction(Collections.singletonList(transactionMsgSendResult), true, "");
        Objects.requireNonNull(sendResults).add(transactionMsgSendResult);
        return transactionMsgSendResult;
    }


    public void endHalfTransactionMsg(SendResult sendResult,
                                      LocalTransactionState localTransactionState, Throwable localException) {
        try {
            transactionMQProducer.getDefaultMQProducerImpl().endTransaction(sendResult, localTransactionState, localException);
            log.info("[TRANS_MQ] topic:{} 半消息:{} {} ", sendResult.getMessageQueue().getTopic(), sendResult.getMsgId(),
                    localTransactionState == LocalTransactionState.COMMIT_MESSAGE ? "确认成功" :
                            localTransactionState == LocalTransactionState.ROLLBACK_MESSAGE ? "回滚成功" : "无法确认状态");
        } catch (Exception e) {
            log.warn("[TRANS_MQ]  结束半消息失败！ 结束状态:" + localTransactionState + ".", e);
        }
    }

    /**
     * 如果消息回查超过了最大次数后可以通过本地保存的消息信息来继续操作半消息
     *
     * @param storageInfo
     * @param localException
     * @param localException
     */
    public void endHalfTransactionMsg(StorageInfo storageInfo, LocalTransactionState localTransactionState,
                                      Throwable localException) {

        try {
            MessageExt messageExt =
                    transactionMQProducer.getDefaultMQProducerImpl().queryMessageByUniqKey(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC,
                            storageInfo.getMsgId());

            MessageQueue messageQueue = new MessageQueue(storageInfo.getTopic(), "", storageInfo.getQueueId());

            SendResult sendResult =
                    new SendResult(SendStatus.SEND_OK, messageExt.getMsgId(), null, messageQueue, messageExt.getQueueOffset());
            sendResult.setTransactionId(messageExt.getTransactionId());
            sendResult.setOffsetMsgId(storageInfo.getOffsetMsgId());
            this.endHalfTransactionMsg(sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.error("[TRANS_MQ]手动操作事务半消息失败:[{}], status:[{}] ", storageInfo, localTransactionState, e);
            throw new RuntimeException(e);
        }
    }
}

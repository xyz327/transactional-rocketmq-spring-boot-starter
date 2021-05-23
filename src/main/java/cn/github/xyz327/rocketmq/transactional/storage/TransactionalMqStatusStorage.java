package cn.github.xyz327.rocketmq.transactional.storage;


import cn.github.xyz327.rocketmq.transactional.TransactionMsgSendResult;

import java.util.List;
import java.util.Map;

/**
 * @author xizhou
 * @since 2019/7/16 9:55
 */
public interface TransactionalMqStatusStorage {
    /**
     * storage name
     * @return name
     */
    String getName();
    // ================需要和本地事务的操作绑定(使用本地事务的资源)============================
    
    /**
     * 记录事务消息的状态
     * @param transactionId 消息id
     * @param status  本地事务的状态 成功/失败
     * @throws RuntimeException
     */
    void recordTransaction(String transactionId, Boolean status) throws RuntimeException;
    
    /**
     * 记录多个消息的状态
     * @param transactionIds
     * @param status
     * @throws RuntimeException
     */
    @Deprecated
    void recordTransaction(List<String> transactionIds, Boolean status) throws RuntimeException;
    
    /**
     * 记录多个消息的状态
     * @param transactionIds
     * @param status
     * @param localTransactionId
     */
    void recordTransaction(List<TransactionMsgSendResult> transactionIds, Boolean status, String localTransactionId);
    
    // ============================================
    
    //================不能使用本地事务的资源================================
    
    /**
     * 更具本地事务消息状态表获取事务是否成功
     * @param transactionId
     * @return
     */
    Boolean getTransactionStatus(String transactionId);
    
    /**
     * 删除本地事务消息状态表记录
     * @param transactionId
     */
    void removeTransactionStatus(String transactionId);
    
    /**
     * 删除本地事务消息状态表记录
     * @param msgIds
     */
    void removeTransactionStatus(List<String> msgIds);
    /**
     * 更新
     *
     * @param transactionId
     * @param reconsumeTimes
     */
    void updateRecordTransaction(String transactionId, int reconsumeTimes);
    
    /**
     * 获取多个消息的状态
     * @param transactionIds
     * @return
     */
    Map<String, Boolean> getTransactionStatus(List<String> transactionIds);
    
    /**
     * 更新消息的状态
     * @param sendResults
     * @param status
     * @param localTransactionId
     */
    void updateTransaction(List<TransactionMsgSendResult> sendResults, boolean status, String localTransactionId);
    
    
    
    //================================================
}

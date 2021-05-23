package cn.github.xyz327.rocketmq.transactional.storage;

import cn.github.xyz327.rocketmq.transactional.TransactionMsgSendResult;

import java.util.List;
import java.util.Map;

/**
 *
 * @author xizhou
 * @since 2019/7/16 9:56
 */
public class UnSupportsTransactionalMqStatusStorage implements TransactionalMqStatusStorage {
    @Override
    public String getName() {
        throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
    }
    
    @Override
	public void recordTransaction(String transactionId, Boolean status) {
		throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
	}

	@Override
	public void recordTransaction(List<String> transactionIds, Boolean status) {
		throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
	}

	@Override
	public Boolean getTransactionStatus(String transactionId) {
		throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
	}

	@Override
	public void removeTransactionStatus(String transactionId) {
		throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
	}
    
    @Override
    public void removeTransactionStatus(List<String> msgIds) {
        throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
    }
    
    @Override
    public void updateRecordTransaction(String transactionId, int reconsumeTimes) {
        throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
    }
    
    @Override
    public void recordTransaction(List<TransactionMsgSendResult> transactionIds, Boolean status, String localTransactionId) {
        throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
    }
    
    @Override
    public Map<String, Boolean> getTransactionStatus(List<String> transactionIds) {
        throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
    }
    
    @Override
    public void updateTransaction(List<TransactionMsgSendResult> sendResults, boolean status, String localTransactionId) {
        throw new UnsupportedOperationException("请配置事务消息储存,例如 TransactionMQRedisStorage");
    }
}

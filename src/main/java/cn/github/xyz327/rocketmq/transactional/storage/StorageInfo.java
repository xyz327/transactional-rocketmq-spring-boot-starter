package cn.github.xyz327.rocketmq.transactional.storage;

import cn.github.xyz327.rocketmq.transactional.TransactionMsgSendResult;
import lombok.Data;
import org.apache.rocketmq.client.producer.SendResult;

import java.util.Date;

/**
 * @author xizhou
 * @since 2019/11/14 10:15
 */
@Data
public class StorageInfo {
    // ======message context
    
    private String msgId;
    private String offsetMsgId;
    private long queueOffset;
    private int queueId;
    private String topic;
    
    // ======message context
    /**
     * 本地事务执行状态
     */
    private Boolean status;
    
    private Date createTime;
    
    public static StorageInfo formSendResult(TransactionMsgSendResult sendResult, Boolean status) {
        StorageInfo storageInfo = new StorageInfo();
        storageInfo.setCreateTime(new Date());
        storageInfo.setStatus(status);
        // ===msg
        SendResult result = sendResult.getSendResult();
        storageInfo.setMsgId(result.getMsgId());
        storageInfo.setOffsetMsgId(result.getOffsetMsgId());
        storageInfo.setOffsetMsgId(result.getOffsetMsgId());
        storageInfo.setQueueId(result.getMessageQueue().getQueueId());
        storageInfo.setTopic(result.getMessageQueue().getTopic());
        
        return storageInfo;
    }
}

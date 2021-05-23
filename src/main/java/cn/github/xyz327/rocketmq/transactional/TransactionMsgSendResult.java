package cn.github.xyz327.rocketmq.transactional;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.client.producer.SendResult;

/**
 * @author xizhou
 * @since 2019/3/11 9:05
 */
@Data
@AllArgsConstructor
public class TransactionMsgSendResult {
    private String producerGroup;
	private String topic;
    private String transactionId;
    private SendResult sendResult;
}

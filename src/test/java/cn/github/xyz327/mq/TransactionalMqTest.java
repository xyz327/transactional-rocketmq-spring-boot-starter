package cn.github.xyz327.mq;

import cn.github.xyz327.rocketmq.transactional.TransactionalRocketMqProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/**
 * @author <a href="mailto:xyz327@outlook.com">xizhou</a>
 * @since 2021/5/23 6:26 下午
 */
@SpringBootTest(classes = TransactionalMqTestServer.class)
@RunWith(SpringRunner.class)
public class TransactionalMqTest {
    @Autowired
    private TransactionalRocketMqProducer transactionalRocketMqProducer;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @Transactional
    public boolean transactionalMessage() throws Exception {
        // 做本地事务操作
        jdbcTemplate.queryForList("select 1 from dual");

        // 发送事务消息
        Message message = new Message();
        message.setTopic("trans_topic");
        message.setBody("事务消息".getBytes(StandardCharsets.UTF_8));
        transactionalRocketMqProducer.sendTransactionalMessage(message);
        return true;
    }
}

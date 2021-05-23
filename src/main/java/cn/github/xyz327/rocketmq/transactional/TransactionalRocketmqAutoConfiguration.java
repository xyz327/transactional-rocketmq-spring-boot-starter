package cn.github.xyz327.rocketmq.transactional;

import cn.github.xyz327.rocketmq.transactional.advice.TransactionalMqAdvice;
import cn.github.xyz327.rocketmq.transactional.storage.PlatformDatasourceTransactionalMqStatusStorage;
import cn.github.xyz327.rocketmq.transactional.storage.TransactionalMqStatusStorage;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * @author <a href="mailto:xyz327@outlook.com">xizhou</a>
 * @since 2021/5/21 9:33 下午
 */
@Configuration
public class TransactionalRocketmqAutoConfiguration {

    @Bean
    public TransactionalMqStatusStorage transactionalMqStatusStorage(DataSource dataSource, PlatformTransactionManager platformTransactionManager) {
        return new PlatformDatasourceTransactionalMqStatusStorage(dataSource, platformTransactionManager);
    }

    @Bean
    public DefaultTransactionCheckListener defaultTransactionCheckListener(){
        return new DefaultTransactionCheckListener();
    }

    @Bean
    public TransactionalRocketMqProducer transactionalRocketMqTemplate(@Qualifier("DefaultMQProducer") TransactionMQProducer defaultMQProducer) {
        return new TransactionalRocketMqProducer(defaultMQProducer);
    }

    @Bean
    public TransactionalMqListener transactionalMqListener() {
        return new TransactionalMqListener();
    }

    @Bean
    public TransactionalMqAdvice transactionalMqAdvice() {
        return new TransactionalMqAdvice();
    }
}

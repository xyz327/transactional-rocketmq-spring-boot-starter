package cn.github.xyz327.rocketmq.transactional.advice;

import cn.github.xyz327.rocketmq.transactional.TransactionalMqListener;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author <a href="mailto:xyz327@outlook.com">xizhou</a>
 * @since 2021/5/23 9:50 上午
 */
@Aspect
public class TransactionalMqAdvice {

    @Autowired
    private TransactionalMqListener transactionalMqListener;

    @Before(value = "@annotation(org.springframework.transaction.annotation.Transactional)")
    public void handleTransactional() {
        transactionalMqListener.handleTransactionStart();
    }


    @Before(value = "@execution(* org.springframework.transaction.support.TransactionTemplate.execute(org.springframework.transaction.support.TransactionCallback))")
    public void handleTransactionTemplate() {
        transactionalMqListener.handleTransactionStart();
    }
}

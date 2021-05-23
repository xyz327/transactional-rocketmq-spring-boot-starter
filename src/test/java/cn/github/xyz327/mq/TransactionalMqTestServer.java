package cn.github.xyz327.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author <a href="mailto:xyz327@outlook.com">xizhou</a>
 * @since 2021/5/23 6:26 下午
 */
@SpringBootApplication
public class TransactionalMqTestServer {
    public static void main(String[] args) {
        SpringApplication.run(TransactionalMqTestServer.class, args);
    }
}

# transactional-rocketmq-spring-boot-starter

> rocketmq 事务消息结合本地事务实现方便的发送事务  

+ 基于`rocketmq-spring-boot-starter`  
+ 依赖本地事务记录表来完成事务消息

## 增加依赖
```xml
 <dependency>
    <groupId>cn.github.xyz327</groupId>
    <artifactId>transactional-rocketmq-spring-boot-starter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
## 在业务数据库中增加事务记录表

> 执行 script/init.sql

## 使用

```java
public class TransactionalMqTest {
    @Autowired
    private TransactionalRocketMqProducer transactionalRocketMqProducer;
    @Autowired
    private JdbcTemplate jdbcTemplate;

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
```
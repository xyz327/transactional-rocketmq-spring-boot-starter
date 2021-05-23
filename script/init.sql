create table trans_mq_status
(
    id          int auto_increment,
    msg_id      varchar(64) not null comment '消息id',
    offset_msg_id varchar(64) not null  default '' comment '消息offsetid',
    queue_offset int default null comment '消息在队列的offset',
    queue_id int default null  comment '所属队列',
    topic varchar(128) not null  default '' comment '消息topic',
    local_id    varchar(64) not null default '' comment '本地事务id,预留',
    status      tinyint     not null default false comment '本地事务执行状态',
    create_time timestamp   not null default CURRENT_TIMESTAMP comment '创建时间',
    update_time timestamp   not null default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment '修改时间',
    primary key (id),
    unique key (msg_id)

) engine innodb comment '事务消息本地事务状态存储表'


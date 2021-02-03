## FlinkSQL debezium 进行 CDC 维表 ETL 到 elasticsearch

### Docker

```bash
docker-compose build
docker-compose up -d
```

可以访问 Flink Web UI (http://localhost:8081), 或者 Kibana (http://localhost:5601)

# 注册 debezium 的数据源

```bash
curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/mysql-source-ec
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-ec.json

curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/mysql-source-crm
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-crm.json

curl -i -X DELETE -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/mongodb-source-crawler

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-crawler-mongodb.json

curl -i -X GET -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors
```

## MySQL 数据源

### DDL

```sql
CREATE DATABASE ec;
CREATE TABLE orders(
  id VARCHAR(40) PRIMARY KEY,
  user_id VARCHAR(40),
  amount decimal,
  channel VARCHAR(256),
  status VARCHAR(40),
  ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  utime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
  id VARCHAR(40) PRIMARY KEY,
  order_id VARCHAR(40),
  product_id VARCHAR(40),
  quantity integer(12),
  price DECIMAL,
  amount DECIMAL,
  ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  utime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE products (
  id VARCHAR(40) PRIMARY KEY,
  name VARCHAR(256),
  price DECIMAL,
  ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  utime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE DATABASE crm;
CREATE TABLE users(
  id VARCHAR(40) PRIMARY KEY,
  name VARCHAR(256),
  age INT,
  ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  utime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### 写数据

```sql
INSERT INTO users(id, name, age) VALUES('1', 'andy', 25);
INSERT INTO users(id, name, age) VALUES('2', 'mary', 30);

INSERT INTO products(id, name, price) VALUES('1', 'product1', 40);
INSERT INTO orders VALUES('1', '1', 100);
INSERT INTO order_items(id, order_id, product_id, quantity, price, amount)
VALUES('1', '1', '1', 2, 50, 100);

```

### Kafka

查看数据有没有到达 kafka

```bash
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic shard1.ec.orders
```

### FLINK SQL

启动 Flink SQL 客户端:

```bash
docker-compose exec sql-client ./sql-client.sh
```

#### postgrel 支持 catalog, 注册一个 catalog, 这样可以直接通过 JDBC 来访问外部元数据.

```sql
CREATE CATALOG mysql_ec WITH (
    'type'='jdbc',
    'property-version'='1',
    'base-url'='jdbc:mysql://mysql:3306/',
    'default-database'='ec',
    'username'='root',
    'password'='debezium'
);

CREATE CATALOG mysql_crm WITH (
    'type'='jdbc',
    'property-version'='1',
    'base-url'='jdbc:mysql://mysql:3306/',
    'default-database'='crm',
    'username'='mysqluser',
    'password'='mysqlpw'
);
```

#### MySQL 构造 orders 流数据表

#### debezium cdc

```sql
CREATE TABLE orders (
  id STRING,
  user_id STRING,
  amount DECIMAL,
  ctime TIMESTAMP(0),
  utime TIMESTAMP(0),
  proc_time AS PROCTIME(),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'shard1.ec.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'cdc',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false',
  'debezium-json.ignore-parse-errors' = 'true',
  'debezium-json.timestamp-format.standard' = 'ISO-8601'
);
```

#### flink-cdc 表

```sql
CREATE TABLE orders (
  id STRING,
  user_id STRING,
  amount DECIMAL,
  ctime TIMESTAMP,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' ='mysql',
  'port' = '3306',
  'username' ='root',
  'password' ='debezium',
  'database-name' ='ec',
  'table-name' ='orders'
);
```

### MySQL 构造 users 流数据表

```sql
CREATE TABLE stream_users(
  id STRING,
  name STRING,
  age INT,
  ctime TIMESTAMP,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'shard1.crm.users',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'cdc',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false'
);
```

#### flink-cdc 表

```sql
CREATE TABLE users (
  id STRING,
  name STRING,
  age INT,
  ctime TIMESTAMP,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' ='mysql',
  'port' = '3306',
  'username' ='root',
  'password' ='debezium',
  'database-name' ='crm',
  'table-name' ='users'
);
```

#### Postgrel 构造 orders 流数据表

```sql
CREATE TABLE default_catalog.default_database.orders
WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.ec.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'cdc-group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset'
 )
LIKE `ec.orders` (
EXCLUDING OPTIONS);
```

#### MySQL 构造 users 维度数据表

```sql
CREATE TABLE dim_users(
  id STRING,
  name STRING,
  age INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' ='jdbc',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'url' ='jdbc:mysql://mysql:3306/crm?charset=utf8',
  'username' ='root',
  'password' ='debezium',
  'table-name' ='users',
  'lookup.cache.max-rows' = '3000',
  'lookup.cache.ttl' = '10s',
  'lookup.max-retries' = '3'
);
```

#### Postgrel 构造 users 维度数据表

```sql
CREATE TABLE mysql_crm.crm.users
LIKE `crm.users` (
INCLUDING OPTIONS);
```

#### Posgrel 使用 catalog

```sql
USE CATALOG default_catalog;
SELECT * FROM orders;
```

### 创建 elasticsearch 目标数据表

```sql
CREATE TABLE order_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  `order.amount` DECIMAL,
  `user.name` STRING,
  `user.age` int,
  ctime TIMESTAMP(0)
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'order_view'
);
```

#### 创建用户索引

```sql
CREATE TABLE user_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  name STRING,
  age INT,
  ctime TIMESTAMP
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'user_view'
);

CREATE TABLE user_order_stats_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  `order.amount.day` DECIMAL,
  `order.count.day` BIGINT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'user_view'
);
```

#### 提交一个 users 持续查询到 Flink 集群

```sql
INSERT INTO user_view SELECT id, name, age, ctime FROM users
```

提交一个 orders 持续查询到 Flink 集群:

```sql
INSERT INTO order_view
SELECT orders.id id,
       orders.amount `order.amount`,
       users.name `user.name`,
       users.age `user.age`,
       orders.ctime ctime
FROM orders
JOIN users
ON orders.user_id = users.id;
```

#### 加入统计

```sql
INSERT INTO user_order_stats_view SELECT
  user_id id,
  SUM(amount) `order.amount.day`,
  COUNT(*) `order.count.day`
FROM orders
GROUP BY user_id, date_format(ctime, '%Y%m%d');
```

### 在 kibana 中查看

http://localhost:5601

### 总结

- flink-sql 的 ddl 语句不会触发 flink-job , 同时创建的表、视图仅在会话级别有效。
- 对于连接表的 insert、select 等操作，则会触发相应的流 job， 并自动提交到 flink 集群，无限地运行下去，直到主动取消或者 job 报错。
- flink-sql 客户端关闭后，对于已经提交到 flink 集群的 job 不会有任何影响。
- flnik 本身不存储业务数据，只作为流批一体的引擎存在，所以主要的用法为读取外部系统的数据，处理后，再写到外部系统。
- flink 本身的元数据，包括表、函数等，默认情况下只是存放在内存里面，所以仅会话级别有效。可以存储到 Hive Metastore 中。
- 使用 Flink CDC 读取全量数据，全量数据同步完成后，Flink CDC 会无缝切换至 MySQL 的 binlog 位点继续消费增量的变更数据，且保证不会多消费一条也不会少消费一条

### 参考

- https://github.com/morsapaes/flink-sql-CDC#change-data-capture-with-flink-sql-and-debezium

- [MySQL 的 CDC 源表](https://help.aliyun.com/document_detail/184874.html)

- [Flink SQL CDC 上线！我们总结了 13 条生产实践经验](https://www.jianshu.com/p/d6ac601438a5)

- [flink-cdc-connectors](https://github.com/ververica/flink-cdc-connectors)

- [flink sql 实战案例之商品销量实时统计](https://blog.csdn.net/songjifei/article/details/105270666)

-[电商例子](https://github.com/zhp8341/flink-streaming-platform-web/blob/87d9e6aa1ff498ea31ebf74ce4f757ade791c4d1/docs/sql_demo/demo_6.md)

- [flink debezium 格式](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/connectors/formats/debezium.html)

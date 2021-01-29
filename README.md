## FlinkSQL debezium 进行 CDC 维表ETL 到 elasticsearch

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
```

## MySQL数据源
### DDL
```sql
CREATE DATABASE ec;
CREATE TABLE orders(id VARCHAR(40) primary key, user_id VARCHAR(40), amount decimal);

CREATE DATABASE crm;
CREATE TABLE users(id VARCHAR(40) primary key, name VARCHAR(256), age INT);
```

### 写数据
```sql
INSERT INTO users(id, name, age) VALUES('1', 'andy', 25);
INSERT INTO users(id, name, age) VALUES('2', 'mary', 30);

INSERT INTO orders(id, user_id, amount) VALUES('1', '1', 100);
INSERT INTO orders(id, user_id, amount) VALUES('2', '1', 200);
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
#### postgrel 支持 catalog, 注册一个 catalog, 这样可以直接通过JDBC来访问外部元数据.
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

#### MySQL构造 orders 流数据表
```sql
CREATE TABLE orders (
  id STRING,
  user_id STRING,
  amount double
) WITH (
  'connector' = 'kafka',
  'topic' = 'shard1.ec.orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'cdc',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'true'
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
CREATE TABLE users(
  id STRING,
  name STRING,
  age int,
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
  `order.amount` DOUBLE,
  `user.name` STRING,
  `user.age` int
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'order_view'
);
```

提交一个持续查询到 Flink 集群:
```sql
INSERT INTO order_view
SELECT orders.id id,
       orders.amount `order.amount`,
       users.name `user.name`,
       users.age `user.age`
FROM orders
JOIN users
ON orders.user_id = users.id;
```

### 在 kibana 中查看
http://localhost:5601

### 参考
https://github.com/morsapaes/flink-sql-CDC#change-data-capture-with-flink-sql-and-debezium
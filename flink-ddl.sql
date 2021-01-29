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
INSERT INTO order_view
SELECT orders.id id,
       orders.amount `order.amount`,
       dim_users.name `user.name`,
       dim_users.age `user.age`,
       orders.ctime ctime
FROM orders
JOIN dim_users
ON orders.user_id = dim_users.id;


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

INSERT INTO user_view SELECT id, name, age, ctime FROM users;


CREATE TABLE user_order_stats_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  `order.amount.day` DECIMAL,
  `order.count.day` BIGINT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'user_view'
);

INSERT INTO user_order_stats_view SELECT
  user_id id,
  SUM(amount) `order.amount.day`,
  COUNT(*) `order.count.day`
FROM orders
GROUP BY user_id, date_format(ctime, 'yy-mm-dd');
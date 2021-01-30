CREATE TABLE orders (
  id STRING,
  user_id STRING,
  amount DECIMAL,
  status STRING,
  ctime TIMESTAMP,
  utime TIMESTAMP,
  PRIMARY KEY (id) NOT ENFORCED,
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

CREATE TABLE order_items (
  id STRING,
  order_id STRING,
  product_id STRING,
  quantity BIGINT,
  price DECIMAL,
  amount DECIMAL,
  ctime TIMESTAMP,
  utime TIMESTAMP,
  PRIMARY KEY (id) NOT ENFORCED,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' ='mysql',
  'port' = '3306',
  'username' ='root',
  'password' ='debezium',
  'database-name' ='ec',
  'table-name' ='order_items'
);

CREATE TABLE products (
  id STRING,
  name STRING,
  price DECIMAL,
  ctime TIMESTAMP,
  utime TIMESTAMP,
  PRIMARY KEY (id) NOT ENFORCED,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' ='mysql',
  'port' = '3306',
  'username' ='root',
  'password' ='debezium',
  'database-name' ='ec',
  'table-name' ='products'
);


CREATE TABLE users (
  id STRING,
  name STRING,
  age INT,
  ctime TIMESTAMP,
  utime TIMESTAMP,
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

--- 这个暂时可以不用
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
  `order.status` STRING,
  `user.name` STRING,
  `user.age` INT,
  ctime TIMESTAMP,
  utime TIMESTAMP
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'order_view'
);

CREATE TABLE user_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  name STRING,
  age INT,
  ctime TIMESTAMP,
  utime TIMESTAMP
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'user_view'
);

CREATE TABLE product_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  name STRING,
  price DECIMAL,
  ctime TIMESTAMP,
  utime TIMESTAMP
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'product_view'
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

CREATE TABLE order_stats_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  amount DECIMAL,
  cnt BIGINT
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'order_stats_view'
);


INSERT INTO order_view
SELECT orders.id id,
       orders.amount `order.amount`,
       orders.status `order.status`,
       users.name `user.name`,
       users.age `user.age`,
       orders.ctime ctime,
       orders.utime utime
FROM orders
JOIN users
ON orders.user_id = users.id;

INSERT INTO user_view SELECT id, name, age, ctime, utime FROM users;

INSERT INTO product_view SELECT id, name, price, ctime, utime FROM products;

--- 用户的订单统计
INSERT INTO user_order_stats_view SELECT
  user_id id,
  SUM(amount) `order.amount.day`,
  sum(cnt) `order.count.day`
FROM (
  SELECT 
    user_id,
    date_format(ctime, 'yyyy-MM-dd') cday,
    SUM(amount) amount,
    count(*) cnt
  FROM orders
  WHERE orders.status <> 'closed'
  GROUP BY user_id, mod(hash_code(FLOOR(RAND(1)*1000)), 256), date_format(ctime, 'yyyy-MM-dd')
)
GROUP BY cday, user_id;

--- 每日订单 金额，数量 统计, 要加入撤回功能，因为有订单可能取消
INSERT INTO order_stats_view SELECT
  cday id,
  SUM(amount) amount,
  sum(cnt) cnt
FROM (
  SELECT 
    date_format(ctime, 'yyyy-MM-dd') cday,
    SUM(amount) amount,
    count(*) cnt
  FROM orders
  WHERE orders.status <> 'closed'
  GROUP BY mod(hash_code(FLOOR(RAND(1)*1000)), 256), date_format(ctime, 'yyyy-MM-dd')
)
GROUP BY cday;




--- 产品累积销售情况
CREATE TABLE product_stats_view (
  id STRING PRIMARY KEY NOT ENFORCED,
  quantity BIGINT,
  amount DECIMAL
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'product_view'
);

INSERT INTO product_stats_view SELECT
  product_id id,
  SUM(quantity1) quantity,
  SUM(amount1) amount
FROM (
  SELECT 
    order_items.product_id product_id,
    count(*) quantity1,
    SUM(order_items.amount) amount1
  FROM order_items
  JOIN orders
  ON
    order_items.order_id = orders.id
  WHERE orders.status <> 'closed'
  GROUP BY order_items.product_id, mod(hash_code(FLOOR(RAND(1)*1000)), 256)
)
GROUP BY product_id;
# 批处理数据管道练习

## 目标

1. 理解传统数据仓库中的数据转换的基本范式
2. 了解使用 Airflow 编排数据管道
3. 理解事实和维度
4. 理解时间维度
5. 理解缓慢变化维
6. 熟悉使用 SQL 进行数据转换
7. 了解在PostgreSQL中实现缓慢变化维的方式
8. 了解使用BI工具进行数据可视化

## 问题描述

给定两个数据集：“商品主数据”和“订单事件数据”，计算相应指标。数据示例见`sample-data/`目录。 

**商品主数据**

商品主数据（products）是每个源系统里的商品全量数据，每日更新，定义如下

|字段|类型|说明|
|---|---|---|
|id|字符串|商品ID，在每个源系统中是唯一的|
|title|字符串|商品标题|
|category|字符串|商品类别，可能会会变化|
|price|指导价格|可能会变化|

**订单事件数据**（增量数据，每日更新）

订单事件数据（order_events）包含了前一天内属性发生变化的订单数据。

|字段|类型|说明|
|---|---|---|
|id|字符串|订单ID|
|productId|字符串|商品ID，对应商品主数据里的ID。每个订单只包含同一个商品ID|
|amount|数字|商品数量|
|status|字符串|当前的当前状态。目前包括created，completed，deleted 状态，但以后还可能有其他状态|
|timestamp|时间戳|事件发生的时间|

注意，id 可能有重复，但 id 和 timestamp组合起来是唯一的。

计算如下指标：

1. 当前已创建未完成的订单数
2. 最近2年内创建的订单数，按每个季度
3. 最近2年内创建的订单数，按每个季度每个产品类别查看
4. 计算本月之前创建，但还未完成的订单数（留存订单）
5. 计算2年内每个月当月创建，但没有完成的订单数，按月展示

并通过BI工具创建对应的图表。

## 第二部分问题描述

新增商品库存数据源，包含了每日商品库存的快照（只包含库存有变化的商品）。

|字段|类型|说明|
|---|---|---|
|productId|字符串|商品ID，对应商品主数据里的ID|
|amount|数字|库存数量|
|date|日期|快照的日期|

计算如下指标：
1. 最近1个月内，每一类商品在每天的库存数量
2. 最近2年内，每一类商品在每月最后一天的的库存数量

## 准备工作

安装如下工具

1. docker 和 docker-compose
2. 编辑器如 Visual Studio Code
3. PostgreSQL GUI 工具，比如搜索VS Code的相关扩展

熟悉以下编程知识

1. Python
2. SQL

## 开始练习

运行相关服务

```
docker-compose up
```

编写Airflow所需的DAG，放在 `dags/` 目录中，将数据放在 `data/` 目录中（示例数据见 `/sample-data` ）。最后可以在 Metabase 中构建图表。

## 快速链接

* Airflow http://localhost:8080
* Metabase http://localhost:8082

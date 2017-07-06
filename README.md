# kafka stream学习
每5秒输出过去1小时18岁到35岁用户所购买的商品中，每种品类销售额排名前十的订单汇总信息：
使用数据内的时间(Event Time)作为timestamp
每5秒输出一次
每次计算到输出为止过去1小时的数据
支持订单详情和用户详情的更新和增加
输出字段包含时间窗口（起始时间，结束时间），品类（category），商品名（item_name），销量（quantity），单价（price），总销售额，该商品在该品类内的销售额排名
订单Topic作为KStream，用户和商品Topic作为KTable

创建topic：
src/main/resources/topic.txt
输出结果：
result.txt

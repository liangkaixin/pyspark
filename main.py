"""
需求：Top10热门品类

需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量（次数）来统计热门品类。
鞋			点击数 下单数  支付数
衣服		点击数 下单数  支付数
电脑		点击数 下单数  支付数
例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
为了更好的泛用性，当前案例按照点击次数进行排序，如果点击相同，按照下单数，如果下单还是相同，按照支付数。
"""
# city_id = split('_')[-1]
# 支付的产品ids = split('_')[-2]
# 支付品类ids = split('_')[-3]
# 下单的产品ids = split('_')[-4]
# 下单品类ids = split('_')[-5]
# 点击的产品id = split('_')[-6]
# 点击的品类ids = split('_')[-7]
# 搜索关键字 = split('_')[-8]
from pyspark import SparkConf, SparkContext

# 定义数据类
class UserVisitAction:
    def __init__(self, *args):
        self.date = args[0]
        self.user_id = args[1]
        self.session_id = args[2]
        self.page_id = args[3]
        self.action_time = args[4]
        self.search_keyword = args[5]
        self.click_category_id = args[6]
        self.click_product_id = args[7]
        self.order_category_ids = args[8]
        self.order_product_ids = args[9]
        self.pay_category_ids = args[10]
        self.pay_product_ids = args[11]
        self.city_id = args[12]

class CategoryCountInfo:
    def __init__(self, category_id, click_count=0, order_count=0, pay_count=0):
        self.categoryId = category_id
        self.clickCount = click_count
        self.orderCount = order_count
        self.payCount = pay_count

    def __lt__(self, other):
        # 实现比较逻辑，用于排序
        if self.clickCount != other.clickCount:
            return self.clickCount < other.clickCount
        elif self.orderCount != other.orderCount:
            return self.orderCount < other.orderCount
        else:
            return self.payCount < other.payCount

    def __str__(self):
        return f"CategoryCountInfo(categoryId={self.categoryId}, clickCount={self.clickCount}, orderCount={self.orderCount}, payCount={self.payCount})"

if __name__ == "__main__":
    # 1. 创建Spark配置和上下文
    conf = SparkConf() \
        .setMaster("local[*]") \
        .setAppName("sparkCore") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sc = SparkContext(conf=conf)

    # 2. 读取数据
    line_rdd = sc.textFile("user_visit_action.txt")

    # 3. 转换为UserVisitAction对象
    action_rdd = line_rdd.map(lambda line: UserVisitAction(*line.split("_")))

    # 4. 扁平化处理，生成CategoryCountInfo
    def flat_map_func(user_visit_action):
        result = []
        if user_visit_action.click_category_id != "-1":
            # 点击数据
            result.append(CategoryCountInfo(user_visit_action.click_category_id, 1, 0, 0))
        elif user_visit_action.order_category_ids != "null":
            # 订单数据
            for order in user_visit_action.order_category_ids.split(","):
                result.append(CategoryCountInfo(order, 0, 1, 0))
        elif user_visit_action.pay_category_ids != "null":
            # 支付数据
            for pay in user_visit_action.pay_category_ids.split(","):
                result.append(CategoryCountInfo(pay, 0, 0, 1))
        return result

    category_count_rdd = action_rdd.flatMap(flat_map_func)
    # 5. 按categoryId聚合统计
    def map_to_pair_func(category_count_info):
        return category_count_info.categoryId, category_count_info

    def reduce_func(v1, v2):
        v1.clickCount += v2.clickCount
        v1.orderCount += v2.orderCount
        v1.payCount += v2.payCount
        return v1

    count_info_rdd = category_count_rdd \
        .map(map_to_pair_func) \
        .reduceByKey(reduce_func) \
        .map(lambda x: x[1])

    # 6. 排序并获取结果
    sorted_result = count_info_rdd.sortBy(lambda x: x, ascending=False, numPartitions=2)

    # 7. 输出结果
    for item in sorted_result.collect():
        print(item)

    # 8. 关闭SparkContext
    sc.stop()


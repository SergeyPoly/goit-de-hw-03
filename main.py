from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round, lit

# Створюємо сесію Spark
spark = SparkSession.builder.appName("HW3_Sandbox").getOrCreate()

# Завантажуємо датасети
products_df = spark.read.csv("CSV_files/products.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("CSV_files/purchases.csv", header=True, inferSchema=True)
users_df = spark.read.csv("CSV_files/users.csv", header=True, inferSchema=True)

# Видаляємо строки з пропущеними значеннями
products_df = products_df.dropna()
purchases_df = purchases_df.dropna()
users_df = users_df.dropna()

# Об'єднуємо таблиці покупок та товарів і додаємо колонку із загальною вартістю покупки (кількість Х вартість одиниці товару)
purchases_products_df = purchases_df.join(
    products_df, on="product_id", how="inner"
).withColumn("total_cost", col("quantity") * col("price"))

# Вираховуємо суму покупок за категоріями
category_sales_df = purchases_products_df.groupBy("category").agg(
    round(sum("total_cost"), 2).alias("total_sales")
)

# результат рішення для п.3
category_sales_df.show()

# Фільтруємо користувачів 18–25 років
young_users_df = users_df.filter((col("age") >= 18) & (col("age") <= 25))

# Об'єднуємо таблиці покупок та відфльтрованих користувачів
young_purchases_df = purchases_products_df.join(
    young_users_df, on="user_id", how="inner"
)

# Вираховуємо суму покупок за категоріями
young_category_sales_df = young_purchases_df.groupBy("category").agg(
    round(sum("total_cost"), 2).alias("total_sales_18_25")
)

# результат рішення для п.4
young_category_sales_df.show()

# Вираховуємо загальну суму покупок вікової группи 18–25
total_sales_18_25 = young_category_sales_df.agg(sum("total_sales_18_25")).first()[0]

# Додаємо стовбець з часткою покупок за категоріями
young_category_sales_df = young_category_sales_df.withColumn(
    "percentage", round((col("total_sales_18_25") / lit(total_sales_18_25)) * 100, 2)
)

# результат рішення для п.5
young_category_sales_df.show()

# Відсортовуємо за зменшенням відсотка та обираємо топ-3
top_3_categories_df = young_category_sales_df.orderBy(col("percentage").desc()).limit(3)

# результат рішення для п.6
top_3_categories_df.show()

# Закриваємо сесію Spark
spark.stop()

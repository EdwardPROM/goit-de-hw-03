# # Покрокова інструкція виконання

# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder.appName("HW_3").getOrCreate()

# Завантажуємо датасет
users_df = spark.read.csv('HW/HW_3/users.csv', header=True)
purchases_df = spark.read.csv('HW/HW_3/purchases.csv', header=True)
products_df = spark.read.csv('HW/HW_3/products.csv', header=True)

# # # Виводимо перші 5 записів
users_df.show(5, truncate=False)
purchases_df.show(5, truncate=False)
products_df.show(5, truncate=False)

# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# # Виведення очищених даних для перевірки
print("Users:")
users_df.show()

print("Purchases:")
purchases_df.show()

print("Products:")
products_df.show()

# 3. Визначте загальну суму покупок за кожною категорією продуктів.
from pyspark.sql.functions import col, round, sum as _sum

# Об'єднуємо purchases_df і products_df
merged_df = purchases_df.join(products_df, on="product_id", how="inner")

# Обчислюємо загальну суму покупок для кожної категорії
category_purchases = merged_df.withColumn("total_purchase", col("quantity") * col("price")) \
                              .groupBy("category") \
                              .agg(_sum("total_purchase").alias("total_purchase_sum"))

# Сортуємо за спаданням загальної суми
category_purchases = category_purchases.withColumn("total_purchase_sum", round(col("total_purchase_sum"), 2))

# Виводимо результати
category_purchases.show()

# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.

# Об'єднуємо всі три таблиці
all_df = users_df.join(purchases_df, on="user_id", how="inner") \
                    .join(products_df, on="product_id", how="inner")

# Фільтруємо користувачів віком від 18 до 25 років
filtered_df = all_df.filter((col("age") >= 18) & (col("age") <= 25))

# Обчислюємо суму покупок для кожної категорії
category_purchases_18_25 = filtered_df.withColumn("total_purchase", col("quantity") * col("price")) \
                                      .groupBy("category") \
                                      .agg(round(_sum("total_purchase"), 2).alias("total_purchase_sum"))

# Сортуємо результати за спаданням
category_purchases_18_25 = category_purchases_18_25.orderBy(col("total_purchase_sum").desc())

# Виводимо результати
category_purchases_18_25.show()


# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.

# Обчислюємо загальну суму покупок у віковій групі 18-25
total_sum = category_purchases_18_25.agg(_sum("total_purchase_sum").alias("total_sum")).collect()[0]["total_sum"]

# Обчислюємо частку покупок за кожною категорією
category_purchases_18_25 = category_purchases_18_25.withColumn(
    "purchase_share",
    round((col("total_purchase_sum") / total_sum) * 100, 2)  # У відсотках
).orderBy(col("purchase_share").desc())

# Виводимо результати
category_purchases_18_25.show(truncate=False)


# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.

# Вибираємо 3 категорії з найвищим відсотком витрат
top_3_categories = category_purchases_18_25.limit(3)

# Виводимо результат
top_3_categories.show(truncate=False)

# Закриваємо сесію Spark
spark.stop()
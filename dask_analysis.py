import dask.dataframe as dd

# Завантажуємо CSV файли
users = dd.read_csv('users.csv', assume_missing=True)
purchases = dd.read_csv('purchases.csv', assume_missing=True)
products = dd.read_csv('products.csv', assume_missing=True)

# Фільтруємо користувачів вікової категорії 18-25
filtered_users = users[(users['age'] >= 18) & (users['age'] <= 25)]

# Об'єднуємо таблиці
merged_data = purchases.merge(filtered_users, on='user_id').merge(products, on='product_id')

# Обчислюємо загальну суму покупок за кожною категорією для вікової групи 18-25
total_by_category_age_18_25 = (merged_data.assign(total_price=merged_data['quantity'] * merged_data['price'])
                               .groupby('category')['total_price']
                               .sum())

# Обчислюємо сумарні витрати для вікової групи 18-25
total_spending_age_18_25 = total_by_category_age_18_25.sum()

# Визначаємо частку покупок за кожною категорією та округлюємо до двох знаків після коми
category_share = ((total_by_category_age_18_25 / total_spending_age_18_25) * 100).round(2)

# Вибираємо 3 категорії з найвищими відсотками витрат
top_3_categories = category_share.nlargest(3)

# Виводимо результат
print("Топ 3 категорії з найвищим відсотком витрат для вікової групи 18-25 років:\n")
print(top_3_categories.compute())
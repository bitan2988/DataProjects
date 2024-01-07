import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


customers_file = "../data/olist_customers_dataset.csv"
geolocation_file = "../data/olist_geolocation_dataset.csv"
order_items_file = "../data/olist_order_items_dataset.csv"
payments_file = "../data/olist_order_payments_dataset.csv"
reviews_file = "../data/olist_order_reviews_dataset.csv"
orders_file = "../data/olist_orders_dataset.csv"
products_file = "../data/olist_products_dataset.csv"
sellers_file = "../data/olist_sellers_dataset.csv"
prod_cat_name_translation_file = "../data/product_category_name_translation.csv"

customer = pd.read_csv(customers_file)
order_items = pd.read_csv(order_items_file)
payemnts = pd.read_csv(payments_file)
order = pd.read_csv(orders_file)
products = pd.read_csv(products_file)


'''
    Churn:
        1 if the customer has made a purchase before 3 months and then made a second purhcase in last 3 months
'''
order['order_purchase_timestamp'] = pd.to_datetime(order['order_purchase_timestamp'])

order = pd.merge(order, customer, on="customer_id", how="left")

# Timestamp of first purchase
first_purchase = order[['customer_unique_id', 'order_purchase_timestamp']].groupby(['customer_unique_id']).agg({'order_purchase_timestamp': 'min'}).reset_index()
first_purchase.columns = ['customer_unique_id', 'first_purchase_timestamp']

order = pd.merge(order, first_purchase, on="customer_unique_id", how="left")

three_months_ago = np.max(order['order_purchase_timestamp']) - timedelta(days=3*30)

order = order[order['first_purchase_timestamp'] < three_months_ago]

filtered_df = order[(order['order_purchase_timestamp'] >= three_months_ago) & (order['first_purchase_timestamp'] < three_months_ago)]
purchased_90_days = list(set(filtered_df['customer_unique_id'].to_list()))

order["churn"] = order.apply(lambda x: 1 if x["customer_unique_id"] in purchased_90_days else 0, axis=1)


# Filtering only the data for last 3 months
order = order[order['order_purchase_timestamp'] >= three_months_ago]

products = products[['product_id']][:1000]

order = pd.merge(order, order_items, on="order_id", how="left")

order_quantity = order.groupby(['order_id', 'product_id']).size().reset_index()
order_quantity.columns = ['order_id', 'product_id', 'quantity_purchased']

order = pd.merge(order, order_quantity, on=["order_id",  'product_id'], how="left")

distinct_product_purchased = list(set(order['product_id'].to_list()))

all_products = products['product_id'].to_list()

for prd in distinct_product_purchased:
    if prd not in all_products:
        all_products.append(prd)
        
products_final = pd.DataFrame(data=all_products, columns=['product_id_initial'])

# Cartesian-join
order['cross_join_key'] = 1
products_final['cross_join_key'] = 1

order = pd.merge(order, products_final, on="cross_join_key")



order["bp"] = order.apply(lambda x: 1 if (str(x["product_id"])==str(x["product_id_initial"]) and x["churn"]==1) else 0, axis=1)
order["quantity_purchased"] = order.apply(lambda x: 0 if x["bp"]==0 else x["quantity_purchased"], axis=1)

order = pd.merge(order, payemnts, on="order_id", how="left")

order = order[['order_id', 'customer_unique_id', 'order_purchase_timestamp', 'first_purchase_timestamp', 'churn', 'product_id_initial', 'price', 'freight_value', 'payment_value', 'bp', 'quantity_purchased']]

order["payment_value"] = order.apply(lambda x: 0 if x["bp"]==0 else x["payment_value"], axis=1)
order["price"] = order.apply(lambda x: 0 if x["bp"]==0 else x["price"], axis=1)
order["freight_value"] = order.apply(lambda x: 0 if x["bp"]==0 else x["freight_value"], axis=1)

order.to_csv('churn_dataset.csv')


bp_df = order[['customer_unique_id', 'product_id_initial', 'price', 'freight_value', 'quantity_purchased', 'payment_value', 'bp']]

bp_df = bp_df.groupby(['customer_unique_id', 'product_id_initial']).agg({
    'price': 'mean',
    'freight_value': 'mean',
    'quantity_purchased': 'sum',
    'payment_value': 'sum',
    'bp': 'max'
}).reset_index()

bp_df.to_csv('customerXproduct_level_bp_dataset.csv')


churn_df = order[['customer_unique_id', 'churn']]

churn_df = churn_df.groupby(['customer_unique_id']).agg({
    'churn': 'max'
}).reset_index()

churn_df.to_csv('customer_level_churn_dataset.csv')
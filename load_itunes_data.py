import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)
register_adapter(np.bool_, AsIs)


def upsert_apps(cur, df):
    apps_df = df[['App Apple ID', 'App Name']].drop_duplicates()
    apps_df.columns = ['app_apple_id', 'app_name']
    apps_records = list(apps_df.to_records(index=False))

    query = '''
    INSERT INTO apps (app_apple_id, app_name)
    VALUES %s
    ON CONFLICT (app_apple_id) DO UPDATE
    SET app_name = EXCLUDED.app_name
    WHERE apps.app_name IS DISTINCT FROM EXCLUDED.app_name;
    '''

    extras.execute_values(cur, query, apps_records)

def upsert_subscriptions(cur, df):
    subscriptions_df = df[['Subscription Apple ID', 'Subscription Group ID', 'Subscription Name', 'Subscription Duration']].drop_duplicates()
    subscriptions_df.columns = ['subscription_apple_id', 'subscription_group_id', 'subscription_name', 'subscription_duration']
    subscriptions_records = list(subscriptions_df.to_records(index=False))

    query = '''
    INSERT INTO subscriptions (subscription_apple_id, subscription_group_id, subscription_name, subscription_duration)
    VALUES %s
    ON CONFLICT (subscription_apple_id) DO UPDATE
    SET subscription_group_id = EXCLUDED.subscription_group_id,
        subscription_name = EXCLUDED.subscription_name,
        subscription_duration = EXCLUDED.subscription_duration
    WHERE subscriptions.subscription_group_id IS DISTINCT FROM EXCLUDED.subscription_group_id OR
          subscriptions.subscription_name IS DISTINCT FROM EXCLUDED.subscription_name OR
          subscriptions.subscription_duration IS DISTINCT FROM EXCLUDED.subscription_duration;
    '''

    extras.execute_values(cur, query, subscriptions_records)

def upsert_subscribers(cur, df):
    subscribers_df = df[['Subscriber ID', 'Subscriber ID Reset']].drop_duplicates()
    subscribers_df['Subscriber ID Reset'] = subscribers_df['Subscriber ID Reset'] == 'Yes'
    subscribers_df.columns = ['subscriber_apple_id', 'subscriber_id_reset']
    subscribers_records = list(subscribers_df.to_records(index=False))

    query = '''
    INSERT INTO subscribers (subscriber_apple_id, subscriber_id_reset)
    VALUES %s
    ON CONFLICT (subscriber_apple_id) DO UPDATE
    SET subscriber_id_reset = EXCLUDED.subscriber_id_reset
    WHERE subscribers.subscriber_id_reset IS DISTINCT FROM EXCLUDED.subscriber_id_reset;
    '''

    extras.execute_values(cur, query, subscribers_records)

def insert_orders(cur, df):
    df['Is Trial'] = df['Introductory Price Type'] == 'Free Trial'

    orders_df = df[(df['Refund'].isna()) | (df['Refund'] != 'Yes')][['Event Date', 
                                                                     'Subscriber ID', 
                                                                     'App Apple ID', 
                                                                     'Subscription Apple ID', 
                                                                     'Is Trial', 
                                                                     'Introductory Price Duration',
                                                                     'Customer Price',
                                                                     'Customer Currency',
                                                                     'Developer Proceeds',
                                                                     'Proceeds Currency',
                                                                     'Country',
                                                                     'Device',
                                                                     'Marketing Opt-In Duration',
                                                                     'Preserved Pricing',
                                                                     'Proceeds Reason',
                                                                     'Client']]
    
    orders_df.columns = ['order_date', 
                         'subscriber_id', 
                         'app_id', 
                         'subscription_id', 
                         'is_trial', 
                         'trial_duration', 
                         'customer_price', 
                         'customer_currency', 
                         'developer_proceeds', 
                         'proceeds_currency', 
                         'country',
                         'device',
                         'marketing_opt_in_duration',
                         'preserved_pricing', 
                         'proceeds_reason', 
                         'client']
    
    orders_records = list(orders_df.to_records(index=False))
    
    query = '''
    INSERT INTO orders (order_date, subscriber_id, app_id, subscription_id, is_trial, trial_duration, customer_price, customer_currency, developer_proceeds, proceeds_currency, country, device, marketing_opt_in_duration, preserved_pricing, proceeds_reason, client)
    VALUES %s;
    '''

    extras.execute_values(cur, query, orders_records)

def insert_refunds(cur, df):
    refunds_df = df[df['Refund'] == 'Yes'][['Event Date', 
                                            'Subscriber ID', 
                                            'App Apple ID', 
                                            'Subscription Apple ID',
                                            'Customer Price',
                                            'Customer Currency',
                                            'Developer Proceeds',
                                            'Proceeds Currency',
                                            'Purchase Date',
                                            'Country',
                                            'Device',
                                            'Marketing Opt-In Duration',
                                            'Preserved Pricing',
                                            'Proceeds Reason',
                                            'Client']]
    
    refunds_df.columns = ['refund_date', 
                          'subscriber_id',
                          'app_id',
                          'subscription_id',
                          'customer_price',
                          'customer_currency',
                          'developer_proceeds',
                          'proceeds_currency',
                          'original_purchase_date',
                          'country',
                          'device',
                          'marketing_opt_in_duration',
                          'preserved_pricing',
                          'proceeds_reason',
                          'client']
    
    refunds_records = list(refunds_df.to_records(index=False))

    query = '''
    INSERT INTO refunds (refund_date, subscriber_id, app_id, subscription_id, customer_price, customer_currency, developer_proceeds, proceeds_currency, original_purchase_date, country, device, marketing_opt_in_duration, preserved_pricing, proceeds_reason, client)
    VALUES %s;
    '''

    extras.execute_values(cur, query, refunds_records)

DB_NAME = 'DB name'
DB_USER = 'Username'
DB_PASSWORD = 'Password'
DB_HOST = 'localhost'
DB_PORT = '5432'

file_names = ['20190201.txt', '20190202.txt', '20190203.txt', '20190204.txt', '20190205.txt', '20190206.txt', '20190207.txt', '20190208.txt', '20190209.txt', '20190210.txt']

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
conn.autocommit = False

for file_name in file_names:
    df = pd.read_csv(file_name, sep='\t')
    df = df.replace(['', ' ', 'NaN'], np.nan)
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    with conn.cursor() as cur:
        upsert_apps(cur, df)
        upsert_subscriptions(cur, df)
        upsert_subscribers(cur, df)
        insert_orders(cur, df)
        insert_refunds(cur, df)

    conn.commit()

    print(f'{file_name} was pushed successfully.')

conn.close()

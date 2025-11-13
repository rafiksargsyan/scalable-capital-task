import re
import boto3
from botocore.exceptions import WaiterError, ClientError
import csv
from io import StringIO
from python_dynamodb_lock.python_dynamodb_lock import *
import os
import datetime

def lambda_handler(event, context):
    dynamodb_resource = boto3.resource('dynamodb')
    lock_client = DynamoDBLockClient(dynamodb_resource, os.environ['LOCK_TABLE'])
    lock = lock_client.acquire_lock('lock_key', retry_timeout=datetime.timedelta(seconds=1))

    bucket, key = get_s3_object(event)
    date = get_date(key)
    accounts_file_name = f"accounts_{date}.csv"
    clients_file_name = f"clients_{date}.csv"
    portfolios_file_name = f"portfolios_{date}.csv"
    transactions_file_name = f"transactions_{date}.csv"
    wait_for_s3_object(bucket, accounts_file_name)
    wait_for_s3_object(bucket, clients_file_name)
    wait_for_s3_object(bucket, portfolios_file_name)
    wait_for_s3_object(bucket, transactions_file_name)
    accounts = read_csv_from_s3_stream(bucket, accounts_file_name)
    clients = read_csv_from_s3_stream(bucket, clients_file_name)
    portfolios = read_csv_from_s3_stream(bucket, portfolios_file_name)
    transactions = read_csv_from_s3_stream(bucket, transactions_file_name)

    account_aggr = {}

    for transaction in transactions:
        account_number = transaction['account_number']
        if account_number not in account_aggr:
            account_aggr[account_number] = {}
        account_aggr[account_number]['number_of_transactions'] =(
            account_aggr[account_number].get('number_of_transactions', 0) + 1)
        if transaction['keyword'] == 'DEPOSIT':
            account_aggr[account_number]['sum_of_deposits'] =(
                    account_aggr[account_number].get('sum_of_deposits', 0) + float(transaction['amount']))

    for account in accounts:
        if account['account_number'] not in account_aggr:
            account_aggr[account['account_number']] = {}
        account_aggr[account['account_number']]['cash_balance'] = float(account['cash_balance'])
        account_aggr[account['account_number']]['taxes_paid'] = float(account['taxes_paid'])

    client_aggr = {}

    for portfolio in portfolios:
        client_reference = portfolio['client_reference']
        account_number = portfolio['account_number']
        taxes_paid = account_aggr[account_number]['taxes_paid']

        if client_reference not in client_aggr:
            client_aggr[client_reference] = {}
        client_aggr[client_reference]['taxes_paid'] = client_aggr[client_reference].get('taxes_paid', 0) + taxes_paid

    client_messages = []
    for client in clients:
        client_reference = client['client_reference']
        tax_free_allowance = client['tax_free_allowance']
        client_msg = {
            "type": "client_message",
            "client_reference": client_reference,
            "tax_free_allowance": tax_free_allowance,
            "taxes_paid": client_aggr[client_reference]['taxes_paid'],
        }
        client_messages.append(client_msg)

    portfolio_messages = []
    for portfolio in portfolios:
        account_number = portfolio['account_number']
        portfolio_msg = {
            "type": "portfolio_message",
            "portfolio_reference": portfolio['portfolio_reference'],
            "cash_balance": account_aggr[account_number]['cash_balance'],
            "number_of_transactions": account_aggr[account_number]['number_of_transactions'],
            "sum_of_deposits": account_aggr[account_number].get('sum_of_deposits', 0),
        }
        portfolio_messages.append(portfolio_msg)

    print(client_messages)
    print(portfolio_messages)

    lock.release()
    lock_client.close()


def get_s3_object(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    return bucket, key

def get_date(key: str):
    m = re.match(r'(accounts|clients|portfolios|transactions)_(\d+).csv', key)
    return m.group(2)


def wait_for_s3_object(bucket_name: str, object_key: str, max_attempts: int = 20, delay_seconds: int = 5):
    s3_client = boto3.client('s3')

    waiter = s3_client.get_waiter('object_exists')

    print(f"Waiting for object: s3://{bucket_name}/{object_key}...")

    try:
        waiter.wait(
            Bucket=bucket_name,
            Key=object_key,
            WaiterConfig={
                'Delay': delay_seconds,
                'MaxAttempts': max_attempts
            }
        )
        total_wait_time = max_attempts * delay_seconds
        print(f"Success! Object found after at most {total_wait_time} seconds.")
        return True

    except WaiterError as e:
        print(f"Error: Object was not found after {max_attempts} attempts.")
        print(f"Waiter Error Details: {e}")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidBucketName':
            print(f"Error: Invalid bucket name provided: {bucket_name}")
        else:
            print(f"An unexpected AWS client error occurred: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False


def read_csv_from_s3_stream(bucket_name: str, key: str) -> list[dict]:
    s3_client = boto3.client('s3')

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_body = response['Body']
        csv_string = csv_body.read().decode('utf-8')
        io_string = StringIO(csv_string)
        reader = csv.DictReader(io_string)
        data_rows = list(reader)
        print(f"Successfully read {len(data_rows)} rows from s3://{bucket_name}/{key}")
        return data_rows

    except s3_client.exceptions.NoSuchKey:
        print(f"Error: File not found at s3://{bucket_name}/{key}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []
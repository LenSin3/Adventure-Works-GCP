import pandas as pd

def save_to_gcs_bucket(df, bucket_name, dest_dir_name, transformed_file_name):
    df.to_csv(f'gs://{bucket_name}/{dest_dir_name}/{transformed_file_name}.csv')

def transform_customers(file_uri, bucket_name, dest_dir_name, transformed_file_name):
    df = pd.read_csv(file_uri, encoding ='ISO-8859-1')
    # make first and last name title case
    df['FirstName'] = df['FirstName'].str.title()
    df['LastName'] = df['LastName'].str.title()
    # convert b irthdate to date
    df['BirthDate'] = pd.to_datetime(df['BirthDate'], yearfirst=True)
    # convert AnnualIncome to int64
    df['AnnualIncome'] = df['AnnualIncome'].str.replace('$', '').str.replace(',','').str.strip('').astype('int64')
    # save to gcs
    save_to_gcs_bucket(df, bucket_name, dest_dir_name, transformed_file_name)
    
# used for territories, categories and subcategories
def save_no_transform(file_uri, bucket_name, dest_dir_name, transformed_file_name):
    df = pd.read_csv(file_uri, encoding ='ISO-8859-1')
    save_to_gcs_bucket(df, bucket_name, dest_dir_name, transformed_file_name)
    
# transform products
def transform_products(file_uri, bucket_name, dest_dir_name, transformed_file_name):
    df = pd.read_csv(file_uri, encoding ='ISO-8859-1')
    df['ProductColor'] = df['ProductColor'].fillna('NoColor')
    save_to_gcs_bucket(df, bucket_name, dest_dir_name, transformed_file_name)
    
def transform_returns(file_uri, bucket_name, dest_dir_name, transformed_file_name):
    df = pd.read_csv(file_uri, encoding ='ISO-8859-1')
    df['ReturnDate'] = pd.to_datetime(df['ReturnDate'], yearfirst=True)
    save_to_gcs_bucket(df, bucket_name, dest_dir_name, transformed_file_name)

# reading all files in sales directory
def transform_sales(bucket_name, dest_dir_name, transformed_file_name):
    from google.cloud import storage 
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    frames = []
    for blob in blobs:
        if 'AdventureWorks_Sales' in blob.name:
            path = f'gs://{bucket_name}/{blob.name}'
            sales = pd.read_csv(path)
            sales['OrderDate'] = pd.to_datetime(sales['OrderDate'], yearfirst=True)
            sales['StockDate'] = pd.to_datetime(sales['StockDate'], yearfirst=True)
            frames.append(sales)

    df = pd.concat(frames)
    save_to_gcs_bucket(df, bucket_name, dest_dir_name, transformed_file_name)
        

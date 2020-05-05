import boto3
import configparser
import os

conf_file = configparser.ConfigParser()
conf_file.read('conf.cfg')

KEY = conf_file['AWS']['KEY']
SECRET = conf_file['AWS']['SECRET']
region = conf_file['AWS']['region']
project_bucket = conf_file['AWS']['project_bucket']
input_immigration_data = conf_file['AWS']['input_immigration_data']

def create_aws_client(client_for, KEY,SECRET,region):
    return boto3.client(client_for,
                      aws_access_key_id = KEY,
                      aws_secret_access_key = SECRET,
                      region_name = region)

def create_bucket(s3_client,bucket_name, region):
    location = {'LocationConstraint': region}
    s3_client.create_bucket(Bucket=project_bucket, CreateBucketConfiguration=location)

def move_csv_files(s3_client,files_list, s3_bucket, path):
    for file in files_list:
        s3_client.upload_file(file,s3_bucket, path + '/' + file)
    
    
if __name__ == "__main__":
        
    
    s3client = create_aws_client('s3',KEY,SECRET,region)
    
    create_bucket(s3client, project_bucket, region)
    
    for filename in os.listdir('../../data/18-83510-I94-Data-2016'):
        s3client.upload_file('../../data/18-83510-I94-Data-2016/'+ filename,project_bucket,input_immigration_data +'/2016/'+ filename)

    csv_files_list = ['country_codes.csv','port_of_entry.csv','US_Visas.csv','us-cities-demographics.csv']
    
    move_csv_files(s3client, csv_files_list,project_bucket, 'input')

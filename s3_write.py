
import boto3
from secrets import secret_access_key, access_key_id

client = boto3.client("s3",aws_access_key_id = access_key_id,aws_secret_access_key = secret_access_key)


file_name = "secrets.py"
upload_file_bucket = "fondosinversion"
upload_file_key = "python/"+ file_name
client.upload_file(file_name,upload_file_bucket,upload_file_key)
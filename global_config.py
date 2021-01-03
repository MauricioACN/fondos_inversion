import boto3
from secrets import access_key_id,secret_access_key

##acceso a credenciales de aws para leer datos en la nube
client = boto3.client("s3",aws_access_key_id = access_key_id,aws_secret_access_key = secret_access_key)

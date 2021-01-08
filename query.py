import ConsultasApi
import awshelper
import secrets
import pandas as pd

#Conexion a aws
aws = awshelper.AWSHelper()
aws.init_s3_session(aws_access_key_id=secrets.access_key_id,aws_secret_access_key=secrets.secret_access_key)

#lectura de data actual en aws
aws.call_s3_data(bucket_name="fondosinversion",file_name="datarentabilidad.csv",download_name="datarentabilidad_aws.csv")
aws.call_s3_data(bucket_name="fondosinversion",file_name="datafondos.csv",download_name="datafondos_aws.csv")

#consulta fondos de inversion
consulta = ConsultasApi.ConsultasApi()

#Leer base aws
renta_aws = pd.read_csv("datarentabilidad_aws.csv")
fondos_aws = pd.read_csv("datafondos_aws.csv")
#Union y borrar duplicados
consulta.datarentabilidad = consulta.datarentabilidad.append(renta_aws)
consulta.datarentabilidad.drop_duplicates(inplace=True)
consulta.datafondos = consulta.datafondos.append(fondos_aws)
consulta.datafondos.drop_duplicates(inplace=True)
#Escritura nueva base
consulta.datarentabilidad.to_csv("datarentabilidad.csv",index=False)
consulta.datafondos.to_csv("datafondos.csv",index=False)

#Cargue a aws
aws.load_s3_data(bucket="fondosinversion",file_name="datarentabilidad.csv")
aws.load_s3_data(bucket="fondosinversion",file_name="datafondos.csv")
import requests
import pandas as pd
import urllib3
import boto3
import os
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
import functions
from global_config import client

url = "https://fiduciaria.grupobancolombia.com/consultarFondosInversion/rest/servicio/buscarInformacionFondo"
url_fondos = "https://fiduciaria.grupobancolombia.com/consultarFondosInversion/rest/servicio/consultarListaFondos"

#informacion de los fondos nit y nombre
payload={}
headers = {}
r_fondos = requests.get(url_fondos, headers=headers, data=payload,verify = False)
data_fondos = r_fondos.json()
data_fondos = pd.io.json.json_normalize(data_fondos)
data_fondos.drop_duplicates(inplace=True)

#extraer rentabilidad de fondos
base_data = functions.extraer_rentabilidades(fondos = data_fondos,url = url)

variables_to_transf = ['rentabilidad.anios.anioCorrido', 'rentabilidad.anios.ultimoAnio','rentabilidad.anios.ultimos2Anios', 'rentabilidad.anios.ultimos3Anios',
       'rentabilidad.dias.mensual', 'rentabilidad.dias.semanal','rentabilidad.dias.semestral','valorDeUnidad', 'valorEnPesos']

base_data_clean = base_data.copy()
#limpieza preliminar del df
for i in variables_to_transf:
    base_data_clean[i] = functions.limpieza_numerical_var(base_data_clean,i)

####bases a escribir en S3
#base_data_clean ------> base limpia con limpieza sobre las variables numericas
#base_data ------> base limpia de la extracción para validaciones psoteriores
#data_fondos ------>  relacion fondo y nit para posteriro análisis

#validacion de no duplicados en la nube con la nueva data
data_fondos_aws = functions.read_csv_from_s3(bucket_name="fondosinversion",key = "databases/data_fondos.csv")
data_fondos_aws = data_fondos_aws.append(data_fondos)
data_fondos_aws.drop_duplicates(inplace = True)

base_data_aws = functions.read_csv_from_s3(bucket_name="fondosinversion",key = "databases/base_data.csv")
base_data_aws = base_data_aws.append(base_data)
base_data_aws.drop_duplicates(inplace = True)

base_data_clean_aws = functions.read_csv_from_s3(bucket_name="fondosinversion",key = "databases/base_data_clean.csv")
base_data_clean_aws = base_data_clean_aws.append(base_data_clean)
base_data_clean_aws.drop_duplicates(inplace = True)

#escribir bases de datos
data_fondos_aws.to_csv("data_fondos.csv",sep  = "|")
base_data_clean_aws.to_csv("base_data_clean.csv",sep  = "|")
base_data_aws.to_csv("base_data.csv",sep  = "|")

### escribir en aws
for file in os.listdir():
    if '.csv' in file:
        upload_file_bucket = "fondosinversion"
        upload_file_key = "databases/"+ str(file)
        client.upload_file(file,upload_file_bucket,upload_file_key)


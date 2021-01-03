def read_csv_from_s3(bucket_name, key):
    data = client.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_csv(data["Body"],sep = "|")
    return data

def extraer_rentabilidades(fondos,url):
    #base de rentabilidades y detalles de los fondos
    base_escritura = pd.DataFrame()
    #loop para extraer rentabilidades
    for i in fondos["nit"]:
        url_test = url+"/"+i
        r = requests.get(url_test, headers=headers, data=payload,verify = False)
        data = r.json()
        data = pd.io.json.json_normalize(data)
        base_escritura  = base_escritura.append(data, sort = False)
    return(base_escritura)
    
def limpieza_numerical_var(base,var_name):
    var = base[var_name].str.replace('\\,|\\%|\\.|\\$','').astype('int64')/100
    return(var)
import requests
import urllib3
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

class ConsultasApi:

    URLFONDOS="https://fiduciaria.grupobancolombia.com/consultarFondosInversion/rest/servicio/consultarListaFondos"
    URLRENTABILIDADES = "https://fiduciaria.grupobancolombia.com/consultarFondosInversion/rest/servicio/buscarInformacionFondo"
    VARIABLES = ['rentabilidad.anios.anioCorrido', 'rentabilidad.anios.ultimoAnio','rentabilidad.anios.ultimos2Anios','rentabilidad.anios.ultimos3Anios','rentabilidad.dias.mensual', 'rentabilidad.dias.semanal','rentabilidad.dias.semestral','valorDeUnidad', 'valorEnPesos']
    payload={}
    headers={}

    def __init__ (self):
        self.datafondos=self.data_extract(ConsultasApi.URLFONDOS)
        self.datarentabilidad=self._extraer_rentabilidades()
      
    def _extraer_rentabilidades(self):
        #base de rentabilidades y detalles de los fondos
        base_escritura = pd.DataFrame()
        #loop para extraer rentabilidades
        for i in self.datafondos["nit"]:
            url_test = ConsultasApi.URLRENTABILIDADES+"/"+i
            data = ConsultasApi.data_extract(url = url_test)
            base_escritura  = base_escritura.append(data, sort = False)
        return base_escritura
    
    @staticmethod
    def data_extract(url):
        r_fondos = requests.get(url = url, headers=ConsultasApi.headers, data=ConsultasApi.payload,verify = False)
        data_fondos = r_fondos.json()
        data_fondos = pd.json_normalize(data_fondos)
        data_fondos.drop_duplicates(inplace=True)
        return data_fondos 

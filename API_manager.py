#PREPARAMOS EL ENTORNO DE TRABAJO IMPORTANDO LAS LIBRERIAS NECESARIAS
from pprint import pprint
import pandas as pd
import requests, os, json, fastparquet, arrow
from configparser import ConfigParser
from datetime import datetime, timedelta

#ESTABLECEMOS LOS DIRECTORIOS PRINCIPALES DEL PROYECTO
project_root=os.path.join(os.path.dirname(__file__),"Datalake","Bronce")
stocks_dir=os.path.join(project_root,"Stocks")
market_dir=os.path.join(project_root,"Markets")
state_json_dir=os.path.join(os.path.dirname(__file__),"state.json")

if not os.path.exists(project_root):
    os.makedirs(project_root)

if not os.path.exists(stocks_dir):
    os.makedirs(stocks_dir)

if not os.path.exists(market_dir):
    os.makedirs(market_dir)


#CREAMOS LA CLASE QUE MANEJARA LAS CONSULTAS A LA API.
#EL OBJETIVO ES TENER UN OBJETO QUE ALMACENE TODOS LOS DETALLES SOBRE LA CONSULTA RELAIZADA.
#LOS METODOS QUE COMIENZAN CON "__" FUERON PENSADOS PARA SER UTILIZADOS EN LA CONSTRUCCION DE LA CLASE
#Y NO DIRECTAMENTE POR EL USUARIO
class manager:
    raiz="https://eodhd.com/api"
    
#LA MAYOR PARTE DE LOS ATRIBUTOS DE CLASE SE INICIALIZAN VACIOS DADO QUE SE IRAN ASIGNANDO
#AL REALIZAR LOS REQUESTS. EL ATRIBUTO TOKEN ES PROPORCIONADO DESDE EL ARCHIVO CONFIG.INI    
    def __init__(self):
        self.token=self.__readSecret_var("API","TOKEN")
        self.endpoint=None
        self.params={"api_token":self.token,"fmt":"json"}
        self.url=None
        self.req=None
        self.json=None
        self.req_type=None
        self.stock=None
        self.market=None
        self.state_json=None
        self.state_date=None
        self.df=None
        self.particion=None
        self.dir=None

#REALIZA LA CONSULTA Y GUARDA LA REQUEST, EL JSON Y LLAMA LA FUNCION MAKEDATAFRAME PARA CREAR UN DF
#A PARTIR DEL JSON. IMPRIME EN CONSOLA DETALLES DE LA CONSULTA
    def __makeRequest(self):        
        try:
            self.req=requests.get(self.url,self.params)
            self.json=self.req.json()
            print(f"Endpoint:{self.endpoint}")
            print(f"Estado de consulta:{self.req.status_code}")
            print(f"Parametros de consulta:",{key:value for key,value in self.params.items() if key!='api_token'})
            self.__makeDataframe()
            print(self.df.head(3))

        except requests.exceptions.RequestException:
            if self.req.text=="Exchange Not Found.":
                print(self.req.status_code,"ERROR - Corroborar que el Exchange ingresado sea correcto.")
            if self.req.text=="Ticker Not Found.":
                print(self.req.status_code,"ERROR - Corroborar que el Ticker ingresado sea correcto.")

#ESTA FUNCION LEE DATOS DEL ARCHIVO "CONFIG.INI"    
    def __readSecret_var(self, section, var):
        conf=ConfigParser()
        try:    
            conf.read("config.ini")
        except FileNotFoundError:
            print("config.ini no existe")
        return conf[section][var]
    
#ESTA FUNCION SERA UTILIZADA PARA LEER LA ULTIMA FECHA DE ACTUALIZACION DE LOS DATOS.
#EN CASO DE NO HALLAR UNA FECHA DE ACTUALIZACION PARA EL ITEM BUSCADO, ARROJARA UN VALOR ANOMALO
#PARA TRAER EL DATO MAS ANTIGUO DISPONIBLE
    def __readState(self):
        try:
            with open(state_json_dir, 'r') as file:
                self.state_json = json.load(file)
                try:
                    if self.req_type=="Stock":
                        self.state_date=self.state_json["Stock"][self.stock]
                        return self.state_date
                    if self.req_type=="Market":
                        self.state_date=json.load(file)["Market"][self.market]
                        return self.state_date
                except:
                    self.state_date="1990-1-1"
                    return self.state_date
        except FileNotFoundError:
            raise FileNotFoundError(f"El archivo JSON en la ruta {state_json_dir} no existe.")
        except json.JSONDecodeError:
            raise json.JSONDecodeError(f"El archivo JSON en la ruta {state_json_dir} no es válido.")

#GUARDA LA FECHA DE ULTIMA ACTUALIZACION. PARA ESTO COMPARA LA FECHA MAS ACTUAL DE LOS DATOS DE LA CONSULTA,
#CONTRA LA FECHA ALMACENADA EN EL STATE.JSON Y ESCRIBE LA MAYOR
    def __saveState(self):
        try:
            with open(state_json_dir, 'w') as file:                
                if self.req_type=="Stock":
                    last_date_df=self.df["stock_date"].sort_values().iloc[-1]
                    if last_date_df>datetime.strptime(self.state_date,"%Y-%m-%d"):
                        self.state_json["Stock"][self.stock]=str((last_date_df).date())
                if self.req_type=="Market":
                    self.state_json["Market"][self.market]=arrow.now().format("YYYY-MM-DD")
                json.dump(self.state_json, file, indent=4)
        except FileNotFoundError:
            raise FileNotFoundError(f"El archivo JSON en la ruta {state_json_dir} no existe.")
        except json.JSONDecodeError:
            raise json.JSONDecodeError(f"El archivo JSON en la ruta {state_json_dir} no es válido.")

#METODO PARA REALIZAR UNA REQUEST QUE PIDE LAS COTIZACION HISTORICAS DE UNA STOCK.
#SE LE DEBE PROPORCIONAR COMO PARAMETRO LA STOCK DESEADA Y EL PERIODO DE BUSQUEDA.
#BUSCARA DESDE LA ULTIMA FECHA DE ACTUALIZACION REGISTRADA EN STATE.JSON (+1 DIA) HASTA
#LA FECHA ACTUAL
    def getStockData(self, stock):
        self.req_type="Stock"
        self.dir=stocks_dir
        self.stock=stock
        self.particion=["stock_year","stock_month","stock_day","stock_ticker"]
        self.endpoint=f"eod/{stock}.US"
        self.params.update({"from":str(datetime.strptime(self.__readState(),"%Y-%m-%d").date()+timedelta(days=1))})
        self.url=f"{self.raiz}/{self.endpoint}"
        return self.__makeRequest()

#METODO PARA RELIZAR UNA REQUEST SOBRE LOS MERCADOS ESTADOUNIDENSES, SE DEBE PASAR COMO PARAMETRO EL MERCADO
#DESADO, EJ: NYSE, NASDAQ, PINK, ETC. LA FECHA NO ES RELEVANTE.    
    def getMarketData(self, market):
        self.req_type="Market"
        self.market=market
        self.dir=market_dir
        self.particion=["market_exchange"]
        self.endpoint=f"/exchange-symbol-list/{market}"
        self.params.update({"type":"common_stock","from":None})
        self.url=f"{self.raiz}/{self.endpoint}"
        self.__readState()
        return self.__makeRequest()
    
#ESTA FUNCION CREA UN DATAFRAME A PARTIR DE LA INFORMACION OBTENIDA EN LA REQUEST Y REALIZA DIFEFERENTES TRANSFORMACIONES
#DEPENDIENDO EL ENDPOINT CONSULTADO.
#SI EL DF ESTA VACIO QUIERE DECIR QUE LA CONSULTA NO TRAJO NUEVOS DATOS EN CONSECUNCIA ESTE METODO NO HARA NADA    
    def __makeDataframe(self):
#       CREAMOS EL DF A PARTIR DEL JSON QUE GUARDAMOS AL REALIZAR LA CONSULTA        
        df=pd.DataFrame.from_dict(self.json)
        if df.empty:
            print("La consulta esta vacia, no hay datos para la fecha consultada.")
        else:    
#           APLICAMOS TRANSFORMACION Al DF SOBRE COTIZACIONES      
            if self.req_type=="Stock":
    #           AGREGAMOS COLUMNA CON EL NOMBRE LA STOCK
                df["stock"]=self.stock
    #           TRANSFORMAMOS LA FECHA A TIPO DATE Y CAMBIAMOS EL FORMAT 
                df["date"]=pd.to_datetime(df["date"])
    #           AGREGAMOS COLUMNAS DE AÑO, MES Y DIA
                df["año"]=df["date"].dt.year
                df["mes"]=df["date"].dt.month
                df["dia"]=df["date"].dt.day
    #           AGREGAMOS UNA COLUMNA QUE SERA LA PRIMARY KEY DE ESTA TABLA, COMPUESTA POR LA FECHA Y LA STOCK
                df["key"]=df["date"].astype(str)+"/"+df["stock"]
    #           ELINAMOS COLUMNAAS INNECESARIAS
                if "adjusted_close" in df.columns:    
                    df.drop('adjusted_close', axis='columns', inplace=True)
                if "warning" in df.columns:
                    df.drop('warning', axis='columns', inplace=True)
    #           RENOMBRAMOS LAS COLUMNAS DEL DATAFRAME PARA QUE COINCIDAN CON LOS CAMPOS DE LA BASE DE DATOS    
                df.rename(inplace=True, columns={
                    "date":"stock_date",
                    "open":"stock_open",
                    "high":"stock_high",
                    "low":"stock_low",
                    "close":"stock_close",
                    "volume":"stock_volume",
                    "stock":"stock_ticker",
                    "año":"stock_year",
                    "mes":"stock_month",
                    "dia":"stock_day",  
                    "mean-30days":"stock_30daymean",
                    "dif_mean":"stock_dif_mean",
                    "key":"stock_key"})

    #       APLICAMOS TRANSFORMACION A LA INFO SOBRE MERCADOS            
            if self.req_type=="Market":
    #           FILTRAMOS LA COLUMNA TYPE POR EL ACTIVO COMMON STOCK
                df=df.loc[df["Type"]=="Common Stock"]
    #           ELIMNAMOS LA COLUMNA TYPE
                df.drop('Type', axis='columns', inplace=True)
    #           RENOMBRAMOS LAS COLUMNAS PARA QUE COINCIDAN CON LOS CAMPOS DE LA BASE DE DATOS            
                df.rename(inplace=True, columns={
                    "Code":"market_stockid",
                    "Name":"market_companyname",
                    "Country":"market_country",
                    "Exchange":"market_exchange",
                    "Currency":"market_currency",
                    "Isin":"market_stockisin"})
                
        self.df=df
            
#ESTE METODO GUARDA EL DF COMO PARQUET. SI EL DF QUEDO VACIO NO HACE NADA
    def save_to_parquet(self):
        if self.df.empty:
            print("No hay nada para cargar.")
        else:    
            self.df.to_parquet(self.dir, partition_cols=self.particion)
            self.__saveState()

#VACIA EL JSON CON LOS ESTADOS DE ACTUALIZACION DE LAS TABLAS PARA PODER CORRER NUEVAMENTE LOS PROCESOS
#DE ETL. ESTE METODO ES OPCIONAL
    def reiniciar_fecha_busqueda(self):
        try:
            with open(state_json_dir, 'w') as file:
                empty_json={
                    "Stock":{},
                    "Market":{}
                }
                json.dump(empty_json, file, indent=4)
        except FileNotFoundError:
            raise FileNotFoundError(f"El archivo JSON en la ruta {state_json_dir} no existe.")
        except json.JSONDecodeError:
            raise json.JSONDecodeError(f"El archivo JSON en la ruta {state_json_dir} no es válido.")        



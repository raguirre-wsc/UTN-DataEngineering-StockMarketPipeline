import psycopg2
import sqlalchemy
from configparser import ConfigParser
import pandas as pd

#ESTA CLASE SE ENCARGARA DE GESTIONAR LAS INTERACCIONES CON LA BASE DE DATOS
#LOS ATRIBUTOS PARA LA CONEXION SON TOMADOS EL ARCHIVO CONFIG.INI
class manager:
    def __init__(self):
        self.driver=self.__readSecret_var("DB","DRIVER")
        self.user=self.__readSecret_var("DB","USER")
        self.pwd=self.__readSecret_var("DB","PWD")
        self.host=self.__readSecret_var("DB","HOST")
        self.port=self.__readSecret_var("DB","PORT")
        self.dbname=self.__readSecret_var("DB","DBNAME")
        self.url=f"{self.driver}://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.dbname}"
        self.engine = sqlalchemy.create_engine(f"{self.driver}://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.dbname}")

#CREAMOS LA CLASE PARA LEER DEL ARCHIVO CONFIG.INI
    def __readSecret_var(self, section, var):
        conf=ConfigParser()
        try:    
            conf.read("config.ini")
        except FileNotFoundError:
            print("config.ini no existe")
        return conf[section][var]

#ESTE METODO SERVIRA PARA EJECUTAR CUALQUIER QUERY, DEBEMOS PASARLA COMO PARAMETRO
    def makeQuery(self, query):
        try:
            with self.engine.connect() as connection:
                con=connection.execute(sqlalchemy.text(query))
                return con 
        except sqlalchemy.exc.SQLAlchemyError as e:
            print(f"An error occurred: {e}")

#METODO PARA CREAR TABLAS EN LA BD, CREAMOS LOS SCHEMAS STAGE (CON LAS TABLAS STG_STOCK_PRICES Y STG_MARKETS)
#Y EL SCHEMA DATAWAREHOUSE (CON LAS TABLAS STOCK_PRICES Y MARKETS)        
    def createTables(self):
        create_tables_query="""
        BEGIN;
        CREATE SCHEMA IF NOT EXISTS stage;
        CREATE TABLE IF NOT EXISTS stage.stg_stock_prices(
            stock_date DATE,
            stock_open FLOAT,
            stock_high FLOAT,
            stock_low FLOAT,
            stock_close FLOAT,
            stock_volume INT,
            stock_ticker VARCHAR(10),
            stock_year INT,
            stock_month INT,
            stock_day INT,
            stock_key VARCHAR(20) PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS stage.stg_markets(
            market_stockid VARCHAR(10) PRIMARY KEY,
            market_companyname VARCHAR(255),
            market_country VARCHAR(25),
            market_exchange VARCHAR(12),
            market_currency VARCHAR(5),
            market_stockISIN VARCHAR(20)
        );

        CREATE SCHEMA IF NOT EXISTS datawarehouse;
        CREATE TABLE IF NOT EXISTS datawarehouse.stock_prices(
            stock_date DATE,
            stock_open FLOAT,
            stock_high FLOAT,
            stock_low FLOAT,
            stock_close FLOAT,
            stock_volume INT,
            stock_ticker VARCHAR(10),
            stock_year INT,
            stock_month INT,
            stock_day INT,
            stock_key VARCHAR(20) PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS datawarehouse.markets(
            market_stockid VARCHAR(10) PRIMARY KEY,
            market_companyname VARCHAR(255),
            market_country VARCHAR(25),
            market_exchange VARCHAR(12),
            market_currency VARCHAR(5),
            market_stockISIN VARCHAR(20)
        );
        COMMIT;
        """
        self.makeQuery(create_tables_query)     
        print("Tablas creadas correctamente.")

#METODO PARA TRUNCAR LOS DATOS DE LA TABLA ESPECIFICADA
    def truncate_table(self,schema,table):
        truncate_query=f"""TRUNCATE TABLE {schema}.{table};"""
        self.makeQuery(truncate_query)

#METODO PARA ELIMINAR TODOS LOS SCHEMAS DE LA BD
    def delete_schemas(self):
        query_drop_schemas="""
        DROP SCHEMA IF EXISTS datawarehouse, stage CASCADE;
        """ 
        self.makeQuery(query_drop_schemas)
        print("Se eliminaron todos los schemas.")

#ESTA FUNCION GUARDARA UN DF EN LA TABLA ESPECIFICADA EN EL SCHEMA STAGE, USA LA FUNCION DE GUARDADO DE PANDAS.
#DEBEMOS PROVEER LA TABLA Y EL DF COMO PARAMETROS
    def save_to_stage(self, table, dataframe):
        df=dataframe
        len=df.shape[0]
        if table=="stg_stock_prices":
            self.truncate_table("stage","stg_stock_prices")
            df.to_sql(con=self.engine, schema="stage", name="stg_stock_prices", index=False, if_exists='append', dtype={
                'stock_date':sqlalchemy.Date, 
                'stock_open':sqlalchemy.Float,
                'stock_high':sqlalchemy.Float,
                'stock_low':sqlalchemy.Float,
                'stock_close':sqlalchemy.Float,
                'stock_volume':sqlalchemy.Integer,
                'stock_ticker':sqlalchemy.String(10),
                'stock_year':sqlalchemy.Integer,
                'stock_month':sqlalchemy.Integer,
                'stock_day':sqlalchemy.Integer,
                'stock_key':sqlalchemy.String(20)}
                )
            print(f"Se cargaron {len} registros en la tabla stage.stg_stock_prices.")    
        if table=="stg_markets":
            self.truncate_table("stage","stg_markets")
            df.to_sql(con=self.engine, schema="stage", name="stg_markets", index=False, if_exists='append', dtype={
                'market_stockid': sqlalchemy.String(10),
                'market_companyname': sqlalchemy.String(255),
                'market_country': sqlalchemy.String(25),
                'market_exchange': sqlalchemy.String(12),
                'market_currency': sqlalchemy.String(5),
                'market_stockisin': sqlalchemy.String(20)}   
            )
            print(f"Se cargaron {len} registros en la tabla stage.stg_markets.")

#ESTE METODO PASARA LOS DATOS DEL SCHEMA DE STAGE AL WAREHOUSE, DEBEMOS ESPECIFICAR QUE TABLA QUEREMOS GUARDAR.
#UTILIZAREMOS LA ESTRATEGIA DE SCD TIPO 0 DEBIDO A QUE SON DATOS ESTATICOS QUE NO DEBERIAN RECIBIR MODIFCACIONES
    def commit_to_warehouse(self, table):
        if table=="markets":
            commit_query=""" 
            INSERT INTO datawarehouse.markets
            SELECT
                stg.market_stockid,
                stg.market_companyname,
                stg.market_country,
                stg.market_exchange,
                stg.market_currency,
                stg.market_stockisin
            FROM stage.stg_markets AS stg
            LEFT JOIN datawarehouse.markets AS dim
            ON stg.market_stockid = dim.market_stockid
            WHERE dim.market_stockid IS NULL;
            COMMIT;
            """
        if table=="stock_prices":
            commit_query=""" 
        INSERT INTO datawarehouse.stock_prices
        SELECT
            stg.stock_date, 
            stg.stock_open,
            stg.stock_high,
            stg.stock_low,
            stg.stock_close,
            stg.stock_volume,
            stg.stock_ticker,
            stg.stock_year,
            stg.stock_month,
            stg.stock_day,
            stg.stock_key
        FROM stage.stg_stock_prices AS stg
        LEFT JOIN datawarehouse.stock_prices AS dim
        ON stg.stock_key = dim.stock_key
        WHERE dim.stock_key IS NULL;
        COMMIT;
        """ 
        self.makeQuery(commit_query)
        print(f"Se guardaron los nuevos registros en la tabla datawarehouse.{table} exitosamente.")

    def get_last_price(self,stock):
        last_price_query=f"""
        SELECT
        TO_CHAR(S.stock_date, 'DD-MM-YYYY'),
        S.stock_ticker,
        M.market_companyname,
        S.stock_close,
        M.market_exchange,
        M.market_stockisin
        FROM datawarehouse.stock_prices AS S
        LEFT JOIN datawarehouse.markets AS M ON S.stock_ticker = M.market_stockid
        WHERE S.stock_date = (
            SELECT MAX(stock_date)
            FROM datawarehouse.stock_prices
            )
        AND S.stock_ticker='{stock}';
        """
        cur=self.makeQuery(last_price_query)
        return [print(x) for x in cur.fetchall()]








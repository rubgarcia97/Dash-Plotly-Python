from fastf1.ergast import Ergast

import fastf1 as f1
import pandas as pd
import numpy as np
import datetime as dt
import time

class GetTables():

    def __init__(self):
        
        self.__ergast = Ergast(result_type='pandas', auto_cast=True)
        self.__range = range(1950,dt.date.today().year,1)


    def events(self,rango=None):

        '''
        Genera un dataframe con todos los eventos que se han generado dentro del rango definido
        '''

        if rango is None:
            rango = self.__range



        data=[]
        for year in rango:

            try:
                for race in range(1,100):
                    events = f1.get_event(year=year,gp=race,backend='ergast')
                    data.append(events)

            except:
                continue

        dataframe = pd.concat(data,axis=1).T.reset_index(drop=True)
        dataframe['key'] = dataframe['RoundNumber'].astype(str) + dataframe['EventDate'].dt.year.astype(str)

        print(dataframe)


    def drivers(self, rango=None):

        '''
        Obtiene el listado de todos los pilotos que comienzan cada una de las temporadas, excluyendo de esta a los que hicieron participaciones puntuales en mitad de algún evento
        '''

        if rango is None:
            rango = self.__range
        

        unique_columns = set()
        df = pd.DataFrame()

        for year in rango:
            try:
                dataframe = self.__ergast.get_driver_info(season=year)
                dataframe['season'] = year

                # Actualiza las columnas únicas
                unique_columns.update(dataframe.columns)
                
                # Asegúrate de que el DataFrame tenga las mismas columnas que 'unique_columns'
                for column in unique_columns:
                    if column not in dataframe.columns:
                        dataframe[column] = np.nan  # Rellena con NaN si falta una columna
                
                # Concatena el DataFrame del año al DataFrame principal
                df = pd.concat([df, dataframe], ignore_index=True)
                

            except:
                continue



    def results(self, rango=None):

        if rango is None:
            rango = self.__range


        df=pd.DataFrame()
        iteration = []
        time_sleep = 60
        attempts = 0

        for year in rango:
            try:
                for round in range(1,100,1):
                    dataframe = self.__ergast.get_race_results(season=year,round=round)
                    dataframe = dataframe.content[0]
                    
                    data = pd.DataFrame(dataframe)
                    df = pd.concat([df,data],ignore_index=True)

                    iteration={year:round}
                    print(f'Iteration: {iteration}')

            except IndexError:

                continue

            except :
                if attempts == 0 : 

                    print(f"Límite de velocidad excedido. Esperando {time_sleep} segundos...")
                    time.sleep(time_sleep)

                elif attempts == 1:

                    print(f"Límite de peticiones excedido excedido. Esperando {time_sleep * 61} segundos...")

                else :
                    attempts += 1


                continue #Tengo sospechas de que cuando este 'continue' se da por limite de peticiones, pasa directamente al siguiente anyo en lugar de seguir las rondas. Revisar la logica

        df.to_csv("./results_testing.csv")


    def result_col(self,rango=None):

        if rango is None:
            rango = self.__range

        columns = []

        for year in rango:

            try:
                for round in range(1,100,1):

                    dataframe = self.__ergast.get_race_results(season=year,round=round)
                    dataframe = dataframe.content[0]

                    columns.append(len(dataframe.columns))

            except:
                continue

        print(columns)

        



if __name__=='__main__':
    GetTables().results()
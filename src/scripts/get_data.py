from fastf1.ergast import Ergast

import fastf1 as f1
import pandas as pd
import datetime as dt

class GetTables():

    def __init__(self):
        
        self.__ergast = Ergast()


    def events(self):

        data=[]
        for year in range(1950,dt.date.today().year,1):

            try:
                for race in range(1,100):
                    events = f1.get_event(year=year,gp=race,backend='ergast')
                    data.append(events)

            except:
                continue

        dataframe = pd.concat(data,axis=1).T.reset_index(drop=True)
        dataframe['key'] = dataframe['RoundNumber'].astype(str) + dataframe['EventDate'].dt.year.astype(str)

        print(dataframe)


    def drivers(self):
        
        dataframe = self.__ergast.get_driver_info(season=2006)
        print(dataframe)



if __name__=='__main__':
    GetTables().drivers()
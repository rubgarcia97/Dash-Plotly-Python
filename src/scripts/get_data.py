import fastf1 as f1
import pandas as pd
import datetime as dt

class GetTables():

    def __init__(self):
        pass

    def events1(self):
        events = f1.get_event(year=2022,gp=7,backend='ergast')
        
        events_df = pd.DataFrame(data=[events.values], columns=events.index)
        print(events_df)


    def events(self):

        data=[]
        for year in range(2020,dt.date.today().year,1):

            try:
                for race in range(1,100):
                    events = f1.get_event(year=year,gp=race,backend='ergast')
                    data.append(events)

            except:
                continue

        dataframe = pd.concat(data,axis=1).T.reset_index(drop=True)
        dataframe["key"] = dataframe['EventDate'].year
        print(dataframe.info())

if __name__=='__main__':
    GetTables().events()
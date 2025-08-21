from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class BigQuery:
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    @staticmethod
    def get_table(varname):
        return {'2m_air_temperature': 'air_temperature',
                'total_precipitation': 'precipitation_amount',
                '2m_relative_humidity': 'relative_humidity',
                '10m_u_component_of_wind': 'wind_speed_u',
                '10m_v_component_of_wind': 'wind_speed_v',
                '10m_wind_speed': 'wind_speed',
                '10m_wind_direction': 'wind_direction',
                '10m_wind_gust': 'wind_speed_of_gust',
                'msl_pressure': 'msl_pressure'}.get(varname, None)

    def connect(self):
        # Logic to connect to BigQuery
        pass

    def query(self, varname, start_date, end_date, provider=['inmet', 'cemaden']):
        # Logic to execute a SQL query
        # TODO: Add black list?
        # TODO: Create a table with station climatology?

        provider_str = ', '.join([f"'{value}'" for value in provider])
    
        query = f"""
        SELECT *
        FROM `{self.project_id}.stations.{self.get_table(varname)}`
            WHERE TIMESTAMP(datetime) <= TIMESTAMP('{end_date.isoformat()}')
                    AND TIMESTAMP(datetime) > TIMESTAMP('{start_date.isoformat()}')
                    AND station_provider IN ({provider_str})
        """

        df = self.client.query(query).to_dataframe(progress_bar_type='tqdm')#,
                                                    # bqstorage_client=bq.stclient) # TODO: Is it make sense?

        df['datetime'] = pd.to_datetime(df['datetime'])
        # df = df.drop_duplicates(subset=['datetime', 'station_id'], keep='last')

        df.set_index('datetime', inplace=True)
        df.index = pd.DatetimeIndex(df.index).floor('min')
        df.sort_index(inplace=True)
        df = df.sort_values(by=['station_id'])

        uniq_providers = np.unique(df['station_provider'].values)
        for up in uniq_providers:
            print(f"Total number of {up} stations: {len(np.unique(df[df['station_provider'] == up]['station_id'].values))}")
        return df
    
    def close(self):
        # Logic to close the connection
        pass
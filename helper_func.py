import logging
import json
import os
import time
import datetime as dt
from datetime import datetime
import numpy as np
import pandas as pd
from haversine import haversine, Unit
from sqlalchemy import create_engine
from urllib import parse
from azure.data.tables import TableServiceClient, UpdateMode
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.eventhub import EventData, EventHubProducerClient

#######################################################
def convert_utctimestamp_to_datetimesgt(utc_timestamp):
    '''
    This function converts the UTC timestamp to Singapore Timezone
    '''

    # Define the Singapore Timezone as UTC+8
    sgt = dt.timezone(dt.timedelta(hours=8))
    
    datetime_utc = datetime.fromisoformat(utc_timestamp)
    # Convert the UTC time to Singapore Timezone
    sgt_time = datetime_utc.astimezone(sgt)
    return sgt_time

#######################################################
def secret_retrieval():
    ''' 
    This function retrieves the Cosmos DB Connection String from Azure Key Vault
    '''
    
    # Retrieve Azure Key Vault URL from Application Settings
    key_vault_url = os.environ["KeyVaultUrl"]
    # Create a DefaultAzureCredential object to authenticate with Azure key vault using MSI
    credential = DefaultAzureCredential()

    # Create a SecretClient Object to retrieve the API key from Azure Key Vault
    try:
        client = SecretClient(vault_url=key_vault_url, credential=credential)
    except Exception as e:
        logging.error('Unable to connect to Azure Key Vault: ' + str(e))
        return e

    try:
        CosmosDbTableConnectionString = client.get_secret("CosmosDbTableConnectionString").value
        AzureSqlServer = client.get_secret("AzureSqlServer").value
        AzureSqlUserName = client.get_secret("AzureSqlUserName").value
        AzureSqlPassword = client.get_secret("AzureSqlPassword").value
    except Exception as e:
        logging.error('Unable to retrieve secrets from Azure Key Vault: ' + str(e))
        return e

    # Close credential when no longer needed.
    try:
        credential.close()
    except Exception as e:
        logging.error('Unable to close the credential: ' + str(e))

    return (CosmosDbTableConnectionString, AzureSqlServer, AzureSqlUserName, AzureSqlPassword)

#######################################################
def retrieve_data(CosmosConnectionString):
    '''
    This function retrieves the data from Cosmos DB and return the data in a pandas DataFrame
    '''

    # Initialize the TableServiceClient#
    try:
        table_service_client = TableServiceClient.from_connection_string(CosmosConnectionString)
    except Exception as e:
        logging.error('Unable to connect to Cosmos DB: ' + str(e))
        return e
    
    # Connect to the table
    try:
        table_name = "ltaData"
        table_client = table_service_client.get_table_client(table_name)
    except Exception as e:
        logging.error('Unable to connect to the table in Cosmos DB: ' + str(e))
        return e

    # Query the data and select only unprocessed data (i.e. processed == false)
    try:
        query  = "PartitionKey eq 'partitionkey' and processed eq false"
        entities = list(table_client.query_entities(query))
    except Exception as e:
        logging.error('Unable to query the table in Cosmos DB: ' + str(e))
        return e

    # Check if there is any new entities to be process, if there is no new data return the function. 
    logging.debug(len(entities))
    if len(entities) == 0:
        # return None
        logging.info('No new data to be processed')
        # close the connection to the table
        try:
            table_service_client.close()
        except Exception as e:
            logging.error('Unable to close the connection to Cosmos DB: ' + str(e))
        # raise an error to stop the function
        raise IndexError('No new data to be processed') 
    
    # Convert the entities to a pandas DataFrame
    df = pd.DataFrame(entities)
    numberOfRows = df.shape[0]
    logging.debug(df.shape)
    logging.debug(df.columns.values.tolist())

    return table_service_client, table_client, df, entities, numberOfRows

#######################################################
def df_init(df, df_new, df_target):
    '''
    This function initializes the new dataframes with the same RowKey (i.e. Index) as the original dataframe
    '''
    df_new['RowKey'] = df['RowKey'].copy()
    df_target['RowKey'] = df['RowKey'].copy()


#######################################################
def update_processed_flag(table_service_client, table_client, entities):
    '''
    This function updates the processed flag in the Cosmos DB table
    '''
    for entity in entities:
        entity["processed"] = True
        # Update the entity in the table
        try:
            table_client.update_entity(mode=UpdateMode.MERGE, entity=entity)
        except Exception as e:
            logging.error('Unable to update the processed flog in entity in Cosmos DB: ' + str(e))
            return e
        # Wait for 0.05 seconds to allow for time to update the processed flag in Cosmos DB 
        # (throttling to prevent hitting the RU/s limit for serverless Cosmos DB)
        time.sleep(0.05)
    logging.info('Processed flag updated in Cosmos DB')

    try:
        table_service_client.close()
    except Exception as e:
        logging.error('Unable to close the connection to Cosmos DB: ' + str(e))
        return e

#######################################################
def bus_arrival_processing(df_new, df, numberOfRows):
    '''
    This function processes the bus arrival data and fill in the waiting time in seconds for the next bus for each bus service number
    '''

    # Obtain the list of bus service numbers at the bus stop in front of Plaza Sinagpura
    # The list of bus services returned by API might not be the same throughtout the day as the 
    # bus service first bus and last bus timing might differ for each of the buses. 
    # Hence the setOfBuses is collected for all time interval throughout the day (and by definition set has no duplicates)
    setOfBuses = set()

    try:
        for row in range(numberOfRows):
            time_stamp = json.loads(df.loc[row]['bus_arrival'])
            for i in range(len(time_stamp['Services'])):
                setOfBuses.add(time_stamp['Services'][i]['ServiceNo'])
    except Exception as e:
        logging.error('Unable to retrieve the set of bus services: ' + str(e))
        return e
    
    listOfBuses = sorted(setOfBuses)
    logging.debug(listOfBuses) 
    logging.debug(len(listOfBuses)) 

    # Create new columns in the new dataframe for each bus and initialize the new columns to nan
    df_new[listOfBuses] = np.nan

    # Fill in the waiting time in seconds for the next bus for each bus service number
    try:
        for row in range(numberOfRows):
            bus = json.loads(df.loc[row]['bus_arrival'])
            datetime_rowkey = datetime.fromisoformat(df.loc[row,'RowKey'])
            for bus_counter in range(len(bus['Services'])):
                ServiceNo = bus["Services"][bus_counter]['ServiceNo']
                datetime_busarrival = datetime.fromisoformat(bus["Services"][bus_counter]['NextBus']['EstimatedArrival'])
                waittime = datetime_busarrival - datetime_rowkey
                if waittime.total_seconds() > 0:
                    df_new.loc[row, ServiceNo] = round(waittime.total_seconds())
                else: 
                    df_new.loc[row, ServiceNo] = 0
    except Exception as e:
        logging.error('Unable to process the bus arrival data: ' + str(e))
        return e


#######################################################
def carpark_availability_processing(df_new, df, numberOfRows):
    '''
    This function processes the carpark availability data and fill in the number of available lots for Plaza Singapura
    '''
    
    # Create a new column for AvailableLots and initialize the new column to nan
    df_new['AvailableLots'] = np.nan

    try:
        # Fill in the new column with the number of available lots for Plaza Singapura
        for row in range(numberOfRows):
            cp = json.loads(df.loc[row]['carpark_availability'])
            for carpark in cp['value']:
                if carpark['Development'] == "Plaza Singapura":
                    df_new.loc[row, 'AvailableLots'] = carpark['AvailableLots']
                    break
    except Exception as e:
        logging.error('Unable to process the carpark availability data: ' + str(e))
        return e


#######################################################
def estimate_travel_time_processing(df_new, df, numberOfRows):    
    '''
    This function processes the estimated travel time data (in minutes) and fill in the estimated travel time for the following routes:
    1. PIE/CTE (CITY) INTERCHANGE to PIE/CTE (SLE) INTERCHANGE
    2. BENDEMEER EXIT to PIE/CTE (CITY) INTERCHANGE
    3. ORCHARD RD to HAVELOCK RD
    4. CAIRNHILL CIRCLE to ORCHARD RD
    '''
    
    # Create a new columns for estimated time travels (etts) and initialize the new column to nan
    df_new[['EttStartPie', 'EttEndPie', 'EttStartOrchardrd', 'EttEndOrchardrd']] = np.nan

    try:
        # Fill in the new column with the estimated travel time
        for row in range(numberOfRows):
            etts = json.loads(df.loc[row]['estimated_travel_times'])
            for i in range(len(etts['value'])):
                if etts['value'][i]['StartPoint'] == 'PIE/CTE (CITY) INTERCHANGE':
                    df_new.loc[row, 'EttStartPie'] = etts['value'][i]['EstTime']
                if etts['value'][i]['EndPoint'] == 'PIE/CTE (CITY) INTERCHANGE':
                    df_new.loc[row, 'EttEndPie'] = etts['value'][i]['EstTime']
                if etts['value'][i]['StartPoint'] == 'ORCHARD RD':
                    df_new.loc[row, 'EttStartOrchardrd'] = etts['value'][i]['EstTime']
                if etts['value'][i]['EndPoint'] == 'ORCHARD RD':
                    df_new.loc[row, 'EttEndOrchardrd'] = etts['value'][i]['EstTime']
    except Exception as e:
        logging.error('Unable to process the estimated travel time data: ' + str(e))
        return e


#######################################################
def platform_crowd_processing(df_new, df, numberOfRows):
    '''
    This function processes the platform crowd data and fill in the platform crowd status for the following stations:
    1. NE6
    2. NS24
    3. CC1
    '''

    # Platform Crowd NEL
    # Create a new columns for platform crowd on NE6 and initialize the new column to nan
    df_new['PCNe6'] = np.nan

    # Fill in the new column with the platform crowd status
    for row in range(numberOfRows):
        PCNel = json.loads(df.loc[row]['platform_crowd_nel'])
        try:
            for item in PCNel['value']:
                if item['Station'] == 'NE6':
                    df_new['PCNe6'] = item['CrowdLevel']
        except Exception as e:
            logging.error('Unable to process the platform crowd data for NE6 at row {}: {}'.format(row, str(e)))

    # Platform Crowd NSL
    # Create a new columns for platform crowd on NS24 and initialize the new column to nan
    df_new['PCNs24'] = np.nan

    # Fill in the new column with the platform crowd status
    for row in range(numberOfRows):
        PCNsl = json.loads(df.loc[row]['platform_crowd_nsl'])
        try:
            for item in PCNsl['value']:
                if item['Station'] == 'NS24':
                    df_new['PCNs24'] = item['CrowdLevel']
        except Exception as e:
            logging.error('Unable to process the platform crowd data for NS24 at row {}: {}'.format(row, str(e)))

    # Platform Crowd CCL
    # Create a new columns for platform crowd on CC1 and initialize the new column to nan
    df_new['PCCc1'] = np.nan

    # Fill in the new column with the platform crowd status
    for row in range(numberOfRows):
        PCCcl = json.loads(df.loc[row]['platform_crowd_ccl'])
        try:
            for item in PCCcl['value']:
                if item['Station'] == 'CC1':
                    df_new['PCCc1'] = item['CrowdLevel']
        except Exception as e:
            logging.error('Unable to process the platform crowd data for CC1 at row {}: {}'.format(row, str(e)))

    

#######################################################
def taxi_availability_processing(df_new, df, numberOfRows):   
    '''
    This function processes the taxi availability data and fill in the number of taxis near Plaza Singapura
    '''
    
    # Create a new columns count of taxis near Plaza Sing and initialize the new column to nan
    df_new['TaxiCount'] = np.nan

    # Geo coordinate of Plaza Sing
    plazaSing = (1.3007676722621389, 103.84527851267403) # (Lat, Long)
    # Scope of search in meters
    dist = 1000

    try:
        # Fill in the new column with the count of taxi within the scope of search
        for row in range(numberOfRows):
            TaxiAvailability = json.loads(df.loc[row]['taxi_availability'])
            listTaxiNearPS = []
            for i in range(len(TaxiAvailability['value'])):
                taxi_location = (TaxiAvailability['value'][i]['Latitude'], TaxiAvailability['value'][i]['Longitude'])
                if haversine(taxi_location, plazaSing, unit=Unit.METERS) < dist:
                    listTaxiNearPS.append(taxi_location)
            df_new['TaxiCount'] = len(listTaxiNearPS)
    except Exception as e:
        logging.error('Unable to process the taxi availability data: ' + str(e))
        return e



#######################################################
def get_rainfall_value(data_rain):
    '''
    This function gets the rainfall value from the nearest weather station to Plaza Singapura
    '''

    # Geo coordinate of Plaza Sing
    plazaSing = (1.3007676722621389, 103.84527851267403) # (Lat, Long)
    
    # Get the distance between Plaza Sing and all the weather stations
    weather_station_dist = {}
    for station in data_rain['metadata']['stations']:
        weather_station_location = (station['location']['latitude'], station['location']['longitude'])
        dist_to_weather_station = haversine(weather_station_location, plazaSing, unit=Unit.METERS)
        weather_station_dist[station['id']] = int(dist_to_weather_station)

    # Get the nearest weather station
    nearest_weather_station = min(weather_station_dist, key=weather_station_dist.get)
    logging.debug("Nearest Weather Station: " + str(nearest_weather_station))
    
    # Get the rainfall value from the nearest weather station
    for item in data_rain['items']:
        for reading in item['readings']:
            if reading['station_id'] == nearest_weather_station:
                rainfall_value = reading['value']
    
    return rainfall_value

def rain_data_processing(df_new, df, numberOfRows):
    '''
    This function processes the rainfall data and fill in the rainfall value for the nearest weather station to Plaza Singapura
    '''
    # Create a new columns count for rainfall and initialize the new column to nan
    df_new['RainFall'] = np.nan

    for row in range(numberOfRows):
        data_rain = json.loads(df.loc[row]['rainfall'])
        rainfall_value = get_rainfall_value(data_rain)

        df_new.at[row, 'RainFall'] = rainfall_value

#######################################################

def azuresql_dataupload(AzureSqlServer, AzureSqlUserName, AzureSqlPassword, df_new):
    '''
    This function uploads the data to Azure SQL
    '''

    DRIVER = '{ODBC Driver 17 for SQL Server}'
    SERVER = AzureSqlServer
    DATABASE = 'azure_sql_ltadata'
    USERNAME = AzureSqlUserName
    PASSWORD = AzureSqlPassword

    params = parse.quote_plus(
    f'Driver={DRIVER};Server=tcp:{SERVER},1433;Database={DATABASE};Uid={USERNAME};Pwd={PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    )
    conn_str = f'mssql+pyodbc:///?odbc_connect={params}'

    # Create the engine
    try: 
        engine = create_engine(conn_str)
    except Exception as e:
        logging.error('Unable to create the engine: ' + str(e))

    # Upload the data to Azure SQL
    try:
        df_new.to_sql('ltaData', con=engine, if_exists='append', index=False)
    except Exception as e:
        logging.error('Unable to upload data to Azure SQL: ' + str(e))
        
    # Close the connection
    try:
        engine.dispose()
    except Exception as e:
        logging.error('Unable to close the connection: ' + str(e))

#######################################################

def send_to_eventhub(taxi_availability_json):
    ''' 
    This function sends the taxi availability data to Event Hub
    '''
    
    # Retrieve Event Hub Namespace String from Application Settings
    EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = os.environ["EVENT_HUB_FULLY_QUALIFIED_NAMESPACE"]
    # Retrieve Event Hub Name from Application Settings
    EVENT_HUB_NAME = os.environ["EVENT_HUB_NAME"]
    
    try:
        credential = DefaultAzureCredential()
    except Exception as e:
        logging.error('Unable to create the credential: ' + str(e))
    
    # Create a producer client to send messages to the event hub.
    try:
        producer = EventHubProducerClient(
            fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
            eventhub_name=EVENT_HUB_NAME,
            credential=credential
            )
    except Exception as e:
        logging.error('Unable to create the producer client: ' + str(e))
    
    # Send the batch of 1 event to the event hub.
    try:       
        # producer.send_batch(event_data_batch)
        producer.send_batch([EventData(json.dumps(taxi_availability_json))])
    except Exception as e:
        logging.error('Unable to send the batch of events to the event hub: ' + str(e))    

    # Close the producer.
    try:
        producer.close()
    except Exception as e:
        logging.error('Unable to close the producer: ' + str(e))

    # Close credential when no longer needed.
    try:
        credential.close()
    except Exception as e:
        logging.error('Unable to close the credential: ' + str(e))
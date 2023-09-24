import datetime
import logging
import time
import azure.functions as func
import pandas as pd
from helper_func import secret_retrieval, retrieve_data, df_init, update_processed_flag
from helper_func import bus_arrival_processing, carpark_availability_processing 
from helper_func import estimate_travel_time_processing, platform_crowd_processing
from helper_func import rain_data_processing, taxi_availability_processing
from helper_func import azuresql_dataupload, send_to_eventhub
from helper_func import convert_utctimestamp_to_datetimesgt


app = func.FunctionApp()
@app.schedule(schedule="0 0 */4 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 


def azurefunction_cosmosdb_dataprocessing(myTimer: func.TimerRequest):
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    # Convert the UTC time to Singapore Timezone
    sgt_time = convert_utctimestamp_to_datetimesgt(utc_timestamp)

    # Only run the function between 6am to 12am Singapore Time
    if sgt_time.hour < 6 or sgt_time.hour >= 24:
        return

    # Retrieve Cosmos DB Connection String from Azure Key Vault
    try:
        CosmosDbTableConnectionString, AzureSqlServer, AzureSqlUserName, AzureSqlPassword = secret_retrieval()
    except Exception as e:
        logging.error('Azure Key Vault Connection Failed ' + str(e))
        return

    # Retrieve data from Cosmos DB
    try:
        table_service_client, table_client, df, entities, numberOfRows = retrieve_data(CosmosDbTableConnectionString)
    except Exception as e:
        logging.info('Data Retrieval Failed or there is no new data ' + str(e))
        return

    # Wait for 10 seconds to allow for time to retrieve data from Cosmos DB
    time.sleep(10)

    # Create a new dataframes to hold the processed data
    df_new = pd.DataFrame()
    df_target = pd.DataFrame()

    # Initialize the new dataframes
    df_init(df, df_new, df_target)


    # Process the data
    bus_arrival_processing(df_new, df, numberOfRows)
    carpark_availability_processing(df_new, df, numberOfRows)
    estimate_travel_time_processing(df_new, df, numberOfRows)
    platform_crowd_processing(df_new, df, numberOfRows)
    taxi_availability_processing(df_target, df, numberOfRows)
    taxi_availability_processing(df_new, df, numberOfRows)
    rain_data_processing(df_new, df, numberOfRows)
    logging.info('Data Processing Completed')
    

    # Upload the target data to Azure Event Hub
    send_to_eventhub(df_target.to_json(orient="index"))
    logging.info('Azure Event Hub Target Data Upload Completed')

    # Upload the processed data to Azure SQL
    azuresql_dataupload(AzureSqlServer, AzureSqlUserName, AzureSqlPassword, df_new)
    logging.info('Azure SQL Data Upload Completed')

    update_processed_flag(table_service_client, table_client, entities)
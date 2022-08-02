# My imports
import logging
import json
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from io import BytesIO

'''This is an Azure Queue triggered function.
   The function gets triggered whenever the logs are pushed to the queue.
   The function pushes the received/loaded message/logs to the delta lake as dataframes.
   Taking updates and overwrites into consideration.'''

def main(msg: func.QueueMessage):
    logging.info('Python queue trigger function processed a queue item.')
    
    message = json.loads(msg.get_body().decode('utf-8'))

    # Creating a dataframe from my passed message
    msg_df = pd.DataFrame(data = message)

    # Creating the token credential, an object of the defaultAzureCredential Class
    token_credential = DefaultAzureCredential()
    
    # Creating a service client using the account_url and the token credentials
    blob_service_client = BlobServiceClient(
        account_url = "https://store01asa.dfs.core.windows.net/",
        credential = token_credential)

    # Creating the container and uploading our blobs
    try:

        try:
            # Creating the container using the blob_servce_client if the container does not exists
            container_client = blob_service_client.create_container("Investec_Next_Logs_Container")

            # Upload a my_file.csv file into my_files directory inside the container
            container_client.upload_blob(name = "Consumption_logs/Predict_Logs/inputs.csv", data = msg_df.to_csv(header = True, index = False), overwrite = True)
            logging.info("file uploaded sucessfully")
        
        except:
            # If the container already exists catch the exception thrown and allow it
            logging.info("Container already exists, beginning upload")

            # Get the existing container using blob_service_client
            container_client = blob_service_client.get_container_client("Investec_Next_Logs_Container") 
            
            # Download the already existing blob from the container
            try:
                consumption_blob_data = container_client.download_blob("Consumption_logs/Predict_Logs/inputs.csv").readall()
                existing_blob_df = pd.read_csv(BytesIO(consumption_blob_data))
                logging.info(existing_blob_df)

            except:
                container_client.upload_blob(name = "Consumption_logs/Predict_Logs/inputs.csv", data = msg_df.to_csv(header = True, index = False), overwrite = True)
                logging.info("file uploaded sucessfully")

            consumption_blob_data = container_client.download_blob("Consumption_logs/Predict_Logs/inputs.csv").readall()
            existing_blob_df = pd.read_csv(BytesIO(consumption_blob_data))
            logging.info(existing_blob_df)

            # Append the passed msg_df to the exisiting_blob_df assign that to the final df to be uploaded
            final_df = existing_blob_df.append(msg_df, ignore_index = True)

            # Upload the final_df to the blob and overwrite the existing_df  
            container_client.upload_blob(name = "Consumption_logs/Predict_Logs/inputs.csv", data = final_df.to_csv(header = True, index = False), overwrite = True)
            logging.info("file uploaded sucessfully")

    except Exception as ex:
        print('Exception:')
        print(ex)

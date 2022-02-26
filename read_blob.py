import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient

class AzureBlobFileDownloader:
    def __init__(self, MY_CONNECTION_STRING, MY_BLOB_CONTAINER, LOCAL_BLOB_PATH):
        print("Intializing AzureBlobFileDownloader")
        # Initialize the connection to Azure storage account
        self.blob_service_client =  BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(MY_BLOB_CONTAINER)
        self.local_blob_path = LOCAL_BLOB_PATH
 
    def save_blob(self,file_name,file_content):
        # Get full path to the file
        download_file_path = os.path.join(self.local_blob_path, file_name)
    
        # for nested blobs, create local path as well!
        print(file_name)
        if os.path.splitext(download_file_path)[-1] == '':
            print('sadfasdf')
            download_file_path += '/'
            print(download_file_path)
        
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

        try:
            with open(download_file_path, "wb") as file:
                file.write(file_content)
        except IsADirectoryError:
            pass
    
    def download_all_blobs_in_container(self):
        my_blobs = self.my_container.list_blobs()
        for blob in my_blobs:
            # print(blob.name)
            bytes = self.my_container.get_blob_client(blob).download_blob().readall()
            self.save_blob(str(blob.name), bytes)
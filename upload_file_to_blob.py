# upload_file_to_blob.py
# Python program to bulk upload files as blobs to azure storage
# Uses latest python SDK() for Azure blob storage
# Requires python 3.6 or above
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient

class AzureBlobFileUploader:
  def __init__(self, MY_CONNECTION_STRING, MY_FILE_CONTAINER, LOCAL_FOLDER_PATH):
    '''
    Initialize instance of AzureBlobFileUploader.
    INPUTS:
      - MY_CONNECTION_STRING (str): Connection string associated with storage account.
      - MY_FILE_CONTAINER (str): Name of file container in storage service to upload local data to.
      - LOCAL_FOLDER_PATH (str): Name of designated file upload folder on local device.
    =====
    RETURNS:
      - None
    '''
    print("Intializing AzureBlobFileUploader")

    # Set class inputs
    self.my_connection_string = MY_CONNECTION_STRING
    self.my_file_container = MY_FILE_CONTAINER
    self.local_folder_path = LOCAL_FOLDER_PATH
 
    # Initialize the connection to Azure storage account
    self.blob_service_client =  BlobServiceClient.from_connection_string(self.my_connection_string)
 
  def upload_all_files_in_folder(self, extension='.'):
    '''
    Uploads all files with designated file extension inside LOCAL_FOLDER_PATH to container in blob storage. Replicates file hierarchy.
    INPUTS:
      - extension (str): Desired file extension. Default is '.', indicates all file types will be uploaded to blob container.
    =====
    RETURNS:
      - None
    '''
    # Get all files with specified extension and exclude directories
    all_file_names = [f for f in os.listdir(self.local_folder_path)
                    if os.path.isfile(os.path.join(self.local_folder_path, f)) and extension in f]
 
    # Upload each file
    for file_name in all_file_names:
      self.upload_file(file_name)
 
  def upload_file(self,file_name):
    '''
    Uploads single file inside designated upload folder.
    INPUTS:
      - file_name (str): Filename or filepath relative to LOCAL_FOLDER_PATH location.
    =====
    RETURNS:
      - None
    '''
    # Create blob with same name as local file name
    blob_client = self.blob_service_client.get_blob_client(container=self.my_file_container,
                                                          blob=file_name)
    # Get full path to the file
    upload_file_path = os.path.join(self.local_folder_path, file_name)
 
    # Create blob on storage
    # Overwrite if it already exists!
    file_content_setting = ContentSettings(content_type='application/octet-stream')
    print(f"uploading file - {file_name}")
    with open(upload_file_path, "rb") as data:
      blob_client.upload_blob(data,overwrite=True,content_settings=file_content_setting)





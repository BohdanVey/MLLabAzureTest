import os

from azure.storage.blob import BlockBlobService, PublicAccess


class BlobConnectContainer:

    def __init__(self, blob_container, blob_service_client):
        self.blob_service_client = blob_service_client
        blob_service_client.create_container(blob_container)
        blob_service_client.set_container_acl(
            blob_container, public_access=PublicAccess.Container)
        self.container_name = blob_container

    def add_element(self, location, name=None):
        if not name:
            name = location.split('/')[-1]
        blob_service_client.create_blob_from_path(
            self.container_name, name, location)

    def get_element(self, name, location):
        self.blob_service_client.get_blob_to_path(
            self.container_name, name, location)

    def get_all_element(self, folder_name):
        if not os.path.exists(folder_name):
            os.makedirs(os.path.expanduser(folder_name))
        generator = self.blob_service_client.list_blobs(self.container_name)
        for blob in generator:
            self.get_element(blob.name, os.path.join(folder_name, blob.name))

    def clear_container(self):
        check = input("Do you really want to clear the blob?")
        if check == "yes":
            self.blob_service_client.delete_container(self.container_name)

    def clear_blob(self, blob_name):
        self.blob_service_client.delete_blob(self.container_name, blob_name)


def add_folder(folder_location, blob_name, blob_service_client):
    blob_container = BlobConnectContainer(blob_name, blob_service_client)
    for file_name in os.listdir(folder_location):
        path = os.path.join(folder_location, file_name)
        blob_container.add_element(path)
    return blob_container


blob_service_client = BlockBlobService(
    account_name=<acount_name>,
    account_key=<account_key>)

container = add_folder('folder', 'test', blob_service_client)
container.get_all_element('test')
container.clear_blob('a.txt')

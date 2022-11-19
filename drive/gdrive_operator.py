import io
from typing import List, Dict

from logger.logger import logger
from apiclient import errors
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

CREDS = './creds/creds.json'


class GDriveOperator:
    def __init__(self, credentials=CREDS):
        credentials = service_account.Credentials.from_service_account_file(
            credentials
        )
        self.service = build("drive", "v3", credentials=credentials)
        self.__main_drive_folder = None

    @property
    def main_drive_folder(self) -> str:
        if self.__main_drive_folder is None:
            logger.info('Finding the main folder. . .')
            self.__main_drive_folder = self.find_file(
                'gdrive-operator'
            )["id"]
        return self.__main_drive_folder

    def list_content(self) -> List[Dict]:
        try:
            files = []
            page_token = None
            while True:
                response = self.service.files().list(
                    spaces='drive',
                    fields='nextPageToken, files(id, name, webViewLink)',
                    supportsAllDrives=True,
                    pageToken=page_token,
                ).execute()
                for file in response.get('files', []):
                    logger.info(
                        f'Found file: {file.get("name")}, {file.get("id")}')
                files.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

        except HttpError as error:
            logger.error(f'An error occurred: {error}')
            files = None

        return files

    def upload_file(
            self,
            file_name: str,
            local_folder: str,
            location: str = None
    ) -> None:
        if self.find_file(file_name):
            logger.warning(f'FILE {file_name} ALREADY EXISTS!')
            return
        if not location:
            location = self.main_drive_folder
        location_id = self.find_file(location).get("id")
        file_path = f"./{local_folder}/{file_name}"
        try:
            file_metadata = {
                'name': file_name,
                'parents': [location_id],
            }
            media = MediaFileUpload(
                file_path,
            )
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='name',
                supportsAllDrives=True,
            ).execute()
            logger.info(
                f'File {file.get("name")} has been successfully uploaded!'
            )

        except HttpError as error:
            logger.error(f'An error occurred: {error}')


    def download_file(
            self,
            file_name: str,
            local_folder: str,
            location: str = None
    ) -> None:
        file_id = self.find_file(file_name, location).get("id")
        try:
            request = self.service.files().get_media(
                fileId=file_id,
                supportsAllDrives=True,
            )
            if local_folder:
                file_path = io.FileIO(f"./{local_folder}/{file_name}", "wb")
            else:
                if file_name.startswith("/"):
                    file_path = io.FileIO(file_name, "wb")
                else:
                    file_path = io.FileIO(f"./{file_name}", "wb")
            downloader = MediaIoBaseDownload(file_path, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                logger.info(f'Download {int(status.progress() * 100)}.')

        except HttpError as error:
            logger.error(f'An error occurred: {error}')

    def remove_file(self, file_name: str, location: str = None):
        result = self.find_file(file_name, location)
        if not result:
            logger.warning(f"File {file_name} does NOT EXIST !")
            return
        try:
            self.service.files().delete(
                fileId=result.get("id"),
                supportsAllDrives=True,
            ).execute()
            logger.info(f'File {file_name} has been successfully deleted!')
        except errors.HttpError as error:
            logger.error(f'Error while deleting {file_name}! ERROR: {error}')

    def create_folder(self, folder_name: str, duplicate=False) -> None:
        if not duplicate:
            file = self.find_file(folder_name)
            if file:
                logger.warning(f"Folder {folder_name} already exists!")
                return
        try:
            file_metadata = {
                'name': folder_name,
                'parents': [self.main_drive_folder],
                'mimeType': 'application/vnd.google-apps.folder',
            }
            file = self.service.files().create(
                body=file_metadata,
                fields='id',
                supportsAllDrives=True,
            ).execute()
            logger.info(
                f'Created folder - Folder ID: "{file.get("id")}"; '
                f'NAME: "{file.get("name")}".'
            )
            return

        except HttpError as error:
            logger.error(f'An error occurred: {error}')
            return

    def find_file(self, file_name: str, location: str = None) -> Dict:
        try:
            query = f"name = '{file_name}' and trashed=false"
            if location:
                query = f"{location}' in parents and name = '{file_name}' " \
                        f"and trashed=false"
            files = []
            page_token = None
            while True:
                response = self.service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, webViewLink)',
                    supportsAllDrives=True,
                    pageToken=page_token,
                ).execute()
                for file in response.get('files', []):
                    logger.info(
                        f'Found file: {file.get("name")}, {file.get("id")}')
                files.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

        except HttpError as error:
            logger.error(f'An error occurred: {error}')
            files = None
        if not files:
            logger.warning("No such file!")
            return {}
        return files[0]

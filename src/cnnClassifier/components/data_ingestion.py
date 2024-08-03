import os
import zipfile
import gdown
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
from cnnClassifier.entity.config_entity import DataIngestionConfig


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self) -> str:
        """
        Fetch data from the URL
        """
        try:
            dataset_url = self.config.source_url
            zip_download_dir = self.config.local_data_file
            os.makedirs(zip_download_dir.parent, exist_ok=True)
            logger.info(f"Downloading data from {dataset_url} into file {zip_download_dir}")

            file_id = dataset_url.split("/")[-2]
            prefix = 'https://drive.google.com/uc?export=download&id='
            gdown.download(prefix + file_id, str(zip_download_dir))  # Ensure path is a string

            logger.info(f"Downloaded data from {dataset_url} into file {zip_download_dir}")

        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise
        
    def extract_zip_file(self):
        """
        Extracts the zip file into the data directory
        """
        try:
            unzip_path = self.config.unzip_dir
            os.makedirs(unzip_path, exist_ok=True)
            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_path)

            logger.info(f"Extracted zip file to {unzip_path}")

        except Exception as e:
            logger.error(f"Failed to extract zip file: {e}")
            raise
from cnnClassifier.constants import *
from cnnClassifier.utils.common import read_yaml, create_directories
from cnnClassifier.entity.config_entity import DataIngestionConfig

import os

class ConfigurationManager:
    def __init__(self, config_filepath=CONFIG_FILE_PATH, params_filepath=PARAMS_FILE_PATH):
        self.config = read_yaml(config_filepath)  # Make sure this returns a dictionary
        self.params = read_yaml(params_filepath)
        create_directories([Path(self.config['artifacts_root'])])

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config['data_ingestion']  # Assuming this is a dictionary

        create_directories([Path(config['root_dir'])])

        data_ingestion_config = DataIngestionConfig(
            root_dir=Path(config['root_dir']),
            source_url=config['source_url'],  # Adjusted to match the DataIngestionConfig
            local_data_file=Path(config['local_data_file']),
            unzip_dir=Path(config['unzip_dir'])
        )

        return data_ingestion_config
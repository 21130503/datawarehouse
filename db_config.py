import configparser

class DbConfig:
    def __init__(self):
        self.config = self.load_config()
        # Cấu hình từ file `config.ini`
        self.data_dir = self.config['data']['data_dir']
        self.casualties_csv = self.config['data']['casualties']
        self.crash_csv = self.config['data']['crash']
        self.datetime_csv = self.config['data']['datetime']
        self.description_csv = self.config['data']['description']
        self.location_csv = self.config['data']['location']
        self.vehicles_csv = self.config['data']['vehicles']
        self.db_connection = f"mysql+pymysql://{self.config['database']['user']}:" \
                             f"{self.config['database']['password']}@{self.config['database']['host']}/" \
                             f"{self.config['database']['database']}?charset={self.config['database']['charset']}"
        self.chromedriver_path = self.config['path']['chromedriver']
        self.db_user = self.config['database']['user']

    def load_config(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        return config

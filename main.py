import os
from time import sleep

from dotenv import load_dotenv
from cloud.yandex import YandexDiskUploader

if __name__ == "__main__":
    load_dotenv()
    uploader = YandexDiskUploader(
        token=f"{os.getenv('TOKEN')}",
        base_folder=f"{os.getenv('BASE_DIR_PATH')}",
        savedir=f"{os.getenv('CLOUD_DIR_NAME')}",
        logpath=f"{os.getenv('LOG_DIR_PATH')}",
    )
    while True:
        uploader.get_info()
        check = os.getenv("CHECK_INTERVAL")
        if check is not None:
            sleep(int(check))
        else:
            sleep(5000)

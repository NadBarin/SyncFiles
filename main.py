import os
from yandex import YandexDiskUploader


TOKEN = "y0_AgAAAAASKx4kAAxhPAAAAAEP1YV2AAAtOQASNSVLCKb5RkYnU8Ubj2NwgQ"


if __name__ == "__main__":
    uploader = YandexDiskUploader(
        token=TOKEN,
        base_folder=r"C:\Users\Надежда\PycharmProjects\SyncFiles\test\паапка",
    )
    r"""uploader.load(
        path=r"C:\Users\Надежда\PycharmProjects\SyncFiles\test\паапка"
    )"""
    uploader.get_info()
    '''uploader.delete(
        file_path=r"test/This.txt"
    )'''
    r'''uploader.reload(path=r"C:\Users\Надежда\PycharmProjects\SyncFiles\test\паапка")'''

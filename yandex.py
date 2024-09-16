import time
from datetime import datetime, timezone
import sys
import requests

import os
from loguru import logger

logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {message}")
logger.add("logs/logs.log", format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {message}", rotation="1 day")

class YandexDiskUploader:
    def __init__(self, token, base_folder):
        self.token = token
        self.base_folder = base_folder
        self.savedir = os.path.basename(self.base_folder)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"OAuth {self.token}",
        }

    def _upload_file(self, urlpath, filepath):
        """Загружает файл в облако.
        urlpath: путь для облачного хранилища
        filepath: путь к файлу на компьютере"""
        try:
            response = requests.get(
                f"https://cloud-api.yandex.net/v1/disk/resources/upload?path={urlpath.replace(f'\\', '/').replace('./', '')}",
                headers=self.headers)
            response_json = response.json()
            params = {'url': response_json['href']}
        except KeyError:
            logger.error(response_json['message'])
        with open(filepath.replace('.\\', ''), 'rb') as f:
            try:
                requests.put(response_json['href'], params=params, files={'file': f})
            except KeyError:
                logger.error(response_json)

    def load(self, path):
        """Создаёт папку с заданым в savedir названием и если передаётся путь к папке
        то загружает все файлы и папки на диск.
        path: путь к папке или файлу, который нужно загрузить на диск"""
        requests.put(
            f"https://cloud-api.yandex.net/v1/disk/resources?path={self.savedir}",
            headers=self.headers,
        )
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                relpath = os.path.relpath(root, self.base_folder)
                normalized_relpath = os.path.normpath(os.path.join(self.savedir, relpath))
                requests.put(
                    f"https://cloud-api.yandex.net/v1/disk/resources?path={normalized_relpath.replace(f'\\', '/').replace('./', '')}",
                    headers=self.headers,
                )
                for file in files:
                    self._upload_file(urlpath=os.path.join(normalized_relpath, file),
                                      filepath=os.path.normpath(os.path.join(self.base_folder, relpath, file)))
        else:
            self._upload_file(urlpath=os.path.join(self.savedir, os.path.basename(path)),
                              filepath=path)

    def reload(self, path):
        """сравнивает время на диске и время в системе и удаляет
        если время в системе больше чем на диске и снова загружает уже новый."""
        try:
            relpath = os.path.relpath(path, self.base_folder)
            normalized_relpath = os.path.normpath(os.path.join(self.savedir, relpath))
            response = requests.get(f"https://cloud-api.yandex.net/v1/disk/resources?path={normalized_relpath.replace(f'\\', '/').replace('./', '')}", headers=self.headers)
            if response.status_code == 200:
                file_info = response.json()
                modified = file_info.get('modified')  # Дата последнего изменения файла
                modified = datetime.fromisoformat(modified).astimezone(timezone.utc)
                stat = os.stat(path)
                modification_time = stat.st_mtime
                modification_time = datetime.fromtimestamp(modification_time, tz=timezone.utc)
                if modified<modification_time:
                    self.delete(normalized_relpath.replace(f'\\', '/').replace('./', ''))
                    self.load(path)
                    logger.info("Файлы обновлены")
            elif response.status_code == 404:
                # Если файл не найден, загружаем новый
                self.load(path)
                logger.info("Файл не найден на сервере, загружен новый файл.")
            else:
                logger.error(f"Ошибка при получении информации о файле: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logger.error(f"Ошибка при получении информации о файле: {e}")
        except Exception as e:
            logger.error(f"Произошла ошибка: {e}")


    def delete(self, file_path):
        if not file_path.startswith(os.sep):
            file_path = '/' + file_path
        response = requests.delete(f"https://cloud-api.yandex.net/v1/disk/resources?path={os.path.join(self.savedir, file_path).replace(f'\\', '/').replace('./', '')}",
        headers=self.headers)
        if response.status_code == 204:
            logger.info(f"Файл '{file_path}' успешно удален из Яндекс.Диска.")
        elif response.status_code == 404:
            logger.info(f"Файл '{file_path}' не найден.")
        elif response.status_code == 202:
            status_url = response.json().get('href')
            while True:
                response = requests.get(status_url, headers=self.headers)
                if response.status_code == 200:
                    status = response.json().get('status')
                    if status == 'success':
                        logger.info(f"Файл '{file_path}' успешно удален из Яндекс.Диска.")
                        break
                    elif status == 'failed':
                        logger.info("Операция завершилась с ошибкой.")
                        break
                elif response.status_code == 202:
                    logger.info("Операция еще выполняется, проверка через 5 секунд...")
                else:
                    logger.error(f"Ошибка при удалении файла: {response.status_code} - {response.text}")
                    break
                time.sleep(2)  # Пауза перед следующей проверкой
        else:
            logger.error(f"Ошибка при удалении файла: {response.status_code} - {response.text}")


    def get_info(self):
        queue = [self.savedir]
        while queue:
            current_path = queue.pop()  # Получаем следующий путь из очереди
            url = 'https://cloud-api.yandex.net/v1/disk/resources'
            params = {
                'path': current_path,
                'fields': '_embedded.items.name,_embedded.items.type,_embedded.items.path, _embedded.items.modified'
                # Получаем имена файлов, тип и путь
            }
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 200:
                data = response.json()
                items = data.get('_embedded', {}).get('items', [])
                if not items:
                    logger.info(f'Папка пустая: {current_path}')  # Выводим путь к пустой папке
                for item in items:
                    if item['type'] == 'file':
                        logger.info(f'{item["type"]}, {item["path"]}, {item["modified"]}')  # Печатаем полный путь к файлу
                    elif item['type'] == 'dir':
                        # Добавляем папку в очередь для дальнейшего обхода
                        queue.append(item['path'])
            else:
                logger.error(f'Ошибка: {response.status_code}')
                logger.error(response.text)


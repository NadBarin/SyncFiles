import time
from datetime import datetime, timezone
import sys
import requests

import os
from loguru import logger

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
            logger.info(urlpath)
            logger.info(filepath)
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
                logger.info("Файл загружен на диск")
            except KeyError:
                logger.error(response_json)

    def _create_dirs(self, abspath):
        for root, dirs, files in os.walk(abspath):
            logger.info("Загружаем папки")
            relpath = os.path.relpath(root, self.base_folder)
            normalized_relpath = os.path.normpath(os.path.join(self.savedir, relpath))
            requests.put(
                f"https://cloud-api.yandex.net/v1/disk/resources?path={normalized_relpath.replace(f'\\', '/').replace('./', '')}",
                headers=self.headers,
            )
            return [relpath, normalized_relpath, files]

    def load(self, path):
        """Создаёт папку с заданым в savedir названием и если передаётся путь к папке
        то загружает все файлы и папки на диск.
        path: путь к папке или файлу, который нужно загрузить на диск"""
        requests.put(
            f"https://cloud-api.yandex.net/v1/disk/resources?path={self.savedir}",
            headers=self.headers,
        )
        abspath = os.path.join(self.base_folder, path)
        if os.path.isdir(abspath):
            relpath, normalized_relpath, files = self._create_dirs(abspath)
            for file in files:
                logger.info("Загружаем файлы")
                self._upload_file(urlpath=os.path.join(normalized_relpath, file),
                                  filepath=os.path.normpath(os.path.join(self.base_folder,relpath, file)))
        else:
            logger.info("Загружаем файл")
            dirname = os.path.dirname(abspath)
            self._create_dirs(dirname)
            self._upload_file(urlpath=os.path.join(self.savedir, path),
                              filepath=abspath)

    def reload(self, path):
        """сравнивает время на диске и время в системе и удаляет
        если время в системе больше чем на диске и снова загружает уже новый."""
        try:
            self.delete(path)
            self.load(path)
            logger.info("Файлы обновлены")
        except Exception as e:
            logger.error(f"Произошла ошибка: {e}")

    def delete(self, file_path):
        # if not file_path.startswith(os.sep):
        #    file_path = '/' + file_path
        file_path = os.path.join(self.savedir, file_path).replace(f'\\', '/')
        response = requests.delete(f"https://cloud-api.yandex.net/v1/disk/resources?path={file_path.replace('./', '')}",
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

    def _mod_time_os(self, path):
        stat = os.stat(path)
        modification_time = stat.st_mtime
        modification_time = datetime.fromtimestamp(modification_time, tz=timezone.utc)
        return modification_time

    def _find_all_files(self):
        in_folder_list = {}
        for root, dirs, files in os.walk(self.base_folder):
            for file in files:
                path = os.path.join(root, file)
                in_folder_list[os.path.relpath(path, self.base_folder)] = self._mod_time_os(path=path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                if not os.listdir(dir_path):  # Check if the directory is empty
                    in_folder_list[os.path.relpath(dir_path, self.base_folder)] = self._mod_time_os(path=path)
        return in_folder_list

    def _cloud_path(self, path):
        truncated_path = os.path.dirname(path)
        while os.path.basename(truncated_path) != self.savedir:
            truncated_path = os.path.dirname(truncated_path)
        result = os.path.relpath(path, truncated_path)
        return result

    def _find_all_cloud_files(self):
        queue = [self.savedir]
        result = {}
        while queue:
            current_path = queue.pop()  # Получаем следующий путь из очередиcurrent_path = queue.pop()  # Получаем следующий путь из очереди
            url = 'https://cloud-api.yandex.net/v1/disk/resources'
            params = {
                'path': current_path,
                'fields': '_embedded.items.name,_embedded.items.type,_embedded.items.path, _embedded.items.modified'
                # Получаем имена файлов, тип, путь, время изменения
            }
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                items = data.get('_embedded', {}).get('items', [])
                if not items:
                    result[self._cloud_path(current_path)] = datetime.fromisoformat(item["modified"]).astimezone(
                        timezone.utc)  # Выводим путь к пустой папке
                for item in items:
                    if item['type'] == 'file':
                        result[self._cloud_path(item["path"])] = datetime.fromisoformat(item["modified"]).astimezone(
                            timezone.utc)  # Печатаем полный путь к файлу
                    elif item['type'] == 'dir':
                        # Добавляем папку в очередь для дальнейшего обхода
                        queue.append(item['path'])
            else:
                logger.error(f'Ошибка: {response.status_code}')
                logger.error(response.text)
        return result

    def get_info(self):
        all_cloud = self._find_all_cloud_files()
        all_files = self._find_all_files()
        logger.info(all_files)
        logger.info(all_cloud)
        common_elements = set(all_cloud.keys()) & set(all_files.keys())

        for file_name in common_elements:
            cloud_element = all_cloud[file_name]
            files_element = all_files[file_name]
            if cloud_element < files_element:
                self.reload(cloud_element)
            del all_cloud[file_name]
            del all_files[file_name]

        if all_cloud:
            logger.info("Элементы в облаке не найдены в файлах")
            for element in all_cloud.keys():
                self.delete(element)
        if all_files:
            logger.info("элементы в файлах не найдены в облаке")
            for element in all_files.keys():
                self.load(element)

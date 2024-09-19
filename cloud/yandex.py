import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Dict

import requests
from loguru import logger


def _mod_time_os(path) -> datetime:
    """Возвращает время модификации файла.
    path: абсолютный путь к файлу."""
    try:
        stat = os.stat(path)
        modification_time = stat.st_mtime
        new_modification_time = datetime.fromtimestamp(
            modification_time, tz=timezone.utc
        )
        return new_modification_time
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


class YandexDiskUploader:
    def __init__(self, token, base_folder, savedir, logpath):
        self.token = token
        self.base_folder = base_folder
        self.savedir = savedir
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"OAuth {self.token}",
        }
        logger.add(
            os.path.join(logpath, "logs.log"),
            format="{time:YYYY-MM-DD HH:mm:ss!UTC} | {level} | {message}",
            rotation="1 day",
        )

    def _upload_file(self, urlpath, filepath) -> None:
        """Загружает файл в облако.
        urlpath: путь для облачного хранилища
        filepath: путь к файлу на компьютере"""
        try:

            backslash = "\\"
            api_url: str = (
                f"https://cloud-api.yandex.net/v1/disk"
                f"/resources/upload?path="
                f"{urlpath.replace(backslash, '/').replace('./', '')}"
            )
            response = requests.get(
                api_url,
                headers=self.headers,
            )
            response_json = response.json()

            with open(filepath.replace(".\\", ""), "rb") as f:
                try:
                    params = {"url": response_json["href"]}
                    response = requests.put(
                        response_json["href"],
                        params=params,
                        files={"file": f},
                    )
                    if response.status_code == 201:
                        logger.info(
                            f"Файл '{filepath}' загружен в облачное хранилище."
                        )
                    else:
                        logger.info(
                            f"Ошибка загрузки файла: "
                            f"{response.status_code} - {response.text}."
                        )
                except KeyError:
                    logger.error(response_json)
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())

    def _create_dirs(self, dirname) -> None:
        """Создаёт все папки которые нужно передать,
        кроме savedir - она создаётся ранее.
        dirname: имя или путь к папке."""
        try:
            path_components = []
            while dirname != self.base_folder:
                dirname, component = os.path.split(dirname)
                path_components.append(component)
            path_components.reverse()
            for component in path_components:
                relpath = os.path.join(
                    *path_components[: path_components.index(component) + 1]
                )
                norm_relpath = os.path.normpath(
                    os.path.join(self.savedir, relpath)
                )
                backslash = "\\"
                api_url = (
                    f"https://cloud-api.yandex.net/v1/disk/"
                    f"resources?path="
                    f"{norm_relpath.replace(backslash, '/').replace('./', '')}"
                )
                logger.info(f"Создаём папку: '{relpath}'.")
                response = requests.put(api_url, headers=self.headers)
                if response.status_code == 201:
                    logger.info(f"Папка '{relpath}' создана.")
                elif response.status_code == 409:
                    logger.info(
                        f"Папка '{relpath}' существует, не требуется создание."
                    )
                else:
                    logger.info(
                        f"Ошибка создания директории: "
                        f"{response.status_code} - {response.text}."
                    )
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())

    def load(self, path) -> None:
        """Создаёт папку с заданым в savedir названием и
        если передаётся путь к папке то загружает все файлы
        и папки на диск.
        path: путь к папке или файлу, который нужно загрузить на диск"""
        try:
            response = requests.put(
                f"https://cloud-api.yandex.net/v1/disk/resources"
                f"?path={self.savedir}",
                headers=self.headers,
            )
            if response.status_code == 201:
                logger.info(f"Папка '{self.savedir}' создана.")
            elif response.status_code == 409:
                logger.info(
                    f"Папка '{self.savedir}' "
                    f"существует, не требуется создание."
                )
            else:
                logger.info(
                    f"Ошибка создания директории: "
                    f"{response.status_code} - {response.text}"
                )

            abspath = os.path.join(self.base_folder, path)
            dirname = os.path.dirname(abspath)
            if os.path.isdir(abspath):
                self._create_dirs(abspath)
            else:
                self._create_dirs(dirname)
                logger.info(f"Загружаем файл '{path}'.")
                self._upload_file(
                    urlpath=os.path.join(self.savedir, path), filepath=abspath
                )
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())

    def reload(self, path) -> None:
        """Удаляет файл и загружает новый файл 
        взамен старого.
        path: относительный путь к файлу до base_folder"""
        try:
            self.delete(path)
            self.load(path)
            logger.info("Файлы обновлены.")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())

    def delete(self, path) -> None:
        """Удаляет файлы.
        path: относительный путь к файлу до base_folder.
        """
        try:
            path = os.path.join(self.savedir, path).replace("\\", "/")
            response = requests.delete(
                f"https://cloud-api.yandex.net/v1/disk/resources"
                f"?path={path.replace('./', '')}",
                headers=self.headers,
            )
            if response.status_code == 204:
                logger.info(f"Файл '{path}' успешно удален из Яндекс.Диска.")
            elif response.status_code == 404:
                logger.info(f"Файл '{path}' не найден.")
            elif response.status_code == 202:
                status_url = response.json().get("href")
                while True:
                    response = requests.get(status_url, headers=self.headers)
                    if response.status_code == 200:
                        status = response.json().get("status")
                        if status == "success":
                            logger.info(
                                f"Файл '{path}' "
                                f"успешно удален из Яндекс.Диска."
                            )
                            break
                        elif status == "failed":
                            logger.info("Операция завершилась с ошибкой.")
                            break
                    elif response.status_code == 202:
                        logger.info(
                            "Операция еще выполняется, "
                            "проверка через 5 секунд..."
                        )
                    else:
                        logger.error(
                            f"Ошибка при удалении файла: "
                            f"{response.status_code} - {response.text}"
                        )
                        break
                    time.sleep(2)  # Пауза перед следующей проверкой
            else:
                logger.error(
                    f"Ошибка при удалении файла: "
                    f"{response.status_code} - {response.text}"
                )
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())

    def _find_all_files(self) -> Dict:
        """Находит все файлы в указанном base_folder. Возвращает словарь с
        относительными путями к файлам (относительно base_folder)
        в качестве ключей и с временем изменений файла в качестве
        значения."""
        try:
            in_folder_list = {}
            for root, dirs, files in os.walk(self.base_folder):
                for file in files:
                    path = os.path.join(root, file)
                    in_folder_list[os.path.relpath(path, self.base_folder)] = (
                        _mod_time_os(path=path)
                    )
                for item in dirs:
                    dir_path = os.path.join(root, item)
                    if not os.listdir(
                        dir_path
                    ):  # Check if the directory is empty
                        in_folder_list[
                            os.path.relpath(dir_path, self.base_folder)
                        ] = _mod_time_os(path=dir_path)
            return in_folder_list
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())
            sys.exit(1)

    def _cloud_path(self, path) -> str:
        """Возвращает относительный путь к файлам в облаке
        (относительно savedir)."""
        try:
            truncated_path = os.path.dirname(path)
            while os.path.basename(truncated_path) != self.savedir:
                truncated_path = os.path.dirname(truncated_path)
            result = os.path.relpath(path, truncated_path)
            return result
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())
            sys.exit(1)

    def _find_all_cloud_files(self) -> Dict:
        """Находит все файлы в облаке в указанном savedir. Возвращает словарь с
        относительными путями к файлам (относительно savedir) в качестве ключей
        и с временем изменений файла в качестве значения."""
        try:
            queue = [self.savedir]
            result: dict[str, datetime | None] = {}
            while queue:
                current_path = (
                    queue.pop()
                )  # Получаем следующий путь из очереди
                url = "https://cloud-api.yandex.net/v1/disk/resources"
                params = {
                    "path": current_path,
                    "fields": "_embedded.items.name,_embedded.items.type,"
                    "_embedded.items.path, _embedded.items.modified,",
                    # Получаем имена файлов, тип, путь, время изменения
                }
                response = requests.get(
                    url, headers=self.headers, params=params
                )
                if response.status_code == 200:
                    data = response.json()
                    items = data.get("_embedded", {}).get("items", [])
                    if not items:
                        result[self._cloud_path(current_path)] = None
                    for item in items:
                        if item["type"] == "file":
                            result[
                                self._cloud_path(item["path"])
                            ] = datetime.fromisoformat(
                                item["modified"]
                            ).astimezone(
                                timezone.utc
                            )  # Печатаем полный путь к файлу
                        elif item["type"] == "dir":
                            # Добавляем папку в очередь для дальнейшего обхода
                            queue.append(item["path"])
                elif response.status_code == 404:
                    return {}
                else:
                    logger.error(f"Ошибка: {response.status_code}")
                    logger.error(response.text)
            return result
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())
            sys.exit(1)

    def get_info(self) -> None:
        """Создаёт словари с относительными путями файлов в качестве ключей
        и временем изменений в качестве значений для облака и системной папки.
        Если ключи совпадают, то проверяются значения чтобы изменения папки
        в облаке были позже чем на компьютере, если это не так то файл
        перезагружается на облако.
        Если остались ключи не совпадающие у словаря системной папки,
        то эти файлы загружаются на диск.
        Если остались ключи не совпадающие у словаря облака, то в облаке
        удаляются такие файлы(функция вызывается рекурсивно т.к. в delete
        удаляется только один файл), пока все несовпадающие файлы
        не будут удалены)."""

        try:
            all_cloud = self._find_all_cloud_files()
            all_files = self._find_all_files()

            common_elements = set(all_cloud.keys()) & set(all_files.keys())

            for file_name in common_elements:
                cloud_element = all_cloud[file_name]
                files_element = all_files[file_name]
                if cloud_element is not None and cloud_element < files_element:
                    self.reload(cloud_element)
                del all_cloud[file_name]
                del all_files[file_name]

            if all_files:
                logger.info("Элементы в файлах не найдены в облаке.")
                for element in all_files.keys():
                    self.load(f"{element}")

            if all_cloud:
                logger.info("Элементы в облаке не найдены в файлах.")
                for element in all_cloud.keys():
                    self.delete(f"{element}")
                    self.get_info()
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e.__class__.__name__}: {e}")
            logger.debug(traceback.format_exc())

import logging
import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Получение настроек из переменных окружения
YC_SA_KEY_ID = os.getenv('YC_SA_KEY_ID')
YC_SA_SECRET_KEY = os.getenv('YC_SA_SECRET_KEY')
YC_STORAGE_BUCKET = os.getenv('YC_STORAGE_BUCKET')
YC_ENDPOINT_URL = os.getenv('YC_ENDPOINT_URL', 'https://storage.yandexcloud.net')
LOCAL_FILE_PATH = os.getenv('CSV_FILE_PATH', '../scraper/apartments.csv')


def create_s3_session():
    """
    Создает и настраивает сессию для работы с Yandex Object Storage.
    """
    try:
        session = boto3.session.Session(
            aws_access_key_id=YC_SA_KEY_ID,
            aws_secret_access_key=YC_SA_SECRET_KEY,
            region_name="ru-central1"
        )
        s3_client = session.client(
            service_name='s3',
            endpoint_url=YC_ENDPOINT_URL
        )
        return s3_client
    except Exception as e:
        logging.error(f"Не удалось создать сессию S3: {e}")
        return None

def upload_file_to_s3(s3_client, file_path, bucket_name):
    """
    Загружает файл в Yandex Object Storage (S3).
    """
    # Проверяем, существуют ли учетные данные
    if not YC_SA_KEY_ID or not YC_SA_SECRET_KEY or not YC_STORAGE_BUCKET:
        logging.error("Учетные данные Yandex Cloud (ID, ключ, имя бакета) не найдены в .env файле.")
        logging.error("Пожалуйста, заполните .env файл и перезапустите скрипт.")
        return False
        
    # Проверяем, существует ли файл для загрузки
    if not os.path.exists(file_path):
        logging.error(f"Файл для загрузки не найден: {file_path}")
        return False

    # Создаем имя объекта в бакете. 
    # Хорошая практика - структурировать данные по датам.
    # Например: apartments/year=2023/month=12/day=25/apartments.csv
    today = datetime.utcnow()
    object_name = (f"apartments/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
                   f"{os.path.basename(file_path)}")

    try:
        logging.info(f"Начало загрузки файла {file_path} в бакет {bucket_name} как {object_name}...")
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info("Файл успешно загружен.")
        return True
    except ClientError as e:
        logging.error(f"Ошибка при загрузке файла в S3: {e}")
        return False
    except FileNotFoundError:
        logging.error(f"Локальный файл не найден: {file_path}")
        return False

def main():
    """
    Основная функция для запуска загрузчика.
    """
    s3_client = create_s3_session()
    if s3_client:
        upload_file_to_s3(s3_client, LOCAL_FILE_PATH, YC_STORAGE_BUCKET)

if __name__ == "__main__":
    main() 
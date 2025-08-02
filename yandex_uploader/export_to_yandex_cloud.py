import sys
import os
from datetime import datetime
from typing import Optional, List
from utils.dataframe_creator import create_offers_dataframe_with_districts, save_dataframe_to_csv
from yandex_uploader.uploader import create_s3_session, upload_file_to_s3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def upload_to_yandex_cloud(file_path: str, bucket_name: Optional[str] = None) -> bool:
    """
    Загружает файл в Яндекс Облако.
    
    Args:
        file_path: Путь к файлу для загрузки
        bucket_name: Имя бакета (если не указано, берется из переменных окружения)
        
    Returns:
        bool: True если загрузка успешна
    """
    try:
        # Создаем S3 сессию
        s3_client = create_s3_session()
        if not s3_client:
            logger.error("Не удалось создать S3 сессию")
            return False
        
        # Получаем имя бакета
        if bucket_name is None:
            bucket_name = os.getenv('YC_STORAGE_BUCKET')
            if not bucket_name:
                logger.error("Имя бакета не указано и не найдено в переменных окружения")
                return False
        
        # Создаем структуру папок для объявлений недвижимости
        today = datetime.utcnow()
        object_name = (f"apartments/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
                      f"{os.path.basename(file_path)}")
        
        logger.info(f"Загружаем файл в Яндекс Облако: {file_path} -> {object_name}")
        
        # Загружаем файл
        success = upload_file_to_s3(s3_client, file_path, bucket_name)
        
        if success:
            logger.info(f"Файл успешно загружен в бакет {bucket_name}")
        else:
            logger.error("Ошибка при загрузке файла")
        
        return success
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке в Яндекс Облако: {e}")
        return False


def export_offers_to_yandex_cloud(cassandra_hosts: List[str] = ['127.0.0.1'],
                                 limit: Optional[int] = None,
                                 bucket_name: Optional[str] = None,
                                 keep_local_file: bool = False) -> bool:
    """
    Полный цикл экспорта: Cassandra -> DataFrame -> CSV -> Яндекс Облако.
    
    Args:
        cassandra_hosts: Список хостов Cassandra
        limit: Ограничение количества записей
        bucket_name: Имя бакета в Яндекс Облаке
        keep_local_file: Оставить локальный CSV файл после загрузки
        
    Returns:
        bool: True если весь процесс успешен
    """
    logger.info("=== ЭКСПОРТ ДАННЫХ НЕДВИЖИМОСТИ В ЯНДЕКС ОБЛАКО ===")
    
    try:
        # Шаг 1: Создаем DataFrame из Cassandra
        logger.info("Создание DataFrame из Cassandra...")
        df = create_offers_dataframe_with_districts(cassandra_hosts, limit)
        
        if df is None or df.empty:
            logger.error("Не удалось получить данные из Cassandra")
            return False
        
        logger.info(f"Получено {len(df)} записей")
        
        # Шаг 2: Сохраняем в CSV
        logger.info("Сохранение в CSV файл...")
        csv_file = save_dataframe_to_csv(df)
        
        # Шаг 3: Загружаем в Яндекс Облако
        logger.info("Загрузка в Яндекс Облако...")
        upload_success = upload_to_yandex_cloud(csv_file, bucket_name)
        
        if not upload_success:
            logger.error("Ошибка при загрузке в Яндекс Облако")
            return False
        
        # Шаг 4: Удаляем локальный файл если не нужно сохранять
        if not keep_local_file:
            try:
                os.remove(csv_file)
                logger.info(f"Локальный файл удален: {csv_file}")
            except Exception as e:
                logger.warning(f"Не удалось удалить локальный файл: {e}")
        else:
            logger.info(f"Локальный файл сохранен: {csv_file}")
        
        logger.info("Экспорт завершен успешно!")
        return True
        
    except Exception as e:
        logger.error(f"Критическая ошибка при экспорте: {e}")
        return False


def main():
    """
    Главная функция для запуска экспорта.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Экспорт данных недвижимости из Cassandra в Яндекс Облако')
    parser.add_argument('--limit', type=int, help='Ограничение количества записей')
    parser.add_argument('--keep-file', action='store_true', help='Сохранить локальный CSV файл')
    parser.add_argument('--cassandra-host', default='127.0.0.1', help='Адрес Cassandra')
    parser.add_argument('--bucket', help='Имя бакета в Яндекс Облаке')

    
    args = parser.parse_args()
    
    # Запускаем экспорт
    success = export_offers_to_yandex_cloud(
        cassandra_hosts=[args.cassandra_host],
        limit=args.limit,
        bucket_name=args.bucket,
        keep_local_file=args.keep_file
    )
    
    if success:
        logger.info("Экспорт завершен успешно!")
    else:
        logger.error("Экспорт завершился с ошибками")
        sys.exit(1)


if __name__ == "__main__":
    main()
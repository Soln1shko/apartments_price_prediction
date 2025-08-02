import sys
import os
import pandas as pd
from typing import List, Optional
import logging
from db.cassandra_uploader import CassandraUploader
from utils.area_detector import get_ufa_district

logger = logging.getLogger(__name__)


def create_offers_dataframe_with_districts(cassandra_hosts: List[str] = ['127.0.0.1'],
                                         limit: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Создает DataFrame из таблицы offers в Cassandra с добавлением колонки district.
    
    Args:
        cassandra_hosts: Список хостов Cassandra
        limit: Ограничение количества записей (None = все записи)
        
    Returns:
        pd.DataFrame: DataFrame с данными объявлений и районами или None при ошибке
    """
    uploader = CassandraUploader(hosts=cassandra_hosts)
    
    try:
        logger.info("Подключаемся к Cassandra...")
        if not uploader.connect():
            logger.error("Не удалось подключиться к Cassandra")
            return None
        
        logger.info("Соединение с Cassandra установлено")
        
        # Формируем запрос
        if limit:
            query = f"SELECT * FROM {uploader.keyspace}.offers LIMIT {limit}"
        else:
            query = f"SELECT * FROM {uploader.keyspace}.offers"
        
        logger.info(f"Выполняем запрос: {query}")
        
        # Выполняем запрос
        rows = uploader.session.execute(query)
        
        # Преобразуем результат в список словарей
        data = []
        for row in rows:
            data.append({
                'id': str(row.id),
                'url': row.url,
                'address': row.address,
                'price_rub': row.price_rub,
                'total_area_sqm': row.total_area_sqm,
                'living_area_sqm': row.living_area_sqm,
                'kitchen_area_sqm': row.kitchen_area_sqm,
                'floor': row.floor,
                'floor_total': row.floor_total,
                'ceiling_height_m': row.ceiling_height_m,
                'year_built': row.year_built
            })
        
        if not data:
            logger.warning("В таблице offers нет данных")
            return None
        
        # Создаем DataFrame
        df = pd.DataFrame(data)
        
        logger.info(f"Создан DataFrame с {len(df)} записями")
        logger.info("Начинаем определение районов для каждого адреса...")
        
        # Добавляем колонку с районами
        df['district'] = df['address'].apply(_get_district_safe)
        
        logger.info("Определение районов завершено")
        logger.info(f"Колонки DataFrame: {list(df.columns)}")
        
        # Статистика по районам
        district_counts = df['district'].value_counts()
        logger.info(f"Статистика по районам:")
        for district, count in district_counts.head(10).items():
            logger.info(f"  {district}: {count} объявлений")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании DataFrame: {e}")
        return None
        
    finally:
        uploader.disconnect()


def _get_district_safe(address: str) -> str:
    """
    Безопасное получение района с обработкой ошибок.
    
    Args:
        address: Адрес для определения района
        
    Returns:
        str: Название района или сообщение об ошибке
    """
    if not address or pd.isna(address):
        return "Адрес не указан"
    
    try:
        # Логируем прогресс каждые 10 адресов
        if hasattr(_get_district_safe, 'counter'):
            _get_district_safe.counter += 1
        else:
            _get_district_safe.counter = 1
        
        if _get_district_safe.counter % 10 == 0:
            logger.info(f"Обработано адресов: {_get_district_safe.counter}")
        
        district = get_ufa_district(address)
        return district
        
    except Exception as e:
        logger.error(f"Ошибка при определении района для адреса '{address}': {e}")
        return f"Ошибка определения района: {str(e)}"


def add_districts_to_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет колонку с районами к существующему DataFrame.
    
    Args:
        df: DataFrame с колонкой 'address'
        
    Returns:
        pd.DataFrame: DataFrame с добавленной колонкой 'district'
    """
    if 'address' not in df.columns:
        logger.error("В DataFrame отсутствует колонка 'address'")
        raise ValueError("DataFrame должен содержать колонку 'address'")
    
    logger.info(f"Добавляем районы к DataFrame с {len(df)} записями")
    
    # Сбрасываем счетчик для логирования
    if hasattr(_get_district_safe, 'counter'):
        del _get_district_safe.counter
    
    # Добавляем колонку с районами
    df['district'] = df['address'].apply(_get_district_safe)
    
    logger.info("Добавление районов завершено")
    
    # Статистика по районам
    district_counts = df['district'].value_counts()
    logger.info("Статистика по районам:")
    for district, count in district_counts.head(10).items():
        logger.info(f"  {district}: {count} объявлений")
    
    return df


def save_dataframe_to_csv(df: pd.DataFrame, filename: Optional[str] = None) -> str:
    """
    Сохраняет DataFrame в CSV файл.
    
    Args:
        df: DataFrame для сохранения
        filename: Имя файла (если не указано, генерируется автоматически)
        
    Returns:
        str: Путь к сохраненному файлу
    """
    if filename is None:
        filename = "apartments.csv"
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"DataFrame сохранен в файл: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении CSV: {e}")
        raise

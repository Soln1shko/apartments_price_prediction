import asyncio
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
import uuid


class CassandraUploader:
    """
    Класс для работы с Cassandra базой данных.
    """
    
    def __init__(self, hosts: List[str] = ['127.0.0.1'], port: int = 9042,
                 keyspace: str = 'realty', username: str = None, password: str = None):
        """
        Инициализация подключения к Cassandra.
        
        Args:
            hosts: Список хостов Cassandra
            port: Порт подключения
            keyspace: Имя keyspace
            username: Имя пользователя (опционально)
            password: Пароль (опционально)
        """
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        
        # Настройка аутентификации если указаны credentials
        self.auth_provider = None
        if username and password:
            self.auth_provider = PlainTextAuthProvider(username=username, password=password)
    
    def connect(self) -> bool:
        """
        Устанавливает соединение с Cassandra.
        
        Returns:
            bool: True если соединение успешно, False иначе
        """
        try:
            print(f"Подключаемся к Cassandra: {self.hosts}:{self.port}")
            
            self.cluster = Cluster(
                self.hosts,
                port=self.port,
                auth_provider=self.auth_provider,
                load_balancing_policy=DCAwareRoundRobinPolicy()
            )
            
            self.session = self.cluster.connect()
            print("Соединение с Cassandra установлено успешно")
            
            # Создаем keyspace если не существует
            self._create_keyspace()
            
            # Переключаемся на наш keyspace
            self.session.set_keyspace(self.keyspace)
            
            # Создаем таблицы
            self._create_tables()
            
            return True
            
        except Exception as e:
            print(f"Ошибка подключения к Cassandra: {e}")
            return False
    
    def disconnect(self):
        """
        Закрывает соединение с Cassandra.
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        print("Соединение с Cassandra закрыто")
    
    def _create_keyspace(self):
        """
        Создает keyspace если он не существует.
        """
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """
        
        try:
            self.session.execute(create_keyspace_query)
            print(f"Keyspace '{self.keyspace}' создан или уже существует")
        except Exception as e:
            print(f"Ошибка создания keyspace: {e}")
            raise
    
    def _create_tables(self):
        """
        Создает необходимые таблицы.
        """
        
        # Таблица для объявлений недвижимости
        create_offers_table = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace}.offers (
            id UUID PRIMARY KEY,
            url TEXT,
            address TEXT,
            price_rub BIGINT,
            total_area_sqm DOUBLE,
            living_area_sqm DOUBLE,
            kitchen_area_sqm DOUBLE,
            floor INT,
            floor_total INT,
            ceiling_height_m DOUBLE,
            year_built INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
        
        # Таблица для истории парсинга
        create_parse_history_table = f"""
        CREATE TABLE IF NOT EXISTS {self.keyspace}.parse_history (
            id UUID PRIMARY KEY,
            source_url TEXT,
            parsed_at TIMESTAMP,
            total_offers INT,
            successful_offers INT,
            failed_offers INT,
            status TEXT
        )
        """
        
        try:
            # Создаем таблицы если не существуют
            self.session.execute(create_offers_table)
            self.session.execute(create_parse_history_table)
            print("Таблицы созданы успешно")
        except Exception as e:
            print(f"Ошибка создания таблиц: {e}")
            raise
    
    def insert_offer(self, offer_data: Dict[str, Any]) -> bool:
        """
        Вставляет одно объявление в базу данных.
        
        Args:
            offer_data: Данные объявления
            
        Returns:
            bool: True если вставка успешна, False иначе
        """
        insert_query = f"""
        INSERT INTO {self.keyspace}.offers (
            id, url, address, price_rub, total_area_sqm, living_area_sqm, kitchen_area_sqm,
            floor, floor_total, ceiling_height_m, year_built, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            current_time = datetime.now()
            
            self.session.execute(insert_query, [
                uuid.uuid4(),  # id
                offer_data.get('url'),
                offer_data.get('address'),
                offer_data.get('price_rub'),
                offer_data.get('total_area_sqm'),
                offer_data.get('living_area_sqm'),
                offer_data.get('kitchen_area_sqm'),
                offer_data.get('floor'),
                offer_data.get('floor_total'),
                offer_data.get('ceiling_height_m'),
                offer_data.get('year_built'),
                current_time,  # created_at
                current_time   # updated_at
            ])
            
            return True
            
        except Exception as e:
            print(f"Ошибка вставки объявления {offer_data.get('url', 'N/A')}: {e}")
            return False
    
    def insert_offers_batch(self, offers_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Вставляет множество объявлений в базу данных.
        
        Args:
            offers_data: Список данных объявлений
            
        Returns:
            Dict[str, int]: Статистика вставки (successful, failed)
        """
        successful = 0
        failed = 0
        
        print(f"Начинаем загрузку {len(offers_data)} объявлений в Cassandra...")
        
        for i, offer in enumerate(offers_data, 1):
            if self.insert_offer(offer):
                successful += 1
            else:
                failed += 1
            
            # Показываем прогресс каждые 10 записей
            if i % 10 == 0 or i == len(offers_data):
                print(f"Обработано: {i}/{len(offers_data)} "
                      f"(успешно: {successful}, ошибок: {failed})")
        
        return {
            'successful': successful,
            'failed': failed,
            'total': len(offers_data)
        }
    
    def insert_parse_history(self, source_url: str, total_offers: int, 
                           successful_offers: int, failed_offers: int, 
                           status: str = 'completed') -> bool:
        """
        Записывает историю парсинга.
        
        Args:
            source_url: URL источника
            total_offers: Общее количество объявлений
            successful_offers: Успешно загруженных
            failed_offers: С ошибками
            status: Статус операции
            
        Returns:
            bool: True если запись успешна
        """
        insert_query = f"""
        INSERT INTO {self.keyspace}.parse_history (
            id, source_url, parsed_at, total_offers, successful_offers, failed_offers, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            self.session.execute(insert_query, [
                uuid.uuid4(),
                source_url,
                datetime.now(),
                total_offers,
                successful_offers,
                failed_offers,
                status
            ])
            
            print(f"История парсинга записана: {successful_offers}/{total_offers} успешно")
            return True
            
        except Exception as e:
            print(f"Ошибка записи истории парсинга: {e}")
            return False
    
    def get_offers_count(self) -> int:
        """
        Возвращает общее количество объявлений в базе.
        
        Returns:
            int: Количество объявлений
        """
        try:
            result = self.session.execute(f"SELECT COUNT(*) FROM {self.keyspace}.offers")
            return result.one()[0]
        except Exception as e:
            print(f"Ошибка получения количества объявлений: {e}")
            return 0
    
    def upload_from_json(self, json_file_path: str) -> bool:
        """
        Загружает данные из JSON файла в Cassandra.
        
        Args:
            json_file_path: Путь к JSON файлу
            
        Returns:
            bool: True если загрузка успешна
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            offers = data.get('offers', [])
            source_url = data.get('source_url', 'unknown')
            
            if not offers:
                print("В JSON файле нет данных объявлений")
                return False
            
            # Загружаем объявления
            stats = self.insert_offers_batch(offers)
            
            # Записываем историю
            self.insert_parse_history(
                source_url=source_url,
                total_offers=stats['total'],
                successful_offers=stats['successful'],
                failed_offers=stats['failed']
            )
            
            print(f"Загрузка завершена: {stats['successful']}/{stats['total']} успешно")
            return stats['successful'] > 0
            
        except Exception as e:
            print(f"Ошибка загрузки из JSON файла {json_file_path}: {e}")
            return False


async def main():
    """
    Пример использования CassandraUploader.
    """
    uploader = CassandraUploader()
    
    try:
        # Подключаемся к Cassandra
        if not uploader.connect():
            print("Не удалось подключиться к Cassandra")
            return
        
        # Показываем текущее количество записей
        current_count = uploader.get_offers_count()
        print(f"Текущее количество объявлений в базе: {current_count}")
        
        # Ищем JSON файлы для загрузки
        import os
        json_files = [f for f in os.listdir('.') if f.startswith('parsed_offers_') and f.endswith('.json')]
        
        if json_files:
            latest_file = max(json_files, key=os.path.getctime)
            print(f"Найден файл для загрузки: {latest_file}")
            
            success = uploader.upload_from_json(latest_file)
            if success:
                new_count = uploader.get_offers_count()
                print(f"Загрузка завершена. Новое количество записей: {new_count}")
            else:
                print("Ошибка при загрузке данных")
        else:
            print("JSON файлы с данными не найдены")
    
    except Exception as e:
        print(f"Ошибка в main: {e}")
    
    finally:
        uploader.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
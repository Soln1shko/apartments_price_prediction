import asyncio
import json
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
from url_parser import scrape_yandex_realty
from card_parser import scrape_offers_details
from db.cassandra_uploader import CassandraUploader


class YandexRealtyParser:
    """
    Главный класс для парсинга недвижимости с автоматической выгрузкой в Cassandra.
    """
    
    def __init__(self, base_url: str = "https://realty.yandex.ru/ufa/kupit/kvartira/", 
                 upload_to_cassandra: bool = True, cassandra_hosts: List[str] = ['127.0.0.1']):
        self.base_url = base_url
        self.links = []
        self.offers_data = []
        self.upload_to_cassandra = upload_to_cassandra
        self.cassandra_uploader = None
        
        # Инициализируем Cassandra uploader если нужно
        if self.upload_to_cassandra:
            self.cassandra_uploader = CassandraUploader(hosts=cassandra_hosts)
    
    async def parse_links(self) -> List[str]:
        """
        Парсит ссылки на объявления с помощью url_parser.
        
        Returns:
            List[str]: Список ссылок на объявления
        """
        print(f"Начинаем парсинг ссылок с URL: {self.base_url}")
        
        try:
            self.links = await scrape_yandex_realty(self.base_url)
            print(f"Успешно получено {len(self.links)} ссылок")
            return self.links
        except Exception as e:
            print(f"Ошибка при парсинге ссылок: {e}")
            return []
    
    async def parse_offers(self, links: List[str] = None) -> List[Dict[str, Any]]:
        """
        Парсит данные объявлений с помощью card_parser.
        
        Args:
            links: Список ссылок для парсинга. Если не указан, использует self.links
            
        Returns:
            List[Dict[str, Any]]: Список данных объявлений
        """
        if links is None:
            links = self.links
        
        if not links:
            print("Нет ссылок для парсинга объявлений")
            return []
        
        print(f"Начинаем парсинг {len(links)} объявлений")
        
        try:
            self.offers_data = await scrape_offers_details(links)
            print(f"Успешно спарсено {len(self.offers_data)} объявлений")
            return self.offers_data
        except Exception as e:
            print(f"Ошибка при парсинге объявлений: {e}")
            return []
    
    async def full_parse(self) -> List[Dict[str, Any]]:
        """
        Выполняет полный цикл парсинга: сначала ссылки, потом объявления.
        Автоматически выгружает данные в Cassandra если включено.
        
        Returns:
            List[Dict[str, Any]]: Список данных объявлений
        """
        print("=== Начинаем полный парсинг ===")
        
        # Подключаемся к Cassandra если нужно
        if self.upload_to_cassandra and self.cassandra_uploader:
            if not self.cassandra_uploader.connect():
                print("Предупреждение: не удалось подключиться к Cassandra. Продолжаем без выгрузки в БД.")
                self.upload_to_cassandra = False
        
        try:
            # Парсим ссылки
            links = await self.parse_links()
            
            if not links:
                print("Не удалось получить ссылки. Завершаем работу.")
                return []
            
            # Парсим объявления
            offers = await self.parse_offers(links)
            
            # Сохраняем результат в JSON файл
            json_file = await self.save_to_json(offers)
            
            # Выгружаем в Cassandra если включено
            if self.upload_to_cassandra and self.cassandra_uploader and offers:
                await self.upload_to_cassandra_db(offers)
            
            print("=== Полный парсинг завершен ===")
            return offers
            
        finally:
            # Закрываем соединение с Cassandra
            if self.cassandra_uploader:
                self.cassandra_uploader.disconnect()
    
    async def save_to_json(self, data: List[Dict[str, Any]], filename: str = None) -> str:
        """
        Сохраняет данные в JSON файл.
        
        Args:
            data: Данные для сохранения
            filename: Имя файла. Если не указано, генерируется автоматически
            
        Returns:
            str: Путь к сохраненному файлу
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"parsed_offers_{timestamp}.json"
        
        output_data = {
            'parsed_at': datetime.now().isoformat(),
            'source_url': self.base_url,
            'total_offers': len(data),
            'offers': data
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f"Данные сохранены в файл: {filename}")
            return filename
        except Exception as e:
            print(f"Ошибка при сохранении в файл {filename}: {e}")
            return ""
    
    async def upload_to_cassandra_db(self, offers: List[Dict[str, Any]]) -> bool:
        """
        Выгружает данные объявлений в Cassandra.
        
        Args:
            offers: Список данных объявлений
            
        Returns:
            bool: True если выгрузка успешна
        """
        if not self.cassandra_uploader:
            print("Cassandra uploader не инициализирован")
            return False
        
        print(f"=== Начинаем выгрузку {len(offers)} объявлений в Cassandra ===")
        
        try:
            # Выгружаем объявления
            stats = self.cassandra_uploader.insert_offers_batch(offers)
            
            # Записываем историю парсинга
            self.cassandra_uploader.insert_parse_history(
                source_url=self.base_url,
                total_offers=stats['total'],
                successful_offers=stats['successful'],
                failed_offers=stats['failed'],
                status='completed' if stats['successful'] > 0 else 'failed'
            )
            
            print(f"=== Выгрузка в Cassandra завершена ===")
            print(f"Успешно: {stats['successful']}/{stats['total']}")
            
            if stats['failed'] > 0:
                print(f"Ошибок: {stats['failed']}")
            
            return stats['successful'] > 0
            
        except Exception as e:
            print(f"Ошибка при выгрузке в Cassandra: {e}")
            return False


async def main():
    """
    Главная функция для запуска парсинга с автоматической выгрузкой в Cassandra.
    """
    print("=== ПАРСЕР НЕДВИЖИМОСТИ ЯНДЕКС ===")
    print("Автоматически выгружает данные в Cassandra (127.0.0.1:9042)")
    print()
    
    # Создаем экземпляр парсера с включенной выгрузкой в Cassandra
    parser = YandexRealtyParser(
        base_url="https://realty.yandex.ru/ufa/kupit/kvartira/",
        upload_to_cassandra=True,
        cassandra_hosts=['127.0.0.1']
    )
    
    try:
        # Выполняем полный парсинг с автоматической выгрузкой в Cassandra
        offers = await parser.full_parse()
        
        if offers:
            print(f"\n=== ИТОГОВЫЙ РЕЗУЛЬТАТ ===")
            print(f"Всего спарсено объявлений: {len(offers)}")
            print(f"Данные сохранены в JSON файл и выгружены в Cassandra")
        else:
            print("Не удалось получить данные объявлений")
    
    except Exception as e:
        print(f"Критическая ошибка в main: {e}")


# Дополнительная функция для парсинга без выгрузки в Cassandra
async def parse_only_json():
    """
    Функция для парсинга только в JSON без выгрузки в Cassandra.
    """
    parser = YandexRealtyParser(upload_to_cassandra=False)
    return await parser.full_parse()


if __name__ == "__main__":
    asyncio.run(main())
import requests
import os
from dotenv import load_dotenv

load_dotenv()

YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')

def get_ufa_district(address_str: str) -> str:
    """
    Принимает строку с адресом в Уфе и возвращает административный район.
    Использует двухэтапный запрос к API Геокодера Яндекса для надежности.

    :param address_str: Адрес, например, "улица Цюрупы, 40"
    :return: Название района или сообщение об ошибке.
    """
    full_address = f"Уфа, {address_str}"
    
    # Получаем координаты по адресу
    params_for_coords = {
        'apikey': YANDEX_API_KEY,
        'geocode': full_address,
        'format': 'json',
        'results': 1
    }

    try:
        # Первый запрос: адрес -> координаты
        response_coords = requests.get('https://geocode-maps.yandex.ru/1.x/', params=params_for_coords)
        response_coords.raise_for_status()
        data_coords = response_coords.json()

        feature_members = data_coords['response']['GeoObjectCollection']['featureMember']
        if not feature_members:
            return f"Адрес не найден: {address_str}"

        # Извлекаем координаты. Ответ приходит в формате "долгота широта"
        point_str = feature_members[0]['GeoObject']['Point']['pos']
        # Для следующего запроса меняем пробел на запятую
        coords_for_request = point_str.replace(' ', ',')

        # Получаем район по координатам
        params_for_district = {
            'apikey': YANDEX_API_KEY,
            'geocode': coords_for_request,
            'format': 'json',
            'kind': 'district',  
            'results': 1,
            'lang': 'ru_RU'
        }
        
        # Второй запрос: координаты -> район
        response_district = requests.get('https://geocode-maps.yandex.ru/1.x/', params=params_for_district)
        response_district.raise_for_status()
        data_district = response_district.json()
        
        district_feature_members = data_district['response']['GeoObjectCollection']['featureMember']
        if not district_feature_members:
            return "Не удалось определить район по координатам (нет в базе Яндекса)"
        
        # Извлекаем имя найденного района
        district_name = district_feature_members[0]['GeoObject']['name']
        return district_name

    except requests.exceptions.RequestException as e:
        return f"Ошибка сети при обращении к API: {e}"
    except (KeyError, IndexError):
        return "Не удалось разобрать ответ от API Яндекса. Неожиданная структура."
    except Exception as e:
        return f"Произошла непредвиденная ошибка: {e}"

if __name__ == "__main__":
    print(get_ufa_district("улица Цюрупы, 40"))
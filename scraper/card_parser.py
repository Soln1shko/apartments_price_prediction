import asyncio
import re
from typing import List, Dict, Any
from playwright.async_api import async_playwright
import random

def parse_price(text: str) -> int:
    """
    Преобразует строку с ценой в целое число.
    Пример: "7 750 000 ₽" -> 7750000
    """
    if not text:
        return None
    clean_text = re.sub(r'[^\d]', '', text)
    return int(clean_text) if clean_text.isdigit() else None

def parse_float_value(text: str) -> float:
    """
    Преобразует строку с числом, возможна запятая.
    Пример: "12,4 м²" -> 12.4
    """
    if not text:
        return None
    clean_text = text.split()[0].replace(',', '.')
    try:
        return float(clean_text)
    except ValueError:
        return None

def parse_int_value(text: str) -> int:
    """
    Преобразует строку с целым числом.
    Пример: "10 этаж" -> 10
    """
    if not text:
        return None
    numbers = re.findall(r'\d+', text)
    if numbers:
        return int(numbers[0])
    return None

def parse_address(text: str) -> str:
    """
    Извлекает адрес из полной строки адреса.
    Пример: "Уфа, улица Цюрупы, 151" -> "улица Цюрупы, 151"
    """
    if not text:
        return None
    
    # Убираем "Уфа, " в начале
    if text.startswith('Уфа, '):
        return text[5:]  # Убираем "Уфа, "
    
    return text

async def scrape_offer_details(page, url: str) -> Dict[str, Any]:
    """
    Парсит страницу карточки квартиры и возвращает структурированные данные.
    """
    data = {
        'url': url,
        'address': None,
        'price_rub': None,
        'total_area_sqm': None,
        'living_area_sqm': None,
        'kitchen_area_sqm': None,
        'floor': None,
        'floor_total': None,
        'ceiling_height_m': None,
        'year_built': None
    }

    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        # Даем время на подгрузку JS-данных и рендеринг
        await asyncio.sleep(2)

        # Парсим цену
        price_elem = await page.query_selector('span.OfferCardSummaryInfo__price--2FD3C')
        if price_elem:
            price_text = await price_elem.inner_text()
            data['price_rub'] = parse_price(price_text)

        # Парсим адрес
        address_elem = await page.query_selector('div.CardLocation__addressItem--1JYpZ')
        if address_elem:
            address_text = await address_elem.inner_text()
            data['address'] = parse_address(address_text)

        # Парсим технические характеристики
        # Каждый блок с классом OfferCardHighlight__container--2gZn2 содержит пару: значение и метка
        feature_containers = await page.query_selector_all('div.OfferCardHighlight__container--2gZn2')

        for container in feature_containers:
            value_elem = await container.query_selector('div.OfferCardHighlight__value--HMVgP')
            label_elem = await container.query_selector('div.OfferCardHighlight__label--2uMCy')

            if not value_elem or not label_elem:
                continue

            value_text = await value_elem.inner_text()
            label_text = await label_elem.inner_text()
            label_text = label_text.strip().lower()

            if label_text == 'общая':
                data['total_area_sqm'] = parse_float_value(value_text)
            elif label_text == 'жилая':
                data['living_area_sqm'] = parse_float_value(value_text)
            elif label_text == 'кухня':
                data['kitchen_area_sqm'] = parse_float_value(value_text)
            elif label_text == 'этаж':
                data['floor'] = parse_int_value(value_text)
            elif label_text == 'из 10' or label_text == 'этажей' or label_text.startswith('из'):
                # иногда указано "из XX"
                data['floor_total'] = parse_int_value(value_text)
            elif label_text == 'потолки':
                data['ceiling_height_m'] = parse_float_value(value_text)
            elif label_text == 'год постройки':
                data['year_built'] = parse_int_value(value_text)

    except Exception as e:
        print(f"Ошибка при парсинге {url}: {e}")

    return data

async def scrape_offers_details(links: List[str]) -> List[Dict[str, Any]]:
    """
    Главная функция, принимает массив ссылок,
    парсит каждую страницу квартиры и возвращает массив данных.
    """
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        for i, link in enumerate(links, 1):
            print(f"Парсим ({i}/{len(links)}): {link}")
            offer_data = await scrape_offer_details(page, link)
            results.append(offer_data)
            # Пауза, чтобы не спамить сайт
            await asyncio.sleep(2 + (random.random() * 2))

        await context.close()
        await browser.close()
    return results


if __name__ == "__main__":
    links = ["https://realty.yandex.ru/offer/8750304431505540864/"]
    results = asyncio.run(scrape_offers_details(links))
    print(results)


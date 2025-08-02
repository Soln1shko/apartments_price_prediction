import asyncio
import random
import json
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

async def handle_captcha_if_present(page):
    """
    Проверяет наличие капчи и ждет, пока пользователь ее решит.
    """
    try:
        captcha_frame_selector = 'iframe[src*="captcha"]'
        await page.wait_for_selector(captcha_frame_selector, timeout=5000)
        
        print("Обнаружена капча!")

        
        await page.wait_for_selector(captcha_frame_selector, state='hidden', timeout=0)
        
        print("Капча решена. Продолжаем работу...")
        await asyncio.sleep(random.uniform(2, 3))
        
    except Exception:
        # Капча не найдена, это нормальное поведение
        pass

async def scrape_yandex_realty(base_url: str) -> list[str]:
    """
    Функция-обертка для запуска браузера и рекурсивного парсера.

    Args:
        base_url (str): Стартовый URL для поиска.

    Returns:
        list[str]: Финальный список всех уникальных ссылок.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = await browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            locale='ru-RU'
        )
        page = await context.new_page()
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Вызываем рекурсивную функцию с начальными параметрами:
        # - page: созданный объект страницы
        # - url: стартовый URL
        # - collected_links: пустой set для сбора ссылок
        final_links_list = await _recursive_scrape_page(page, base_url, set())

        print(f"\n\nПарсинг завершен. Всего найдено {len(final_links_list)} уникальных ссылок.")
        
        if final_links_list:
            data = {
                'source_url': base_url,
                'scraped_at': str(asyncio.get_event_loop().time()),
                'links_count': len(final_links_list),
                'links': final_links_list
            }
            with open('yandex_realty_links_full.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Данные сохранены в файл: yandex_realty_links_full.json")

        await context.close()
        await browser.close()
        
        return final_links_list

async def _recursive_scrape_page(page, current_url: str, collected_links: set) -> list[str]:
    """
    Рекурсивная функция, которая парсит одну страницу, добавляет ссылки в массив
    и вызывает саму себя для следующей страницы.
    """
    # Определяем номер страницы из URL для логирования
    parsed_q = parse_qs(urlparse(current_url).query)
    page_param = int(parsed_q.get('page', [1])[0])  # По умолчанию 1, а не 0
    page_number_display = page_param
    
    print(f"\n--- Обрабатываем страницу {page_number_display}: {current_url} ---")
    
    try:
        await page.goto(current_url, wait_until='domcontentloaded', timeout=60000)
        await handle_captcha_if_present(page)

        await page.wait_for_selector('a[href*="/offer/"]', timeout=20000)
    except Exception:
        print("Не удалось найти карточки на странице. Вероятно, это конец списка.")
        return list(collected_links) # Базовый случай рекурсии: возвращаем то, что собрали.

    print("Скроллим страницу...")
    for _ in range(3):
        await page.mouse.wheel(0, 1000)
        await asyncio.sleep(random.uniform(0.8, 1.5))
        
    links_before_scrape = len(collected_links)
    
    links_on_page_elements = await page.query_selector_all('a[href*="/offer/"]')
    for element in links_on_page_elements:
        href = await element.get_attribute('href')
        if href and href.startswith('/offer/'):
            full_url = urljoin(current_url, href)
            collected_links.add(full_url)
            
    found_on_this_page = len(collected_links) - links_before_scrape
    print(f"Найдено {found_on_this_page} новых уникальных ссылок. Всего собрано: {len(collected_links)}")

    # Выход из рекурсии: 
    if found_on_this_page == 0 and page_number_display > 1:
        print("На странице не найдено новых ссылок. Завершаем парсинг.")
        return list(collected_links)
    
    if page_number_display >= 25:
        print("Достигнута максимальная страница 25. Завершаем парсинг.")
        return list(collected_links)

    # Готовим URL для следующей страницы
    if 'page' not in parse_qs(urlparse(current_url).query):
        next_page_num_param = 2
    else:
        next_page_num_param = page_param + 1
    
    # Корректно добавляем или обновляем параметр 'page' в URL
    parsed_url = urlparse(current_url)
    query_params = parse_qs(parsed_url.query)
    query_params['page'] = [str(next_page_num_param)]
    new_query = urlencode(query_params, doseq=True)
    next_page_url = urlunparse(parsed_url._replace(query=new_query))
    
    # Пауза перед переходом
    await asyncio.sleep(random.uniform(2.5, 4.5))

    # Вызываем эту же функцию для следующей страницы, передавая обновленный набор ссылок
    return await _recursive_scrape_page(page, next_page_url, collected_links)


async def main():
    search_url = "https://realty.yandex.ru/ufa/kupit/kvartira/"
    
    try:
        links = await scrape_yandex_realty(search_url)
        if links:
            print(f"\nПример полученных ссылок ({min(5, len(links))} из {len(links)}):")
            for i, link in enumerate(links[:5], 1):
                print(f"{i}. {link}")
    except Exception as e:
        print(f"Ошибка в main: {e}")

if __name__ == "__main__":
    asyncio.run(main())

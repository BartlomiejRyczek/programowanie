import asyncio
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from concurrent.futures import ThreadPoolExecutor

# Konfiguracja loggera
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Abstrakcyjna klasa bazowa dla scraperów"""
    
    def __init__(self, urls: List[str], max_workers: int = 3):
        self.urls = urls if isinstance(urls, list) else [urls]
        self.data: List[Dict] = []
        self.max_workers = max_workers

    @abstractmethod
    def parse_page(self, html: str) -> List[Dict]:
        """Metoda do implementacji przez klasy pochodne"""
        pass

    def get_driver(self) -> webdriver.Chrome:
        """Tworzy i konfiguruje webdriver Chrome"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Uruchamianie w trybie headless
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        return webdriver.Chrome(options=chrome_options)

    def fetch_page(self, url: str) -> Optional[str]:
        """Pobiera stronę używając Selenium"""
        driver = None
        try:
            driver = self.get_driver()
            driver.get(url)
            
            # Przewijanie strony
            body = driver.find_element(By.TAG_NAME, "body")
            for _ in range(3):  # Przewiń kilka razy
                body.send_keys(Keys.PAGE_DOWN)
                time.sleep(0.5)

            return driver.page_source
        except Exception as e:
            logger.error(f"Error fetching URL {url}: {e}")
            return None
        finally:
            if driver:
                driver.quit()

    async def process_url(self, url: str) -> None:
        """Przetwarza pojedynczy URL"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            html = await loop.run_in_executor(executor, self.fetch_page, url)
            if html:
                page_data = self.parse_page(html)
                self.data.extend(page_data)
                logger.info(f"Processed {url}, found {len(page_data)} listings")

    async def process_urls(self) -> None:
        """Przetwarza wszystkie URLs współbieżnie"""
        tasks = []
        sem = asyncio.Semaphore(self.max_workers)
        
        async def process_with_semaphore(url):
            async with sem:
                await self.process_url(url)
        
        for url in self.urls:
            task = asyncio.create_task(process_with_semaphore(url))
            tasks.append(task)
        
        await asyncio.gather(*tasks)

    def save_to_csv(self, filename: str) -> None:
        """Zapisuje zebrane dane do pliku CSV"""
        if not self.data:
            logger.warning("No data to save")
            return

        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.data[0].keys())
                writer.writeheader()
                writer.writerows(self.data)
            logger.info(f"Data saved to {filename}")
        except IOError as e:
            logger.error(f"Error saving to CSV: {e}")

class PracujPlScraper(BaseScraper):
    """Scraper specyficzny dla pracuj.pl"""

    @staticmethod
    def convert_to_iso(date_str: str) -> str:
        """Konwertuje polską datę na format ISO"""
        polish_months = {
            "stycznia": "01", "lutego": "02", "marca": "03", "kwietnia": "04",
            "maja": "05", "czerwca": "06", "lipca": "07", "sierpnia": "08",
            "września": "09", "października": "10", "listopada": "11", "grudnia": "12"
        }
        try:
            if not date_str.strip():
                return "Brak daty"
            day, month_polish, year = date_str.replace('Opublikowana:', '').strip().split()
            month = polish_months[month_polish]
            return f"{year}-{month}-{day.zfill(2)}"
        except Exception:
            return "Brak daty"

    @staticmethod
    def clean_salary(salary: str) -> str:
        """Czyści tekst wynagrodzenia"""
        return salary.replace(' zł / mies. (zal. od umowy)', '').strip()

    def parse_page(self, html: str) -> List[Dict]:
        """Parsuje stronę pracuj.pl"""
        soup = BeautifulSoup(html, 'html.parser')
        offers = soup.find_all('div', class_='gp-pp-reset tiles_b18pwp01 core_po9665q')
        page_data = []

        for offer in offers:
            title_tag = offer.find('a', class_='tiles_o1859gd9 core_n194fgoq')
            company_tag = offer.find('h3', class_='tiles_chl8gsf size-caption core_t1rst47b')
            date_tag = offer.find('p', class_='tiles_a1nm2ekh tiles_s1pgzmte tiles_bg8mbli core_pk4iags size-caption core_t1rst47b')
            salary_tag = offer.find('span', class_='tiles_s1x1fda3')
            tech_tags = offer.find_all('span', class_='_chip_hmm6b_1 _chip--highlight_hmm6b_1 _chip--small_hmm6b_1 _chip--full-corner_hmm6b_1 tiles_c276mrm')
            link_tag = offer.find('a', class_='tiles_cnb3rfy core_n194fgoq')

            job_data = {
                'Tytuł': title_tag.get_text(strip=True) if title_tag else 'Brak tytułu',
                'Firma': company_tag.get_text(strip=True) if company_tag else 'Brak nazwy firmy',
                'Data_opublikowania': self.convert_to_iso(date_tag.get_text() if date_tag else ''),
                'Wynagrodzenie': self.clean_salary(salary_tag.get_text() if salary_tag else 'Brak wynagrodzenia'),
                'Technologie': [tag.get_text(strip=True) for tag in tech_tags] if tech_tags else 'Brak technologii',
                'Link': link_tag['href'] if link_tag else 'Brak linku'
            }
            page_data.append(job_data)

        return page_data

async def main():
    urls = [
        "https://it.pracuj.pl/praca?et=1%2C3%2C17&itth=37",  # Python jobs
        "https://it.pracuj.pl/praca?et=1%2C3%2C17&itth=33",  # JavaScript jobs
        "https://it.pracuj.pl/praca?et=1%2C3%2C17&itth=41"   # C++ jobs
    ]
    
    scraper = PracujPlScraper(urls)
    await scraper.process_urls()
    scraper.save_to_csv("job_listings.csv")

if __name__ == "__main__":
    asyncio.run(main())
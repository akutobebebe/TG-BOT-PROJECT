import requests
import time
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Tuple, Set

# --- ⚙️ ЗАВАНТАЖЕННЯ КОНФІГУРАЦІЇ ---

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get('UPBIT_TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID_STRING = os.environ.get('UPBIT_TELEGRAM_CHAT_ID')

# Перевірка наявності токенів
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID_STRING:
    print("Помилка: Не вдалося знайти 'UPBIT_TELEGRAM_TOKEN' або 'UPBIT_TELEGRAM_CHAT_ID'.")
    print("Будь ласка, переконайтеся, що файл .env існує в тій самій папці, що й скрипт,")
    print("і що він містить коректні значення.")
    sys.exit(1)

# Розбиваємо рядок з ID на список
try:
    TELEGRAM_CHAT_IDS = [chat_id.strip() for chat_id in TELEGRAM_CHAT_ID_STRING.split(',')]
    if not all(TELEGRAM_CHAT_IDS):
        raise ValueError("Один з Chat ID порожній")
    print(f"Знайдено {len(TELEGRAM_CHAT_IDS)} отримувачів (Chat ID).")
except Exception as e:
    print(f"Помилка парсингу TELEGRAM_CHAT_ID: {e}")
    print("Переконайтеся, що ID вказані через кому, без пробілів (напр. 123,456)")
    sys.exit(1)


# --- 📜 Глобальні налаштування скрипта ---

UPBIT_API_URL = "https://api.upbit.com/v1/market/all"
CHECK_INTERVAL_SECONDS = 1 / 3  # ~0.333 секунди (3 запити на секунду)
REQUEST_TIMEOUT = 5
MAX_RETRIES = 3  # Максимальна кількість повторних спроб при помилці
RETRY_DELAY = 2  # Затримка між повторними спробами (секунди)

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- Налаштування сесії для повторного використання з'єднань ---
session = requests.Session()
session.headers.update({"Accept": "application/json"})

# -----------------------------------------------


def send_telegram_message(message_text: str, retries: int = 3) -> bool:
    """
    Відправляє форматоване повідомлення в Telegram УСІМ отримувачам.
    
    Returns:
        bool: True якщо успішно відправлено УСІМ, False якщо хоча б один не отримав
    """
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    all_successful = True
    
    for chat_id in TELEGRAM_CHAT_IDS:
        payload = {
            'chat_id': chat_id,
            'text': message_text,
            'parse_mode': 'Markdown'
        }
        
        sent_to_this_chat = False
        for attempt in range(retries):
            try:
                response = session.post(api_url, data=payload, timeout=10)
                if response.status_code == 200:
                    sent_to_this_chat = True
                    break  # Успішно відправлено цьому чату, виходимо зі спроб
                else:
                    logging.error(f"Помилка відправки в Telegram (Chat ID: {chat_id}, Спроба {attempt + 1}/{retries}): "
                                  f"{response.status_code} - {response.text}")
            except Exception as e:
                logging.error(f"Виняток під час відправки в Telegram (Chat ID: {chat_id}, Спроба {attempt + 1}/{retries}): {e}")
            
            if attempt < retries - 1:
                time.sleep(1)
        
        if not sent_to_this_chat:
            all_successful = False # Якщо хоча б один не отримав, фіксуємо помилку
            logging.error(f"НЕ ВДАЛОСЯ доставити повідомлення в Chat ID: {chat_id} після {retries} спроб.")

    return all_successful


def get_upbit_markets() -> Tuple[Optional[Set[str]], float]:
    """
    Отримує актуальний список торгових пар з Upbit.
    
    Returns:
        Tuple: (set_of_tickers, latency) або (None, 0) у разі помилки
    """
    try:
        start_time = time.time()
        response = session.get(UPBIT_API_URL, timeout=REQUEST_TIMEOUT)
        end_time = time.time()
        
        latency = end_time - start_time
        response.raise_for_status()
        
        data = response.json()
        tickers_set = {item['market'] for item in data if 'market' in item}
        
        return tickers_set, latency
        
    except requests.exceptions.HTTPError as http_err:
        logging.warning(f"HTTP помилка: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.warning(f"Помилка з'єднання: {conn_err}")
    except requests.exceptions.Timeout:
        logging.warning("Таймаут запиту до API Upbit.")
    except requests.exceptions.RequestException as req_err:
        logging.warning(f"Загальна помилка запиту: {req_err}")
    except (ValueError, KeyError) as e:
        logging.error(f"Помилка парсингу JSON: {e}")
    except Exception as e:
        logging.error(f"Неочікувана помилка в get_upbit_markets: {e}")
        
    return None, 0


def wait_for_initial_markets() -> Optional[Set[str]]:
    """
    Чекає на успішне отримання початкового списку пар з повторними спробами.
    
    Returns:
        Set[str] або None: Набір тікерів або None у разі невдачі
    """
    for attempt in range(MAX_RETRIES):
        logging.info(f"Спроба отримати початковий список пар ({attempt + 1}/{MAX_RETRIES})...")
        markets, _ = get_upbit_markets()
        
        if markets:
            return markets
        
        if attempt < MAX_RETRIES - 1:
            logging.warning(f"Повторна спроба через {RETRY_DELAY} секунд...")
            time.sleep(RETRY_DELAY)
    
    return None


def monitor_upbit_listings():
    """
    Головна функція моніторингу нових лістингів.
    """
    logging.info("Запуск моніторингу нових лістингів Upbit...")
    
    # Отримуємо початковий список з повторними спробами
    current_markets_set = wait_for_initial_markets()
    
    if not current_markets_set:
        error_msg = "❌ *Помилка!* Не вдалося отримати початковий список пар Upbit після всіх спроб. Скрипт зупинено."
        logging.error(error_msg)
        send_telegram_message(error_msg)
        return
        
    logging.info(f"Отримано початковий список: {len(current_markets_set)} пар.")
    send_telegram_message(
        f"✅ *Моніторинг Upbit запущено.*\n"
        f"Відстежується: {len(current_markets_set)} пар.\n"
        f"Інтервал перевірки: {CHECK_INTERVAL_SECONDS:.2f}с"
    )

    consecutive_errors = 0
    max_consecutive_errors = 10
    
    try:
        while True:
            new_markets_set, latency = get_upbit_markets()
            
            # Обробка помилок з лічильником
            if not new_markets_set:
                consecutive_errors += 1
                logging.warning(f"Пропуск ітерації через помилку ({consecutive_errors}/{max_consecutive_errors})")
                
                if consecutive_errors >= max_consecutive_errors:
                    error_msg = f"⚠️ *Увага!* {consecutive_errors} послідовних помилок з'єднання з API."
                    logging.error(error_msg)
                    send_telegram_message(error_msg)
                    consecutive_errors = 0  # Скидаємо лічильник після сповіщення
                
                time.sleep(CHECK_INTERVAL_SECONDS * 3)  # Збільшена затримка при помилці
                continue
            
            # Скидаємо лічильник помилок при успішному запиті
            if consecutive_errors > 0:
                logging.info("З'єднання відновлено.")
                consecutive_errors = 0
            
            # Порівнюємо списки
            if new_markets_set != current_markets_set:
                new_listings = new_markets_set - current_markets_set
                delisted_pairs = current_markets_set - new_markets_set
                
                # Обробка нових лістингів
                if new_listings:
                    detection_time = datetime.now()
                    logging.info(f"!!! ЗНАЙДЕНО НОВІ ПАРИ: {new_listings} !!!")
                    
                    # Групове повідомлення для кількох пар одночасно
                    if len(new_listings) == 1:
                        pair = list(new_listings)[0]
                        message = (
                            f"🔔 *Новий лістинг на Upbit!*\n\n"
                            f"*Тікер:* `{pair}`\n"
                            f"*Час:* `{detection_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}`\n"
                            f"*Затримка API:* `{latency:.3f}` сек"
                        )
                        send_telegram_message(message)
                    else:
                        pairs_list = '\n'.join([f"• `{pair}`" for pair in sorted(new_listings)])
                        message = (
                            f"🔔 *Нові лістинги на Upbit!*\n\n"
                            f"{pairs_list}\n\n"
                            f"*Час:* `{detection_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}`\n"
                            f"*Затримка API:* `{latency:.3f}` сек"
                        )
                        send_telegram_message(message)
                
                # Обробка делістингу (опціонально)
                if delisted_pairs:
                    logging.info(f"Зафіксовано делістинг: {delisted_pairs}")
                    # Розкоментуйте для сповіщень про делістинг:
                    # delisted_list = ', '.join([f"`{pair}`" for pair in sorted(delisted_pairs)])
                    # send_telegram_message(f"📉 *Делістинг:* {delisted_list}")
                
                current_markets_set = new_markets_set
            
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logging.info("Отримано сигнал зупинки (Ctrl+C). Завершення роботи...")
        send_telegram_message("🟡 *Моніторинг Upbit зупинено вручну.*")
    except Exception as e:
        logging.critical(f"Критична помилка: {e}", exc_info=True)
        send_telegram_message(f"❌ *Критична помилка!*\n```{str(e)[:200]}```")
    finally:
        session.close()


if __name__ == "__main__":
    monitor_upbit_listings()
import aiohttp
import asyncio
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ConversationHandler
from datetime import datetime, timedelta
import logging
import time
import traceback

# Состояния для ConversationHandler
CHOOSING_COINS, CHOOSING_PERIOD, CHOOSING_SORT, CHOOSING_SORT_TYPE = range(4)

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Токен вашего Telegram-бота (замените на актуальный)
TELEGRAM_TOKEN = '8162981904:AAGb1FejOHnYA924dgw1zpWKYMu9T2rPJQI'

# Bybit API endpoints
BYBIT_API_URL = 'https://api.bybit.com/v5/market'

# Кэш для хранения данных
COIN_CACHE = {}  # {symbol: {market_cap, ...}}
VOLATILITY_CACHE = {}  # {symbol: {days: volatility}}
DRAWDOWN_CACHE = {}  # {symbol: {days: drawdown}}
LAST_CACHE_TIME = None
CACHE_DURATION = 3600  # 1 час

# Функция для создания прогресс-бара
def get_progress_bar(progress, total, width=20):
    filled = int(width * progress / total)
    bar = '█' * filled + ' ' * (width - filled)
    percent = (progress / total) * 100
    return f'[{bar}] {percent:.1f}%'

# Асинхронная функция для получения списка монет с Bybit
async def get_coins(session):
    global COIN_CACHE, LAST_CACHE_TIME
    current_time = time.time()
    
    if LAST_CACHE_TIME and (current_time - LAST_CACHE_TIME < CACHE_DURATION) and COIN_CACHE:
        logger.info("Используется кэш монет")
        return COIN_CACHE
    
    try:
        logger.info("Запрос к API Bybit для получения списка монет")
        async with session.get(f'{BYBIT_API_URL}/tickers', params={'category': 'spot'}) as response:
            response.raise_for_status()
            data = await response.json()
            if data['retCode'] != 0:
                logger.error(f"Ошибка API Bybit: {data['retMsg']}")
                return {}
            
            # Обрабатываем все пары данных напрямую из API
            coin_list = data['result']['list']
            
            # Сохраняем все данные о монетах, объем берем напрямую из поля volume24h
            COIN_CACHE = {}
            for coin in coin_list:
                symbol = coin['symbol']
                
                # Берем правильные поля из API
                volume = float(coin.get('volume24h', 0)) if coin.get('volume24h') else 0
                turnover = float(coin.get('turnover24h', 0)) if coin.get('turnover24h') else 0
                market_cap = float(coin.get('marketCap', 0)) if coin.get('marketCap') else 0
                last_price = float(coin.get('lastPrice', 0)) if coin.get('lastPrice') else 0
                
                COIN_CACHE[symbol] = {
                    'market_cap': market_cap,
                    'volume': volume,
                    'turnover': turnover,  # Денежный объем (объем * цена)
                    'last_price': last_price
                }
            
            LAST_CACHE_TIME = current_time
            logger.info(f"Получено {len(COIN_CACHE)} монет")
            
            # Логируем топ монеты для проверки
            volume_sorted = sorted(COIN_CACHE.items(), key=lambda x: x[1]['turnover'], reverse=True)[:10]
            market_cap_sorted = sorted(COIN_CACHE.items(), key=lambda x: x[1]['market_cap'], reverse=True)[:10]
            
            volume_top_str = ", ".join([f"{coin[0]}={format_number(coin[1].get('turnover', 0))}" for coin in volume_sorted])
            marketcap_top_str = ", ".join([f"{coin[0]}={format_number(coin[1].get('market_cap', 0))}" for coin in market_cap_sorted])
            
            logger.info(f"Топ-10 монет по объему в USD: {volume_top_str}")
            logger.info(f"Топ-10 монет по капитализации: {marketcap_top_str}")
            
            return COIN_CACHE
    except Exception as e:
        logger.error(f"Ошибка при получении списка монет: {e}")
        logger.error(traceback.format_exc())
        return {}

# Асинхронная функция для получения исторических данных и расчета метрик
async def calculate_metrics(session, symbol, days):
    try:
        # Проверяем кэш
        cached_vol = VOLATILITY_CACHE.get(symbol, {}).get(days)
        cached_draw = DRAWDOWN_CACHE.get(symbol, {}).get(days)
        if cached_vol is not None and cached_draw is not None:
            logger.info(f"Используется кэш для {symbol} за {days} дней")
            return cached_vol, cached_draw
        
        logger.info(f"Запрос к API Bybit для {symbol} за {days} дней")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        params = {
            'category': 'spot',
            'symbol': symbol,
            'interval': 'D' if days > 1 else '60',  # Дневные данные или часовые для 24ч
            'start': start_time,
            'end': end_time
        }
        async with session.get(f'{BYBIT_API_URL}/kline', params=params) as response:
            response.raise_for_status()
            data = await response.json()
            if data['retCode'] != 0:
                logger.warning(f"Ошибка API для {symbol}: {data['retMsg']}")
                return None, None

            prices = data['result']['list']
            if not prices:
                logger.warning(f"Нет данных для {symbol}")
                return None, None

            closes = [float(p[4]) for p in prices]
            if not closes:
                return None, None

            # Волатильность: (max - min) / min * 100
            max_price = max(closes)
            min_price = min(closes)
            volatility = ((max_price - min_price) / min_price) * 100 if min_price != 0 else 0
            volatility = round(volatility, 2)

            # Просадка: (max - last) / max * 100
            last_price = closes[0]  # Последняя цена
            drawdown = ((max_price - last_price) / max_price) * 100 if max_price != 0 else 0
            drawdown = round(drawdown, 2)

            # Сохраняем в кэш
            VOLATILITY_CACHE.setdefault(symbol, {})[days] = volatility
            DRAWDOWN_CACHE.setdefault(symbol, {})[days] = drawdown
            logger.info(f"Рассчитаны метрики для {symbol}: волатильность={volatility}%, просадка={drawdown}%")
            return volatility, drawdown
    except Exception as e:
        logger.warning(f"Ошибка при расчете метрик для {symbol}: {e}")
        logger.warning(traceback.format_exc())
        return None, None

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        username = update.effective_user.username or "Неизвестный пользователь"
        logger.info(f"Получена команда /start от {user_id} ({username})")
        
        # Очищаем user_data перед новым выбором
        context.user_data.clear()
        
        keyboard = [
            [InlineKeyboardButton("10 монет", callback_data='10')],
            [InlineKeyboardButton("50 монет", callback_data='50')],
            [InlineKeyboardButton("100 монет", callback_data='100')],
            [InlineKeyboardButton("200 монет", callback_data='200')],
            [InlineKeyboardButton("500 монет", callback_data='500')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            'Выберите количество монет для анализа (Bybit, все спот пары):',
            reply_markup=reply_markup
        )
        logger.info(f"Команда /start выполнена успешно для {user_id}")
        return CHOOSING_COINS
    except Exception as e:
        logger.error(f"Ошибка в /start: {e}")
        logger.error(traceback.format_exc())
        await update.message.reply_text('Произошла ошибка. Попробуйте позже.')
        return ConversationHandler.END

# Обработчик выбора количества монет
async def select_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        user_id = update.effective_user.id
        await query.answer()
        
        num_coins = int(query.data)
        context.user_data['num_coins'] = num_coins
        logger.info(f"Пользователь {user_id} выбрал {num_coins} монет")
        
        # Добавляем выбор метода сортировки монет
        keyboard = [
            [InlineKeyboardButton("По объему торгов (USD)", callback_data='turnover')],
            [InlineKeyboardButton("По объему (количество)", callback_data='volume')],
            [InlineKeyboardButton("По капитализации", callback_data='marketcap')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(
            f'Вы выбрали {num_coins} монет. Выберите метод сортировки монет:',
            reply_markup=reply_markup
        )
        return CHOOSING_SORT_TYPE
    except Exception as e:
        user_id = update.effective_user.id if update and update.effective_user else "Неизвестный"
        logger.error(f"Ошибка в select_coins для пользователя {user_id}: {e}")
        logger.error(traceback.format_exc())
        await query.message.reply_text('Произошла ошибка. Попробуйте позже или начните заново с команды /start')
        return ConversationHandler.END

# Обработчик выбора типа сортировки монет
async def select_sort_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        user_id = update.effective_user.id
        await query.answer()
        
        sort_type = query.data
        context.user_data['sort_type'] = sort_type
        logger.info(f"Пользователь {user_id} выбрал сортировку монет по {sort_type}")
        
        keyboard = [
            [InlineKeyboardButton("24 часа", callback_data='1')],
            [InlineKeyboardButton("3 дня", callback_data='3')],
            [InlineKeyboardButton("7 дней", callback_data='7')],
            [InlineKeyboardButton("10 дней", callback_data='10')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Определяем текст для отображения
        sort_type_text = "объему торгов в USD" if sort_type == "turnover" else ("объему (количество)" if sort_type == "volume" else "капитализации")
        
        # Редактируем существующее сообщение вместо отправки нового
        await query.message.edit_text(
            f'Вы выбрали сортировку монет по {sort_type_text}. Выберите временной интервал:',
            reply_markup=reply_markup
        )
        return CHOOSING_PERIOD
    except Exception as e:
        user_id = update.effective_user.id if update and update.effective_user else "Неизвестный"
        logger.error(f"Ошибка в select_sort_type для пользователя {user_id}: {e}")
        logger.error(traceback.format_exc())
        await query.message.reply_text('Произошла ошибка. Попробуйте позже или начните заново с команды /start')
        return ConversationHandler.END

# Обработчик выбора периода
async def select_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        user_id = update.effective_user.id
        await query.answer()
        
        days = int(query.data)
        context.user_data['days'] = days
        
        keyboard = [
            [InlineKeyboardButton("Волатильность", callback_data='volatility')],
            [InlineKeyboardButton("Просадки", callback_data='drawdown')],
            [InlineKeyboardButton("Волатильность + Просадки", callback_data='both')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Редактируем существующее сообщение вместо отправки нового
        await query.message.edit_text(
            f'Вы выбрали период {days} дней. Выберите тип сортировки результатов:',
            reply_markup=reply_markup
        )
        logger.info(f"Пользователь {user_id} выбрал период {days} дней")
        return CHOOSING_SORT
    except Exception as e:
        user_id = update.effective_user.id if update and update.effective_user else "Неизвестный"
        logger.error(f"Ошибка в select_period для пользователя {user_id}: {e}")
        logger.error(traceback.format_exc())
        await query.message.reply_text('Произошла ошибка. Попробуйте позже или начните заново с команды /start')
        return ConversationHandler.END

# Обработчик сортировки и вывода результатов
async def show_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        user_id = update.effective_user.id
        await query.answer()
        
        sort_by = query.data
        num_coins = context.user_data.get('num_coins', 50)
        days = context.user_data.get('days', 7)
        sort_type = context.user_data.get('sort_type', 'turnover')
        
        logger.info(f"Пользователь {user_id} выбрал сортировку {sort_by}, начинаем анализ {num_coins} монет за {days} дней (сортировка монет по {sort_type})")
        
        # Начальное сообщение с прогресс-баром
        progress_message = await query.message.reply_text(
            f'Анализ {num_coins} монет за {days} дней...\n{get_progress_bar(0, num_coins)}'
        )
        
        async with aiohttp.ClientSession() as session:
            # Получаем список монет
            coins_data = await get_coins(session)
            if not coins_data:
                await query.message.reply_text('Не удалось получить список монет. Попробуйте позже.')
                await progress_message.delete()
                logger.error(f"Список монет пуст для пользователя {user_id}")
                return ConversationHandler.END

            # Сортируем по выбранному критерию (объем в долларах, объем в количестве или капитализация)
            sort_key = 'turnover' if sort_type == 'turnover' else ('volume' if sort_type == 'volume' else 'market_cap')
            sorted_coins = sorted(
                coins_data.items(),
                key=lambda x: x[1].get(sort_key, 0),
                reverse=True
            )[:num_coins]
            
            logger.info(f"Отобрано {len(sorted_coins)} монет для анализа")
            logger.info(f"Топ-5 анализируемых монет: {[coin[0] for coin in sorted_coins[:5]]}")
            
            results = []
            total_coins = len(sorted_coins)
            batch_size = 10  # Обрабатываем по 10 монет за раз
            
            for i in range(0, total_coins, batch_size):
                batch = sorted_coins[i:i + batch_size]
                tasks = [calculate_metrics(session, symbol, days) for symbol, _ in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for (symbol, coin_info), res in zip(batch, batch_results):
                    if isinstance(res, Exception) or res == (None, None):
                        logger.warning(f"Не удалось получить метрики для {symbol}: {res if isinstance(res, Exception) else 'Нет данных'}")
                        continue
                    volatility, drawdown = res
                    results.append({
                        'symbol': symbol,
                        'volatility': volatility,
                        'drawdown': drawdown,
                        'volume': coin_info.get('volume', 0),
                        'turnover': coin_info.get('turnover', 0),
                        'market_cap': coin_info.get('market_cap', 0),
                        'last_price': coin_info.get('last_price', 0)
                    })
                
                # Обновляем прогресс-бар
                processed = min(i + batch_size, total_coins)
                try:
                    await progress_message.edit_text(
                        f'Анализ {num_coins} монет за {days} дней...\n{get_progress_bar(processed, total_coins)}'
                    )
                except Exception as e:
                    logger.warning(f"Не удалось обновить прогресс-бар: {e}")
                await asyncio.sleep(0.1)
            
            if not results:
                await query.message.reply_text('Не удалось выполнить анализ. Попробуйте позже.')
                try:
                    await progress_message.delete()
                except Exception:
                    pass
                logger.error(f"Результаты анализа пусты для пользователя {user_id}")
                return ConversationHandler.END

            # Сортировка по выбранному критерию
            if sort_by == 'volatility':
                results.sort(key=lambda x: x['volatility'], reverse=True)
            elif sort_by == 'drawdown':
                results.sort(key=lambda x: x['drawdown'], reverse=True)
            else:  # both
                # Сортировка по сумме нормализованных значений
                max_vol = max(r['volatility'] for r in results) or 1
                max_draw = max(r['drawdown'] for r in results) or 1
                results.sort(
                    key=lambda x: (x['volatility'] / max_vol + x['drawdown'] / max_draw),
                    reverse=True
                )

            # Формируем сообщение
            sort_type_text = "объему торгов в USD" if sort_type == "turnover" else ("объему (количество)" if sort_type == "volume" else "капитализации")
            sort_result_text = "волатильности" if sort_by == "volatility" else ("просадкам" if sort_by == "drawdown" else "волатильности+просадкам")
            
            message = f'🔍 Анализ {len(results)} монет за {days} дней\n'
            message += f'📊 Монеты отобраны по {sort_type_text}\n'
            message += f'🔢 Результаты отсортированы по {sort_result_text}\n\n'
            
            for i, coin in enumerate(results, 1):
                # Форматируем данные для удобства чтения
                volume_formatted = format_number(coin['volume'])
                turnover_formatted = format_number(coin['turnover'])
                market_cap_formatted = format_number(coin['market_cap']) if coin['market_cap'] > 0 else "N/A"
                price_formatted = f"${coin['last_price']:.4f}" if coin['last_price'] < 1 else f"${coin['last_price']:.2f}"
                
                # Получаем базовую и котируемую валюты
                symbol_parts = coin['symbol'].split('USDT')
                base_currency = symbol_parts[0]
                quote_currency = "USDT"
                if len(symbol_parts) == 1 or not symbol_parts[0]:
                    # Если не USDT пара, пробуем другие варианты
                    for quote in ["BTC", "ETH", "USD", "EUR"]:
                        if quote in coin['symbol']:
                            parts = coin['symbol'].split(quote)
                            if parts[0]:
                                base_currency = parts[0]
                                quote_currency = quote
                                break
                
                # Создаем более форматированный вывод
                message += f"{i}. {base_currency}/{quote_currency}\n"
                message += f"   💰 Цена: {price_formatted}\n"
                message += f"   📈 Волатильность: {coin['volatility']}%\n"
                message += f"   📉 Просадка: {coin['drawdown']}%\n"
                message += f"   💵 Объем 24ч: ${turnover_formatted}\n"
                
                if len(message) > 3500:
                    await query.message.reply_text(message)
                    message = ""
            
            if message:
                await query.message.reply_text(message)
            
            try:
                await progress_message.delete()
            except Exception:
                pass
                
            logger.info(f"Результаты выведены для пользователя {user_id}: {len(results)} монет")
            
            # Предложение начать новый анализ
            await query.message.reply_text(
                "Анализ завершен. Чтобы начать новый анализ, используйте команду /start"
            )
            
            return ConversationHandler.END
    except Exception as e:
        user_id = update.effective_user.id if update and update.effective_user else "Неизвестный"
        logger.error(f"Ошибка в show_results для пользователя {user_id}: {e}")
        logger.error(traceback.format_exc())
        try:
            await query.message.reply_text('Произошла ошибка при анализе. Попробуйте позже или начните заново с команды /start')
            await progress_message.delete()
        except Exception:
            pass
        return ConversationHandler.END

# Функция для форматирования чисел (добавляет разделители и округляет большие числа)
def format_number(number):
    if number >= 1_000_000_000:  # миллиарды
        return f"{number / 1_000_000_000:.2f}B"
    elif number >= 1_000_000:  # миллионы
        return f"{number / 1_000_000:.2f}M"
    elif number >= 1_000:  # тысячи
        return f"{number / 1_000:.2f}K"
    else:
        return f"{number:.2f}"

# Функция отмены
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    logger.info(f"Пользователь {user_id} отменил операцию")
    await update.message.reply_text('Операция отменена. Чтобы начать новый анализ, используйте команду /start')
    return ConversationHandler.END

def main():
    try:
        application = Application.builder().token(TELEGRAM_TOKEN).build()
        
        # Создаем ConversationHandler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', start)],
            states={
                CHOOSING_COINS: [CallbackQueryHandler(select_coins, pattern='^(10|50|100|200|500)$')],
                CHOOSING_SORT_TYPE: [CallbackQueryHandler(select_sort_type, pattern='^(turnover|volume|marketcap)$')],
                CHOOSING_PERIOD: [CallbackQueryHandler(select_period, pattern='^(1|3|7|10)$')],
                CHOOSING_SORT: [CallbackQueryHandler(show_results, pattern='^(volatility|drawdown|both)$')],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
            per_user=True  # Изолированное состояние для каждого пользователя
        )
        
        application.add_handler(conv_handler)
        
        logger.info("Бот запущен")
        application.run_polling(allowed_updates=Update.ALL_TYPES, timeout=30)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
        logger.error(traceback.format_exc())

if __name__ == '__main__':
    main()

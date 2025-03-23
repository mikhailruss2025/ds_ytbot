import discord
from discord.ext import commands, tasks
from discord import ButtonStyle, app_commands
from discord.ui import Button, View
import yt_dlp as youtube_dl
import asyncio
import logging
import os
from dotenv import load_dotenv
from collections import deque, OrderedDict
from typing import Tuple, Optional, Dict, List, Union
from functools import lru_cache
import time
import random
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import shutil
import tempfile
import os.path
import subprocess

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
COOKIES_FILE = 'cookies.txt'
FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -timeout 10000000',
    'options': '-vn -timeout 10000000 -max_muxing_queue_size 1024'
}
MAX_QUEUE_SIZE = 50  # Максимальное количество треков в очереди

# Добавляем константы для таймаутов
FFMPEG_TIMEOUT = 30  # 30 секунд на инициализацию ffmpeg
FFMPEG_KILL_TIMEOUT = 5  # 5 секунд на принудительное завершение
MAX_RETRIES = 3  # Максимальное количество попыток

YDL_OPTIONS = {
    'format': 'bestaudio/best',
    'noplaylist': False,
    'ignoreerrors': True,
    'no_warnings': True,
    'quiet': True,
    'extract_flat': False,
    'no_check_certificate': True,
    'socket_timeout': 15,
    'retries': 5,
    'verbose': True,
    'age_limit': 21,
    'cookiefile': COOKIES_FILE,
    'http_headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-us,en;q=0.5',
        'Sec-Fetch-Mode': 'navigate'
    },
    'extractor_args': {
        'youtube': {
            'skip': [],
            'player_skip': [],
            'skip_unavailable_videos': True,
            'player_client': 'web',
            'player_skip_formats': ['dash', 'hls']
        }
    }
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('music_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YouTubeAccessError(Exception):
    """Ошибка доступа к YouTube"""
    pass

# Оптимизированный кэш с TTL
class TTLCache:
    def __init__(self, max_size=1000, ttl=3600):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl

    def get(self, key):
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                self.cache.move_to_end(key)
                return value
            else:
                del self.cache[key]
        return None

    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[key] = (value, time.time())
        self.cache.move_to_end(key)

    def clear_expired(self):
        current_time = time.time()
        expired = [k for k, (_, t) in self.cache.items() if current_time - t >= self.ttl]
        for k in expired:
            del self.cache[k]

# Создаем экземпляр кэша
audio_cache = TTLCache(max_size=1000, ttl=3600)

# Кэш для хранения информации о треках
track_cache: Dict[str, Tuple[str, str, float]] = {}
CACHE_DURATION = 3600  # 1 час

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

class MusicBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.voice_states = True
        
        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None,
            case_insensitive=True
        )
        self.voice_states = {}
        self._cleanup_task = None
        self._is_shutting_down = False

    async def setup_hook(self):
        """Вызывается при запуске бота"""
        try:
            # Принудительная синхронизация всех команд
            commands = await self.tree.sync()
            logger.info(f"Слэш-команды синхронизированы! Количество команд: {len(commands)}")
            for cmd in commands:
                logger.info(f"Синхронизирована команда: /{cmd.name}")
            self._cleanup_task = self.loop.create_task(self._cleanup_states())
        except Exception as e:
            logger.error(f"Ошибка синхронизации команд: {e}")

    async def close(self):
        """Корректное завершение работы бота"""
        self._is_shutting_down = True
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Отключаем все голосовые соединения
        for guild_id, state in guild_states.items():
            try:
                if state.voice_client:
                    # Останавливаем воспроизведение
                    try:
                        if state.voice_client.is_playing():
                            state.voice_client.stop()
                        if hasattr(state.voice_client, 'source'):
                            state.voice_client.source = None
                    except:
                        pass
                    
                    # Отключаемся от канала
                    try:
                        if state.voice_client.is_connected():
                            await state.voice_client.disconnect(force=True)
                    except:
                        pass
                    
                    # Очищаем состояние
                    state.voice_client = None
                    state.is_playing = False
                    state.current_track = None
            except Exception as e:
                logger.error(f"Ошибка при отключении от сервера {guild_id}: {e}")
        
        # Очищаем все состояния и кэши
        guild_states.clear()
        queues.clear()
        audio_cache.clear_expired()
        track_cache.clear()
        
        # Закрываем YouTube клиент
        await youtube_client.close()
        
        await super().close()

    async def _cleanup_states(self):
        """Периодическая очистка неиспользуемых состояний"""
        while not self._is_shutting_down:
            try:
                current_time = time.time()
                
                # Очищаем истекшие записи в кэше
                audio_cache.clear_expired()
                
                # Проверяем все состояния серверов
                for guild_id, state in list(guild_states.items()):
                    try:
                        # Проверяем неактивные соединения
                        if current_time - state.last_activity > 3600:  # 1 час
                            if state.voice_client and state.voice_client.is_connected():
                                await state.voice_client.disconnect()
                            del guild_states[guild_id]
                            
                        # Проверяем зависшие состояния
                        elif state.voice_client and not state.voice_client.is_connected():
                            if not state.is_playing and not state.queue:
                                del guild_states[guild_id]
                    except Exception as e:
                        logger.error(f"Ошибка при очистке состояния сервера {guild_id}: {e}")
                
                await asyncio.sleep(300)  # Проверяем каждые 5 минут
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в процессе очистки: {e}")
                await asyncio.sleep(60)

    async def on_ready(self):
        """Вызывается когда бот готов к работе"""
        logger.info(f"Бот {self.user} готов к работе!")
        logger.info(f"ID бота: {self.user.id}")
        logger.info("Доступные слэш-команды:")
        for cmd in self.tree.get_commands():
            logger.info(f"/{cmd.name} - {cmd.description}")

bot = MusicBot()

# Добавляем константу для максимального размера очереди
MAX_QUEUE_SIZE = 50  # Максимальное количество треков в очереди

# Добавляем константу для таймаута воспроизведения
PLAY_TIMEOUT = 300  # 5 минут максимум на один трек

def check_cookies() -> None:
    """Проверяет наличие и валидность cookies файла"""
    try:
        if not os.path.exists(COOKIES_FILE):
            logger.warning("Файл cookies.txt не найден, создаем пустой файл")
            with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
                f.write("")
        elif os.path.getsize(COOKIES_FILE) < 100:
            logger.warning("Файл cookies.txt пустой или слишком маленький")
    except Exception as e:
        logger.error(f"Ошибка при проверке cookies: {e}")

def get_queue(guild_id: int):
    """Получение очереди для конкретного сервера"""
    if guild_id not in queues:
        queues[guild_id] = []  # Используем список вместо deque для более гибкой работы
    return queues[guild_id]

async def connect_to_voice(ctx: commands.Context) -> discord.VoiceClient:
    """Подключение к голосовому каналу"""
    check_cookies()
    
    if not ctx.author.voice:
        raise commands.CommandError("Вы должны быть в голосовом канале!")
    
    channel = ctx.author.voice.channel
    
    # Добавляем повторные попытки подключения
    max_retries = 3
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            # Проверяем текущее подключение
            guild_state = get_guild_state(ctx.guild.id)
            if guild_state.voice_client and guild_state.voice_client.is_connected():
                try:
                    if guild_state.voice_client.channel.id == channel.id:
                        return guild_state.voice_client
                    await guild_state.voice_client.move_to(channel)
                    await asyncio.sleep(1)  # Ждем завершения перемещения
                    return guild_state.voice_client
                except Exception as e:
                    logger.warning(f"Ошибка при перемещении: {e}")
                    # Если не удалось переместить, пробуем отключиться и подключиться заново
                    try:
                        await guild_state.voice_client.disconnect(force=True)
                    except:
                        pass
            
            # Пытаемся подключиться
            voice_client = await channel.connect(timeout=20.0, reconnect=True)
            await asyncio.sleep(1)  # Даем время на установку соединения
            
            if voice_client and voice_client.is_connected():
                return voice_client
                
        except Exception as e:
            last_error = e
            logger.warning(f"Попытка {retry_count + 1} подключения не удалась: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(2)  # Увеличиваем задержку между попытками
            
    if last_error:
        logger.error(f"Не удалось подключиться после {max_retries} попыток: {str(last_error)}")
        raise commands.CommandError(f"Не удалось подключиться к голосовому каналу: {str(last_error)}")
    
    raise commands.CommandError("Не удалось подключиться к голосовому каналу")

class GuildState:
    def __init__(self):
        self.queue = deque(maxlen=MAX_QUEUE_SIZE)  # Используем deque с максимальным размером
        self.playlist_queue = deque(maxlen=MAX_QUEUE_SIZE)
        self.current_track = None
        self.disconnect_timer = None
        self.last_activity = time.time()
        self.voice_client = None
        self.is_playing = False
        self.volume = 1.0
        self._lock = asyncio.Lock()
        self._queue_event = asyncio.Event()  # Для оповещения о новых треках

    async def add_to_queue(self, tracks: Union[Tuple[str, str], List[Union[Tuple[str, str], dict]]]) -> int:
        """Добавляет трек или треки в очередь с блокировкой"""
        async with self._lock:
            added_count = 0
            
            if isinstance(tracks, tuple):
                if len(self.queue) < MAX_QUEUE_SIZE:
                    # Добавляем случайную задержку от 10 до 15 секунд
                    await asyncio.sleep(random.uniform(10, 15))
                    self.queue.append(tracks)
                    added_count = 1
                    self._queue_event.set()
            else:
                available_slots = MAX_QUEUE_SIZE - len(self.queue)
                if available_slots > 0:
                    # Фильтруем и добавляем треки
                    for track in tracks[:available_slots]:
                        if isinstance(track, tuple):
                            # Добавляем случайную задержку от 10 до 15 секунд
                            await asyncio.sleep(random.uniform(10, 15))
                            self.queue.append(track)
                            added_count += 1
                        else:
                            self.playlist_queue.append(track)
                            added_count += 1
                    if added_count > 0:
                        self._queue_event.set()
            
            self.update_activity()
            return added_count

    async def get_next_track(self) -> Optional[Tuple[str, str]]:
        """Получает следующий трек из очереди"""
        async with self._lock:
            if self.queue:
                return self.queue.popleft()
            
            while self.playlist_queue:
                entry = self.playlist_queue.popleft()
                try:
                    track_info = await process_playlist_entry(entry)
                    if track_info:
                        return track_info
                except Exception as e:
                    logger.warning(f"Ошибка обработки трека из плейлиста: {str(e)}")
                    continue
            
            self._queue_event.clear()
            return None

    async def wait_for_tracks(self, timeout: float = None) -> bool:
        """Ожидает появления новых треков в очереди"""
        try:
            await asyncio.wait_for(self._queue_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def get_queue_length(self) -> int:
        """Возвращает текущую длину очереди"""
        return len(self.queue) + len(self.playlist_queue)

    async def clear_queue(self):
        """Очищает очередь"""
        async with self._lock:
            self.queue.clear()
            self.playlist_queue.clear()
            self._queue_event.clear()
            self.update_activity()

    def update_activity(self):
        self.last_activity = time.time()

    def clear(self):
        """Очищает состояние сервера"""
        self.queue.clear()
        self.current_track = None
        self.is_playing = False
        if self.disconnect_timer:
            self.disconnect_timer.cancel()
            self.disconnect_timer = None
        if self.voice_client and self.voice_client.is_connected():
            asyncio.create_task(self.voice_client.disconnect())
        self.voice_client = None

    def update_voice_client(self, voice_client: Optional[discord.VoiceClient]):
        """Обновляет состояние голосового клиента"""
        self.voice_client = voice_client
        if not voice_client:
            self.is_playing = False
            if self.disconnect_timer:
                self.disconnect_timer.cancel()
                self.disconnect_timer = None

# Глобальный словарь для хранения состояний серверов
guild_states: Dict[int, GuildState] = {}

# Глобальный словарь для хранения очередей
queues: Dict[int, List[Tuple[str, str]]] = {}

def get_guild_state(guild_id: int) -> GuildState:
    """Получение состояния для конкретного сервера"""
    if guild_id not in guild_states:
        guild_states[guild_id] = GuildState()
    return guild_states[guild_id]

def get_best_audio_format(formats: List[dict]) -> Optional[str]:
    """Находит лучший доступный аудио формат"""
    if not formats:
        return None
        
    # Сначала ищем m4a аудио форматы
    m4a_formats = []
    audio_formats = []
    
    for f in formats:
        if not isinstance(f, dict):
            continue
            
        if f.get('acodec') == 'none' or not f.get('url'):
            continue
            
        format_data = {
            'url': f['url'],
            'abr': float(f.get('abr', 0) or 0),
            'asr': float(f.get('asr', 0) or 0),
            'filesize': float(f.get('filesize', 0) or 0),
            'format_id': f.get('format_id', ''),
            'ext': f.get('ext', ''),
            'vcodec': f.get('vcodec', ''),
            'protocol': f.get('protocol', '')
        }
        
        # Пропускаем форматы с протоколами dash и hls
        if 'dash' in format_data['protocol'].lower() or 'hls' in format_data['protocol'].lower():
            continue
            
        # Пропускаем форматы без аудио или с проблемными кодеками
        if not format_data['abr'] and not format_data['asr']:
            continue
            
        if format_data['ext'] == 'm4a':
            m4a_formats.append(format_data)
        else:
            audio_formats.append(format_data)
    
    # Сортируем форматы по приоритету
    if m4a_formats:
        m4a_formats.sort(key=lambda x: (x['abr'], -x['filesize']), reverse=True)
        return m4a_formats[0]['url']
    
    if audio_formats:
        audio_formats.sort(key=lambda x: (x['abr'], -x['filesize']), reverse=True)
        return audio_formats[0]['url']
    
    # Если не нашли аудио форматы, ищем любые форматы с URL и аудио
    for f in formats:
        if not isinstance(f, dict):
            continue
            
        if f.get('url') and f.get('acodec') != 'none':
            return f['url']
    
    return None

@lru_cache(maxsize=100)
def extract_audio_info(url: str, process_playlist: bool = False) -> Union[Tuple[str, str], List[Tuple[str, str]]]:
    """Извлекает информацию о видео или плейлисте с YouTube с кэшированием"""
    cache_key = f"{url}_{process_playlist}"
    current_time = time.time()
    
    # Проверяем кэш
    cached_data = audio_cache.get(cache_key)
    if cached_data:
        return cached_data
    
    try:
        # Исправляем URL если он начинается с ttps://
        if url.startswith('ttps://'):
            url = 'h' + url
        
        # Если URL не начинается с http:// или https://, считаем его поисковым запросом
        if not url.startswith(('http://', 'https://')):
            url = f"ytsearch:{url}"

        ydl_opts = YDL_OPTIONS.copy()
        if process_playlist:
            ydl_opts.update({
                'extract_flat': 'in_playlist',
                'playlistend': 50,
                'playlistreverse': False,
                'playlist_items': '1-50',
                'ignoreerrors': True,
                'no_warnings': True,
                'quiet': True
            })
        
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            try:
                # Получаем базовую информацию
                info = ydl.extract_info(url, download=False)
                if not info:
                    raise YouTubeAccessError("Не удалось получить информацию о видео")

                # Обработка плейлиста
                if process_playlist and ('entries' in info or info.get('_type') == 'playlist'):
                    entries = info.get('entries', [])
                    if not entries:
                        raise YouTubeAccessError("Плейлист пуст или недоступен")
                    
                    # Фильтруем и обрабатываем записи плейлиста
                    playlist_entries = []
                    for entry in entries:
                        if not entry:
                            continue
                            
                        try:
                            # Пропускаем приватные и недоступные видео
                            if entry.get('availability') in ['private', 'needs_auth']:
                                continue
                                
                            # Получаем информацию о каждом видео отдельно
                            video_url = entry.get('url') or entry.get('webpage_url')
                            if not video_url and entry.get('id'):
                                video_url = f"https://www.youtube.com/watch?v={entry['id']}"
                                
                            if not video_url:
                                continue
                                
                            try:
                                video_info = ydl.extract_info(video_url, download=False)
                                if not video_info:
                                    continue
                                    
                                # Проверяем ограничения
                                if video_info.get('is_live') or video_info.get('was_live'):
                                    continue
                                    
                                if video_info.get('duration', 0) > 7200:  # 2 часа
                                    continue
                                
                                # Получаем URL аудио
                                formats = video_info.get('formats', [])
                                if not formats:
                                    continue
                                    
                                audio_formats = []
                                for f in formats:
                                    if not isinstance(f, dict):
                                        continue
                                        
                                    if f.get('acodec') == 'none' or not f.get('url'):
                                        continue
                                        
                                    # Получаем битрейт, проверяем что это число
                                    try:
                                        abr = float(f.get('abr', 0) or f.get('tbr', 0) or 0)
                                    except (ValueError, TypeError):
                                        continue
                                        
                                    if not abr:
                                        continue
                                        
                                    audio_formats.append({
                                        'url': f['url'],
                                        'abr': abr,
                                        'ext': f.get('ext', ''),
                                        'protocol': f.get('protocol', '').lower()
                                    })
                                
                                if not audio_formats:
                                    continue
                                
                                # Сортируем по битрейту
                                audio_formats.sort(key=lambda x: x['abr'], reverse=True)
                                audio_url = audio_formats[0]['url']
                                
                                # Получаем название
                                title = video_info.get('title', entry.get('title', 'Без названия'))
                                if not title or title == 'Без названия':
                                    title = video_info.get('fulltitle', video_info.get('alt_title', 'Без названия'))
                                
                                playlist_entries.append((audio_url, title))
                                
                            except youtube_dl.utils.DownloadError:
                                continue
                            
                        except Exception as e:
                            logger.warning(f"Пропуск трека в плейлисте: {str(e)}")
                            continue
                    
                    if not playlist_entries:
                        raise YouTubeAccessError("В плейлисте нет доступных треков")
                    
                    audio_cache.set(cache_key, playlist_entries)
                    return playlist_entries

                # Обработка одиночного видео
                return process_single_video(info, ydl)

            except youtube_dl.utils.DownloadError as e:
                error_message = str(e).lower()
                if "sign in to confirm" in error_message:
                    logger.warning("Видео имеет возрастные ограничения, пробуем с повышенным age_limit")
                    ydl_opts['age_limit'] = 25
                    return extract_audio_info(url, process_playlist)
                elif "video unavailable" in error_message or "private video" in error_message:
                    raise YouTubeAccessError("Видео недоступно или является приватным")
                else:
                    logger.error(f"Ошибка загрузки: {error_message}")
                    raise YouTubeAccessError(f"Ошибка при загрузке видео: {str(e)}")
            except Exception as e:
                if isinstance(e, TimeoutError) or "timeout" in str(e).lower():
                    raise YouTubeAccessError("Превышено время ожидания при загрузке. Попробуйте позже.")
                raise
            
    except Exception as e:
        logger.error(f"Ошибка извлечения аудио: {str(e)}")
        raise YouTubeAccessError(f"Неизвестная ошибка: {str(e)}")

def process_single_video(info: dict, ydl: youtube_dl.YoutubeDL) -> Tuple[str, str]:
    """Обрабатывает одиночное видео"""
    try:
        # Проверяем ограничения
        if info.get('is_live'):
            raise YouTubeAccessError("Лайв-стримы не поддерживаются")
        
        if info.get('duration', 0) > 7200:
            raise YouTubeAccessError("Видео слишком длинное (максимум 2 часа)")
        
        # Получаем полную информацию о видео если нужно
        video_info = info
        if not info.get('formats') and info.get('webpage_url'):
            video_info = ydl.extract_info(info['webpage_url'], download=False)
        
        if not video_info:
            raise YouTubeAccessError("Не удалось получить информацию о видео")
        
        # Получаем URL аудио
        formats = video_info.get('formats', [])
        if not formats:
            raise YouTubeAccessError("Не удалось получить форматы видео")
            
        # Ищем лучший аудио формат
        audio_formats = []
        for f in formats:
            if not isinstance(f, dict):
                continue
                
            if f.get('acodec') == 'none' or not f.get('url'):
                continue
                
            # Получаем битрейт, проверяем что это число
            try:
                abr = float(f.get('abr', 0) or f.get('tbr', 0) or 0)
            except (ValueError, TypeError):
                continue
                
            if not abr:
                continue
                
            audio_formats.append({
                'url': f['url'],
                'abr': abr,
                'ext': f.get('ext', ''),
                'protocol': f.get('protocol', '').lower()
            })
        
        if not audio_formats:
            raise YouTubeAccessError("Не найдены аудио форматы")
        
        # Сортируем по битрейту и выбираем лучший
        audio_formats.sort(key=lambda x: x['abr'], reverse=True)
        audio_url = audio_formats[0]['url']
        
        # Получаем название
        title = video_info.get('title', 'Без названия')
        if not title or title == 'Без названия':
            title = video_info.get('fulltitle', video_info.get('alt_title', 'Без названия'))

        return (audio_url, title)
        
    except Exception as e:
        logger.error(f"Ошибка обработки видео: {str(e)}")
        raise YouTubeAccessError(f"Ошибка обработки видео: {str(e)}")

async def process_playlist_entry(entry: dict) -> Optional[Tuple[str, str]]:
    """Обрабатывает отдельную запись из плейлиста с задержкой"""
    try:
        # Проверяем длительность если она доступна
        if entry.get('duration', 0) > 7200:  # 2 часа
            logger.warning(f"Пропускаем трек {entry['title']}: слишком длинный")
            return None
            
        # Добавляем случайную задержку от 1 до 3 секунд
        await asyncio.sleep(random.uniform(1, 3))
        
        url = entry.get('webpage_url') or f"https://www.youtube.com/watch?v={entry['id']}"
        
        # Добавляем случайный User-Agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio/best',
            'extract_flat': False,
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
            'no_check_certificate': True,
            'socket_timeout': 15,
            'retries': 3,
            'skip_unavailable_videos': True,
            'http_headers': {
                'User-Agent': random.choice(user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
                'Sec-Fetch-Mode': 'navigate'
            },
            'extractor_args': {
                'youtube': {
                    'skip': [],
                    'player_skip': [],
                    'skip_unavailable_videos': True,
                    'player_client': 'web',
                    'player_skip_formats': ['dash', 'hls']
                }
            }
        }
        
        max_retries = 2
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                    # Сначала получаем базовую информацию
                    info = ydl.extract_info(url, download=False, process=False)
                    if not info:
                        retry_count += 1
                        await asyncio.sleep(random.uniform(1, 2))
                        continue
                    
                    # Проверяем базовые ограничения
                    if info.get('is_live') or info.get('was_live'):
                        logger.warning(f"Пропускаем трек {entry['title']}: это прямая трансляция")
                        return None
                        
                    if info.get('duration', 0) > 7200:
                        logger.warning(f"Пропускаем трек {entry['title']}: слишком длинный")
                        return None
                    
                    # Получаем полную информацию
                    video_info = ydl.extract_info(url, download=False)
                    if not video_info:
                        retry_count += 1
                        await asyncio.sleep(random.uniform(1, 2))
                        continue
                    
                    # Получаем URL аудио
                    audio_url = get_best_audio_format(video_info.get('formats', []))
                    if not audio_url:
                        retry_count += 1
                        continue
                    
                    # Получаем название
                    title = video_info.get('title', entry.get('title', 'Без названия'))
                    if not title or title == 'Без названия':
                        title = video_info.get('fulltitle', video_info.get('alt_title', 'Без названия'))
                    
                    return (audio_url, title)
                    
            except youtube_dl.utils.DownloadError as e:
                last_error = e
                retry_count += 1
                if retry_count < max_retries:
                    await asyncio.sleep(random.uniform(1, 2))
                continue
            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count < max_retries:
                    await asyncio.sleep(random.uniform(1, 2))
                continue
        
        if last_error:
            logger.warning(f"Не удалось обработать трек {entry.get('title', 'Unknown')} после {max_retries} попыток: {str(last_error)}")
        return None
        
    except Exception as e:
        logger.warning(f"Ошибка обработки видео из плейлиста: {str(e)}")
        return None

class InteractionContext:
    """Обертка для Interaction, чтобы использовать его как Context"""
    def __init__(self, interaction: discord.Interaction):
        self.interaction = interaction
        self.guild = interaction.guild
        self.guild_id = interaction.guild_id
        self.channel = interaction.channel
        self.author = interaction.user
        self._sent_messages = []
        
        # Получаем состояние сервера
        guild_state = get_guild_state(self.guild_id)
        self.voice_client = guild_state.voice_client or interaction.guild.voice_client

    async def send(self, content: str, **kwargs):
        """Отправка сообщения через interaction"""
        try:
            if not self._sent_messages:  # Первое сообщение
                if not self.interaction.response.is_done():
                    await self.interaction.response.send_message(content, **kwargs)
                    message = await self.interaction.original_response()
                else:
                    message = await self.channel.send(content, **kwargs)
                self._sent_messages.append(message)
            else:  # Последующие сообщения
                message = await self.channel.send(content, **kwargs)
                self._sent_messages.append(message)
            return self._sent_messages[-1]
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            # Пробуем отправить через канал если interaction не работает
            try:
                message = await self.channel.send(content, **kwargs)
                self._sent_messages.append(message)
                return message
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения через канал: {e2}")
                return None

class DisconnectTimer:
    def __init__(self):
        self.task = None
        self._is_running = False
        self._lock = asyncio.Lock()
    
    def cancel(self):
        if self.task and not self.task.done():
            self.task.cancel()
            self._is_running = False
    
    async def start(self, ctx):
        async with self._lock:
            self.cancel()
            self._is_running = True
            
            async def disconnect_after_delay():
                try:
                    await asyncio.sleep(600)  # 10 минут
                    async with self._lock:
                        if not self._is_running:
                            return
                            
                        guild_state = get_guild_state(ctx.guild.id)
                        if guild_state.voice_client and guild_state.voice_client.is_connected():
                            await guild_state.voice_client.disconnect()
                            guild_state.update_voice_client(None)
                            try:
                                await ctx.send("⏰ Бот отключён из-за отсутствия активности!")
                            except:
                                logger.warning("Не удалось отправить сообщение об отключении")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Ошибка в таймере отключения: {e}")
            
            self.task = asyncio.create_task(disconnect_after_delay())

# Глобальный словарь для хранения таймеров отключения
disconnect_timers = {}

async def kill_ffmpeg_process(process):
    """Принудительно завершает процесс ffmpeg"""
    try:
        if process and process.poll() is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=FFMPEG_KILL_TIMEOUT)
            except asyncio.TimeoutError:
                process.kill()
    except Exception as e:
        logger.error(f"Ошибка при завершении ffmpeg процесса: {e}")

class FFmpegAudio(discord.FFmpegPCMAudio):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._process = None
        self._start_time = None
        self._retry_count = 0
        
    async def _start_ffmpeg(self):
        """Запускает ffmpeg процесс с таймаутом и повторными попытками"""
        while self._retry_count < MAX_RETRIES:
            try:
                self._process = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: subprocess.Popen(
                            self._cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            **self._popen_kwargs
                        )
                    ),
                    timeout=FFMPEG_TIMEOUT
                )
                self._start_time = time.time()
                return True
            except asyncio.TimeoutError:
                self._retry_count += 1
                logger.warning(f"Таймаут запуска ffmpeg (попытка {self._retry_count}/{MAX_RETRIES})")
                if self._process:
                    await kill_ffmpeg_process(self._process)
                await asyncio.sleep(1)
            except Exception as e:
                self._retry_count += 1
                logger.error(f"Ошибка запуска ffmpeg: {e}")
                if self._process:
                    await kill_ffmpeg_process(self._process)
                await asyncio.sleep(1)
        return False
        
    def cleanup(self):
        """Улучшенная очистка ресурсов"""
        try:
            if self._process:
                asyncio.create_task(kill_ffmpeg_process(self._process))
            super().cleanup()
        except Exception as e:
            logger.error(f"Ошибка при очистке ffmpeg: {e}")

async def play_next(ctx):
    """Воспроизводит следующий трек из очереди"""
    if isinstance(ctx, discord.Interaction):
        ctx = InteractionContext(ctx)
    
    guild_state = get_guild_state(ctx.guild.id)
    guild_state.update_activity()
    
    if guild_state.disconnect_timer:
        guild_state.disconnect_timer.cancel()
    
    try:
        next_track = await guild_state.get_next_track()
        if not next_track:
            guild_state.is_playing = False
            guild_state.current_track = None
            if not guild_state.disconnect_timer:
                guild_state.disconnect_timer = DisconnectTimer()
            await guild_state.disconnect_timer.start(ctx)
            return
        
        # Подключаемся к голосовому каналу если нужно
        voice_client = await connect_to_voice(ctx)
        if not voice_client:
            logger.error("Не удалось подключиться к голосовому каналу")
            await ctx.send("❌ Ошибка подключения к голосовому каналу")
            return
            
        guild_state.voice_client = voice_client
        guild_state.update_voice_client(voice_client)
        
        # Воспроизводим трек с таймаутом
        try:
            audio_url, title = next_track
            guild_state.current_track = (audio_url, title)
            guild_state.is_playing = True
            
            # Создаем таймер для принудительного пропуска трека
            skip_timer = None
            
            def after_callback(error):
                if error:
                    logger.error(f"Ошибка воспроизведения: {error}")
                if skip_timer and not skip_timer.done():
                    skip_timer.cancel()
                asyncio.run_coroutine_threadsafe(
                    handle_song_complete(ctx, error), bot.loop
                ).result()
            
            # Создаем аудио источник с улучшенной обработкой
            audio_source = FFmpegAudio(audio_url, **FFMPEG_OPTIONS)
            
            # Запускаем воспроизведение
            guild_state.voice_client.play(audio_source, after=after_callback)
            guild_state.voice_client.source.volume = guild_state.volume
            
            # Создаем таймер для автоматического пропуска
            async def skip_after_timeout():
                try:
                    await asyncio.sleep(PLAY_TIMEOUT)
                    if guild_state.voice_client and guild_state.voice_client.is_playing():
                        logger.warning(f"Трек {title} играет слишком долго, пропускаем")
                        guild_state.voice_client.stop()
                except asyncio.CancelledError:
                    pass
                
            skip_timer = asyncio.create_task(skip_after_timeout())
            
            view = MusicControlView(ctx)
            await ctx.send(f"▶️ Сейчас играет: {title}", view=view)
            
        except Exception as e:
            logger.error(f"Ошибка при воспроизведении аудио: {e}")
            guild_state.is_playing = False
            guild_state.current_track = None
            await ctx.send("❌ Ошибка воспроизведения: не удалось воспроизвести аудио")
            await handle_song_complete(ctx, e)
            
    except Exception as e:
        logger.error(f"Общая ошибка воспроизведения: {e}")
        guild_state.is_playing = False
        guild_state.current_track = None
        await ctx.send("❌ Произошла неизвестная ошибка при воспроизведении")
        await handle_song_complete(ctx, e)

def check_disk_space():
    """Проверяет свободное место на диске и очищает временные файлы если нужно"""
    try:
        # Получаем путь к временной директории
        temp_dir = tempfile.gettempdir()
        
        # Проверяем свободное место
        total, used, free = shutil.disk_usage(temp_dir)
        free_gb = free / (2**30)  # Конвертируем в ГБ
        
        if free_gb < 0.5:  # Если меньше 500 МБ свободно
            logger.warning(f"Очень мало места на диске: {free_gb:.2f} ГБ")
            
            # Очищаем временные файлы
            for filename in os.listdir(temp_dir):
                if filename.startswith('yt-dlp') or filename.startswith('discord-'):
                    filepath = os.path.join(temp_dir, filename)
                    try:
                        if os.path.isfile(filepath):
                            os.unlink(filepath)
                    except Exception as e:
                        logger.error(f"Ошибка при удалении {filepath}: {e}")
            
            # Проверяем место снова
            total, used, free = shutil.disk_usage(temp_dir)
            free_gb = free / (2**30)
            logger.info(f"После очистки: {free_gb:.2f} ГБ свободно")
            
            if free_gb < 0.1:  # Если меньше 100 МБ после очистки
                logger.error("Критически мало места на диске")
                return False
        
        return True
                
    except Exception as e:
        logger.error(f"Ошибка при проверке места на диске: {e}")
        return False

async def handle_song_complete(ctx, error):
    """Обработчик завершения песни"""
    if isinstance(ctx, discord.Interaction):
        ctx = InteractionContext(ctx)
        
    guild_state = get_guild_state(ctx.guild_id)
    
    try:
        if error:
            logger.error(f"Ошибка воспроизведения: {error}")
            try:
                await ctx.send("❌ Произошла ошибка при воспроизведении")
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения об ошибке: {e}")
        
        # Принудительно очищаем текущий источник воспроизведения
        try:
            if guild_state.voice_client:
                guild_state.voice_client.stop()
                if hasattr(guild_state.voice_client, 'source'):
                    try:
                        guild_state.voice_client.source.cleanup()
                    except:
                        pass
                    guild_state.voice_client.source = None
        except Exception as e:
            logger.error(f"Ошибка при очистке источника воспроизведения: {e}")
        
        # Проверяем место на диске
        if not check_disk_space():
            logger.warning("Недостаточно места на диске, пропускаем следующий трек")
            await ctx.send("⚠️ Внимание: мало места на диске, возможны проблемы с воспроизведением")
        
        guild_state.current_track = None
        guild_state.is_playing = False
        
        # Проверяем наличие следующего трека
        if guild_state.queue or guild_state.playlist_queue:
            try:
                await play_next(ctx)
            except Exception as e:
                logger.error(f"Ошибка при воспроизведении следующего трека: {e}")
                if not guild_state.disconnect_timer:
                    guild_state.disconnect_timer = DisconnectTimer()
                await guild_state.disconnect_timer.start(ctx)
        else:
            if not guild_state.disconnect_timer:
                guild_state.disconnect_timer = DisconnectTimer()
            await guild_state.disconnect_timer.start(ctx)
            
    except Exception as e:
        logger.error(f"Ошибка в обработчике завершения песни: {e}")
        if not guild_state.disconnect_timer:
            guild_state.disconnect_timer = DisconnectTimer()
        await guild_state.disconnect_timer.start(ctx)

class MusicControlView(View):
    def __init__(self, ctx):
        super().__init__(timeout=None)
        self.ctx = ctx
        self.guild_id = ctx.guild.id if isinstance(ctx, InteractionContext) else ctx.guild_id
        
        # Создаем кнопки без custom_id
        self.play_pause = Button(
            style=ButtonStyle.primary,
            emoji="⏯️",
            row=0
        )
        self.play_pause.callback = self.play_pause_callback
        
        self.skip = Button(
            style=ButtonStyle.secondary,
            emoji="⏭️",
            row=0
        )
        self.skip.callback = self.skip_callback
        
        self.stop = Button(
            style=ButtonStyle.danger,
            emoji="⏹️",
            row=0
        )
        self.stop.callback = self.stop_callback
        
        self.queue = Button(
            style=ButtonStyle.secondary,
            emoji="📋",
            row=0
        )
        self.queue.callback = self.queue_callback
        
        # Добавляем кнопки к view
        self.add_item(self.play_pause)
        self.add_item(self.skip)
        self.add_item(self.stop)
        self.add_item(self.queue)
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Проверяет, можно ли обработать interaction"""
        try:
            if not interaction.response:
                return False
            if interaction.response.is_done():
                try:
                    await interaction.followup.send("❌ Это взаимодействие уже обработано", ephemeral=True)
                except:
                    pass
                return False
            return True
        except:
            return False

    async def handle_interaction_error(self, interaction: discord.Interaction, message: str):
        """Обрабатывает ошибки взаимодействия"""
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(message, ephemeral=True)
            else:
                try:
                    await interaction.followup.send(message, ephemeral=True)
                except:
                    pass
        except:
            pass

    async def play_pause_callback(self, interaction: discord.Interaction):
        if not await self.interaction_check(interaction):
            return
            
        try:
            guild_state = get_guild_state(self.guild_id)
            
            if not guild_state.voice_client or not guild_state.voice_client.is_connected():
                await self.handle_interaction_error(interaction, "❌ Бот не подключен к голосовому каналу!")
                return
                
            if guild_state.voice_client.is_playing():
                guild_state.voice_client.pause()
                await self.handle_interaction_error(interaction, "⏸️ Воспроизведение приостановлено")
            elif guild_state.voice_client.is_paused():
                guild_state.voice_client.resume()
                await self.handle_interaction_error(interaction, "▶️ Воспроизведение возобновлено")
            else:
                await self.handle_interaction_error(interaction, "❌ Сейчас ничего не играет!")
        except Exception as e:
            logger.error(f"Ошибка в play_pause_callback: {e}")
            await self.handle_interaction_error(interaction, "❌ Произошла ошибка")

    async def skip_callback(self, interaction: discord.Interaction):
        if not await self.interaction_check(interaction):
            return
            
        try:
            guild_state = get_guild_state(self.guild_id)
            
            if not guild_state.voice_client or not guild_state.voice_client.is_connected() or not guild_state.voice_client.is_playing():
                await self.handle_interaction_error(interaction, "❌ Сейчас ничего не играет!")
                return
                
            guild_state.voice_client.stop()
            await self.handle_interaction_error(interaction, "⏭️ Трек пропущен!")
        except Exception as e:
            logger.error(f"Ошибка в skip_callback: {e}")
            await self.handle_interaction_error(interaction, "❌ Произошла ошибка")

    async def stop_callback(self, interaction: discord.Interaction):
        if not await self.interaction_check(interaction):
            return
            
        try:
            guild_state = get_guild_state(self.guild_id)
            
            if not guild_state.voice_client or not guild_state.voice_client.is_connected():
                await self.handle_interaction_error(interaction, "❌ Бот не подключен к голосовому каналу!")
                return
            
            await guild_state.clear_queue()
            guild_state.is_playing = False
            await guild_state.voice_client.disconnect()
            guild_state.update_voice_client(None)
            await self.handle_interaction_error(interaction, "⏹️ Воспроизведение остановлено")
        except Exception as e:
            logger.error(f"Ошибка в stop_callback: {e}")
            await self.handle_interaction_error(interaction, "❌ Произошла ошибка")

    async def queue_callback(self, interaction: discord.Interaction):
        if not await self.interaction_check(interaction):
            return
            
        try:
            guild_state = get_guild_state(self.guild_id)
            
            async with guild_state._lock:
                if not guild_state.queue and not guild_state.current_track:
                    await self.handle_interaction_error(interaction, "❌ Очередь пуста!")
                    return
                
                queue_text = []
                if guild_state.current_track:
                    queue_text.append("🎵 Сейчас играет:\n" + guild_state.current_track[1])
                
                if guild_state.queue:
                    queue_text.append(f"\n📋 В очереди ({len(guild_state.queue)}/{MAX_QUEUE_SIZE}):")
                    for idx, (_, title) in enumerate(guild_state.queue, 1):
                        queue_text.append(f"{idx}. {title}")
                
                full_text = "\n".join(queue_text)
                
                if len(full_text) > 1900:
                    parts = [full_text[i:i+1900] for i in range(0, len(full_text), 1900)]
                    for i, part in enumerate(parts):
                        if i == 0:
                            await self.handle_interaction_error(interaction, f"Очередь (часть {i+1}/{len(parts)}):\n{part}")
                        else:
                            try:
                                await interaction.followup.send(f"Очередь (часть {i+1}/{len(parts)}):\n{part}", ephemeral=True)
                            except:
                                pass
                else:
                    await self.handle_interaction_error(interaction, full_text)
        except Exception as e:
            logger.error(f"Ошибка в queue_callback: {e}")
            await self.handle_interaction_error(interaction, "❌ Произошла ошибка")

@bot.tree.command(name="play", description="Добавляет трек или плейлист в очередь и начинает воспроизведение")
@app_commands.describe(
    query="Ссылка на видео/плейлист или поисковый запрос"
)
async def play_slash(interaction: discord.Interaction, query: str):
    try:
        member = interaction.guild.get_member(interaction.user.id)
        if not member or not member.voice:
            await interaction.response.send_message(
                "❌ Вы должны быть в голосовом канале!", 
                ephemeral=True
            )
            return

        guild_state = get_guild_state(interaction.guild_id)
        guild_state.update_activity()
        
        if guild_state.get_queue_length() >= MAX_QUEUE_SIZE:
            await interaction.response.send_message(
                f"❌ Очередь переполнена! Максимальный размер: {MAX_QUEUE_SIZE} треков",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message("🔍 Ищу трек...")
        
        try:
            is_playlist = 'list=' in query or 'playlist' in query.lower()
            if is_playlist:
                await interaction.edit_original_response(content="🔍 Загружаю плейлист...")
            
            audio_info = await youtube_client.extract_info(query, process_playlist=is_playlist)
            
            if isinstance(audio_info, list):
                tracks_added = await guild_state.add_to_queue(audio_info)
                if tracks_added == 0:
                    await interaction.edit_original_response(
                        content="❌ Не удалось добавить треки: очередь переполнена"
                    )
                else:
                    await interaction.edit_original_response(
                        content=f"📋 Добавлено {tracks_added} треков из плейлиста в очередь!"
                    )
            else:
                tracks_added = await guild_state.add_to_queue(audio_info)
                if tracks_added == 0:
                    await interaction.edit_original_response(
                        content="❌ Не удалось добавить трек: очередь переполнена"
                    )
                else:
                    await interaction.edit_original_response(
                        content=f"🎵 Трек '{audio_info[1]}' добавлен в очередь!"
                    )
            
            if not guild_state.is_playing and tracks_added > 0:
                await play_next(interaction)
                
        except YouTubeAccessError as e:
            await interaction.edit_original_response(
                content=f"🚫 YouTube Error: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Ошибка получения аудио: {e}")
            await interaction.edit_original_response(
                content="❌ Не удалось получить информацию о треке"
            )
            
    except Exception as e:
        logger.error(f"Ошибка в команде play: {e}")
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "❌ Произошла ошибка при обработке команды",
                ephemeral=True
            )
        else:
            await interaction.edit_original_response(
                content="❌ Произошла ошибка при обработке команды"
            )

@bot.tree.command(name="skip", description="Пропускает текущий трек")
async def skip_slash(interaction: discord.Interaction):
    guild_state = get_guild_state(interaction.guild_id)
    guild_state.update_activity()
    
    if not guild_state.voice_client or not guild_state.voice_client.is_connected():
        await interaction.response.send_message(
            "❌ Бот не находится в голосовом канале!",
            ephemeral=True
        )
        return
        
    if not guild_state.voice_client.is_playing():
        await interaction.response.send_message(
            "❌ Сейчас ничего не играет!",
            ephemeral=True
        )
        return
    
    guild_state.voice_client.stop()
    await interaction.response.send_message("⏭️ Трек пропущен!")

@bot.tree.command(name="leave", description="Отключает бота от голосового канала")
async def leave_slash(interaction: discord.Interaction):
    guild_state = get_guild_state(interaction.guild_id)
    
    if not guild_state.voice_client or not guild_state.voice_client.is_connected():
        await interaction.response.send_message(
            "❌ Бот не подключён к голосовому каналу.",
            ephemeral=True
        )
        return
    
    try:
        # Останавливаем воспроизведение
        if guild_state.voice_client.is_playing():
            guild_state.voice_client.stop()
        if hasattr(guild_state.voice_client, 'source'):
            guild_state.voice_client.source = None
            
        # Очищаем очередь
        await guild_state.clear_queue()
        
        # Отключаемся
        await guild_state.voice_client.disconnect(force=True)
        guild_state.update_voice_client(None)
        guild_state.is_playing = False
        guild_state.current_track = None
        
        await interaction.response.send_message("👋 Бот отключился от голосового канала.")
    except Exception as e:
        logger.error(f"Ошибка при отключении: {e}")
        await interaction.response.send_message("❌ Произошла ошибка при отключении.", ephemeral=True)

@bot.tree.command(name="queue", description="Показывает текущую очередь треков")
async def queue_slash(interaction: discord.Interaction):
    guild_state = get_guild_state(interaction.guild_id)
    guild_state.update_activity()
    
    async with guild_state._lock:  # Блокируем для получения консистентного состояния
        if not guild_state.queue and not guild_state.current_track:
            await interaction.response.send_message(
                "❌ Очередь пуста!",
                ephemeral=True
            )
            return
        
        queue_text = []
        if guild_state.current_track:
            queue_text.append("🎵 Сейчас играет:\n" + guild_state.current_track[1])
        
        if guild_state.queue:
            queue_text.append(f"\n📋 В очереди ({len(guild_state.queue)}/{MAX_QUEUE_SIZE}):")
            for idx, (_, title) in enumerate(guild_state.queue, 1):
                queue_text.append(f"{idx}. {title}")
        
        full_text = "\n".join(queue_text)
        
        if len(full_text) > 1900:
            parts = [full_text[i:i+1900] for i in range(0, len(full_text), 1900)]
            for i, part in enumerate(parts):
                if i == 0:
                    await interaction.response.send_message(
                        f"Очередь (часть {i+1}/{len(parts)}):\n{part}",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"Очередь (часть {i+1}/{len(parts)}):\n{part}",
                        ephemeral=True
                    )
        else:
            await interaction.response.send_message(full_text, ephemeral=True)

@bot.tree.command(name="remove", description="Удаляет трек из очереди по индексу")
@app_commands.describe(index="Номер трека в очереди")
async def remove_slash(interaction: discord.Interaction, index: int):
    guild_state = get_guild_state(interaction.guild_id)
    
    if not guild_state.queue:
        await interaction.response.send_message(
            "❌ Очередь пуста!",
            ephemeral=True
        )
        return
        
    if 1 <= index <= len(guild_state.queue):
        try:
            removed = await guild_state.remove_track(index - 1)
            await interaction.response.send_message(
                f"❌ Трек '{removed[1]}' удалён из очереди."
            )
        except Exception as e:
            logger.error(f"Ошибка удаления трека: {str(e)}")
            await interaction.response.send_message(
                "❌ Ошибка при удалении трека",
                ephemeral=True
            )
    else:
        await interaction.response.send_message(
            "❌ Некорректный индекс трека!",
            ephemeral=True
        )

@bot.tree.command(name="clear", description="Очищает очередь воспроизведения")
async def clear_slash(interaction: discord.Interaction):
    guild_state = get_guild_state(interaction.guild_id)
    await guild_state.clear_queue()
    await interaction.response.send_message("🗑️ Очередь очищена!")

@bot.tree.command(name="pause", description="Ставит воспроизведение на паузу или возобновляет его")
async def pause_slash(interaction: discord.Interaction):
    guild_state = get_guild_state(interaction.guild_id)
    
    if not guild_state.voice_client or not guild_state.voice_client.is_connected():
        await interaction.response.send_message(
            "❌ Бот не подключен к голосовому каналу!",
            ephemeral=True
        )
        return
        
    if guild_state.voice_client.is_playing():
        guild_state.voice_client.pause()
        await interaction.response.send_message("⏸️ Воспроизведение приостановлено")
    elif guild_state.voice_client.is_paused():
        guild_state.voice_client.resume()
        await interaction.response.send_message("▶️ Воспроизведение возобновлено")
    else:
        await interaction.response.send_message(
            "❌ Сейчас ничего не играет!",
            ephemeral=True
        )

@bot.tree.command(name="mark", description="Секретная команда")
async def mark_slash(interaction: discord.Interaction):
    rainbow = "🏳️‍🌈"
    await interaction.response.send_message(f"{rainbow} МАРК ГЕЙ {rainbow}")

@bot.event
async def on_voice_state_update(member, before, after):
    if member == bot.user and after.channel is None:
        # Очищаем состояние сервера при отключении
        guild_id = before.channel.guild.id
        guild_state = get_guild_state(guild_id)
        guild_state.clear()
    elif member == bot.user and after.channel:
        # Обновляем voice_client при подключении
        guild_id = after.channel.guild.id
        guild_state = get_guild_state(guild_id)
        guild_state.update_voice_client(after.channel.guild.voice_client)

@bot.tree.command(name="help", description="Показывает список доступных команд")
async def help_slash(interaction: discord.Interaction):
    help_text = """🎵 **Музыкальные команды:**
`/play` - Добавить трек или плейлист в очередь
`/pause` - Приостановить/возобновить воспроизведение
`/skip` - Пропустить текущий трек
`/queue` - Показать очередь воспроизведения
`/remove` - Удалить трек из очереди по номеру
`/clear` - Очистить очередь
`/leave` - Отключить бота от канала

🎮 **Управление:**
• Используйте кнопки под сообщением о текущем треке для быстрого управления
• Бот автоматически отключается после 10 минут бездействия
• Максимальная длина трека - 2 часа
• Максимальный размер очереди - 50 треков"""
    await interaction.response.send_message(help_text)

@bot.tree.command(name="igor", description="Секретная команда для Игоря")
async def igor_slash(interaction: discord.Interaction):
    await interaction.response.send_message("Игорь, отсоси мне 👍")

@bot.tree.command(name="fix", description="Перезагружает и синхронизирует команды бота")
@app_commands.default_permissions(administrator=True)
async def fix_slash(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Эта команда только для администраторов!", ephemeral=True)
        return
        
    try:
        await interaction.response.send_message("🔄 Синхронизирую команды...", ephemeral=True)
        
        # Принудительная синхронизация команд
        commands = await bot.tree.sync()
        
        # Очищаем состояния серверов
        for guild_id, state in list(guild_states.items()):
            try:
                if state.voice_client and state.voice_client.is_connected():
                    await state.voice_client.disconnect()
                state.clear()
            except:
                pass
        guild_states.clear()
        
        # Очищаем кэш
        audio_cache.clear_expired()
        track_cache.clear()
        
        await interaction.edit_original_response(
            content=f"✅ Бот перезагружен! Синхронизировано {len(commands)} команд."
        )
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды fix: {e}")
        await interaction.edit_original_response(
            content="❌ Произошла ошибка при перезагрузке бота"
        )

# Пул для асинхронных HTTP-запросов
class YouTubeClient:
    def __init__(self, max_connections=10):
        self.session = None
        self.executor = ThreadPoolExecutor(max_workers=max_connections)
        self._lock = asyncio.Lock()
        
    async def get_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
        return self.session
        
    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None
        self.executor.shutdown(wait=False)

    async def extract_info(self, url: str, process_playlist: bool = False) -> Union[Tuple[str, str], List[Tuple[str, str]]]:
        """Асинхронное извлечение информации о видео"""
        cache_key = f"{url}_{process_playlist}"
        
        # Проверяем кэш
        cached_data = audio_cache.get(cache_key)
        if cached_data:
            return cached_data
            
        try:
            # Исправляем URL
            if url.startswith('ttps://'):
                url = 'h' + url
                
            if not url.startswith(('http://', 'https://')):
                url = f"ytsearch:{url}"

            ydl_opts = YDL_OPTIONS.copy()
            if process_playlist:
                ydl_opts.update({
                    'extract_flat': 'in_playlist',
                    'playlistend': 50,
                    'playlistreverse': False,
                    'playlist_items': '1-50'
                })

            # Выполняем запрос в отдельном потоке
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                self.executor,
                lambda: youtube_dl.YoutubeDL(ydl_opts).extract_info(url, download=False)
            )

            if not info:
                raise YouTubeAccessError("Не удалось получить информацию о видео")

            # Обработка результатов
            if process_playlist and ('entries' in info or info.get('_type') == 'playlist'):
                result = await self._process_playlist(info, ydl_opts)
            else:
                result = await self._process_video(info, ydl_opts)

            # Сохраняем в кэш
            audio_cache.set(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Ошибка при извлечении информации: {str(e)}")
            raise YouTubeAccessError(str(e))

    async def _process_playlist(self, info: dict, ydl_opts: dict) -> List[Tuple[str, str]]:
        """Обработка плейлиста"""
        entries = info.get('entries', [])
        if not entries:
            raise YouTubeAccessError("Плейлист пуст или недоступен")

        tasks = []
        for entry in entries:
            if not entry or entry.get('availability') in ['private', 'needs_auth']:
                continue

            video_url = entry.get('url') or entry.get('webpage_url')
            if not video_url and entry.get('id'):
                video_url = f"https://www.youtube.com/watch?v={entry['id']}"

            if video_url:
                tasks.append(self._process_video_entry(video_url, ydl_opts))

        # Выполняем запросы параллельно с ограничением
        results = []
        chunk_size = 5  # Обрабатываем по 5 видео одновременно
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            chunk_results = await asyncio.gather(*chunk, return_exceptions=True)
            results.extend(r for r in chunk_results if isinstance(r, tuple))

        if not results:
            raise YouTubeAccessError("В плейлисте нет доступных треков")
        return results

    async def _process_video_entry(self, url: str, ydl_opts: dict) -> Optional[Tuple[str, str]]:
        """Обработка отдельного видео из плейлиста"""
        try:
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                self.executor,
                lambda: youtube_dl.YoutubeDL(ydl_opts).extract_info(url, download=False)
            )

            if not info or info.get('duration', 0) > 7200:
                return None

            audio_url = get_best_audio_format(info.get('formats', []))
            if not audio_url:
                return None

            title = info.get('title', 'Без названия')
            return (audio_url, title)

        except Exception as e:
            logger.warning(f"Ошибка обработки видео {url}: {str(e)}")
            return None

    async def _process_video(self, info: dict, ydl_opts: dict) -> Tuple[str, str]:
        """Обработка одиночного видео"""
        if info.get('is_live'):
            raise YouTubeAccessError("Лайв-стримы не поддерживаются")

        if info.get('duration', 0) > 7200:
            raise YouTubeAccessError("Видео слишком длинное (максимум 2 часа)")

        audio_url = get_best_audio_format(info.get('formats', []))
        if not audio_url:
            raise YouTubeAccessError("Не найдены аудио форматы")

        title = info.get('title', 'Без названия')
        return (audio_url, title)

# Создаем глобальный экземпляр клиента
youtube_client = YouTubeClient()

if __name__ == "__main__":
    try:
        check_cookies()
        check_disk_space()  # Проверяем место перед запуском
        logger.info("Запуск бота с валидными cookies...")
        
        # Создаем и запускаем бота
        bot.run(os.getenv("DISCORD_TOKEN"))
        
    except YouTubeAccessError as e:
        logger.critical(str(e))
        print(f"CRITICAL ERROR: {str(e)}")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        print(f"CRITICAL ERROR: {str(e)}")

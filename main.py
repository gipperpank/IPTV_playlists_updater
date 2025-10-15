import requests
import time
import json
import os
import logging
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import urllib3
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class IPTVManager:
    def __init__(self, config_file: str = "config.json"):
        self.config = self.load_config(config_file)
        self.setup_logging()
        self.channels_db = {}
        
        self.setup_session()
        
    def setup_session(self):
        """Настройка HTTP сессии с пулом соединений"""
        self.session = requests.Session()
        
        # Настройка повторных попыток - уменьшаем для тихого режима
        retry_strategy = Retry(
            total=2 if self.config.get('quiet_mode', True) else 3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=0.5 if self.config.get('quiet_mode', True) else 1
        )
        
        # Настройка адаптера с пулом соединений
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=self.config.get('pool_connections', 20),
            pool_maxsize=self.config.get('pool_maxsize', 30),
            pool_block=self.config.get('pool_block', False)
        )
        
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.session.verify = self.config.get('verify_ssl', False)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })

    def load_config(self, config_file: str) -> Dict:
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            default_config = {
                "update_frequency_hours": 6,
                "sorting": "ping",
                "enable_logging": True,
                "max_concurrent_checks": 20,
                "timeout_seconds": 8,
                "request_timeout": 10,
                "output_playlist": "local_playlist.m3u",
                "enable_ping_check": True,
                "enable_availability_check": True,
                "verify_ssl": False,
                "backup_playlists": True,
                "max_backup_files": 5,
                "clean_html_files": True,
                "remove_duplicates": True,
                "min_channel_name_length": 3,
                
                # Новые настройки для тихого режима
                "quiet_mode": True,
                "log_level": "INFO",
                "show_progress": True,
                "log_errors_only": False,
                
                # Настройки пула соединений
                "pool_connections": 30,
                "pool_maxsize": 50,
                "pool_block": False,
                "connection_timeout": 3,
                "read_timeout": 5,
                
                # Настройки для проблемных доменов
                "problematic_domains_timeout": 8,
                "problematic_domains": [
                    "rutube.ru", "youtube.com", "vk.com", 
                    "mail.ru", "rambler.ru", "ok.ru"
                ]
            }
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4, ensure_ascii=False)
            return default_config

    def setup_logging(self):
        """Настройка логирования с учетом quiet_mode"""
        if self.config.get('enable_logging', True):
            os.makedirs('logs', exist_ok=True)
            
            from logging.handlers import RotatingFileHandler
            
            # Уровень логирования для файла
            file_log_level = logging.DEBUG if not self.config.get('log_errors_only', False) else logging.INFO
            
            log_handler = RotatingFileHandler(
                'logs/iptv_manager.log', 
                maxBytes=10*1024*1024,
                backupCount=5,
                encoding='utf-8'
            )
            log_handler.setLevel(file_log_level)
            
            # Уровень логирования для консоли
            console_log_level = self.get_console_log_level()
            
            # Форматтер
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            log_handler.setFormatter(formatter)
            
            # Настройка корневого логгера
            logging.basicConfig(
                level=logging.DEBUG,  # Минимальный уровень для обработчиков
                handlers=[log_handler]
            )
            
            # Добавляем консольный обработчик если не включен тихий режим
            if not self.config.get('log_errors_only', False):
                console_handler = logging.StreamHandler()
                console_handler.setLevel(console_log_level)
                console_handler.setFormatter(formatter)
                logging.getLogger().addHandler(console_handler)
            
        else:
            logging.basicConfig(level=logging.WARNING)
        
        self.logger = logging.getLogger(__name__)

    def get_console_log_level(self):
        """Определение уровня логирования для консоли"""
        if self.config.get('log_errors_only', False):
            return logging.ERROR
        elif self.config.get('quiet_mode', True):
            return logging.WARNING
        else:
            log_level = self.config.get('log_level', 'INFO')
            return getattr(logging, log_level.upper())

    def get_request_timeout(self, url: str) -> tuple:
        """Определение таймаута для конкретного URL"""
        base_timeout = (
            self.config.get('connection_timeout', 3),
            self.config.get('read_timeout', 5)
        )
        
        # Увеличиваем таймаут для проблемных доменов
        problematic_domains = self.config.get('problematic_domains', [])
        for domain in problematic_domains:
            if domain in url:
                problematic_timeout = self.config.get('problematic_domains_timeout', 8)
                return (problematic_timeout, problematic_timeout)
        
        return base_timeout

    def clean_html_content(self, content: str) -> str:
        """Очистка HTML и извлечение M3U контента"""
        if content.strip().startswith(('#EXTM3U', '#EXTINF:')):
            return content
        
        self.logger.info("Обнаружен HTML контент, начинаем очистку...")
        
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL)
        content = re.sub(r'<[^>]+>', '', content)
        
        content = '\n'.join([line.strip() for line in content.split('\n') if line.strip()])
        
        lines = content.split('\n')
        m3u_lines = []
        in_m3u_section = False
        
        for line in lines:
            if line.startswith('#EXTM3U'):
                in_m3u_section = True
                m3u_lines.append(line)
            elif line.startswith('#EXTINF:'):
                in_m3u_section = True
                m3u_lines.append(line)
            elif in_m3u_section and line.startswith('http'):
                m3u_lines.append(line)
            elif in_m3u_section and line.strip() == '':
                if m3u_lines and not m3u_lines[-1].startswith('http'):
                    m3u_lines.append(line)
            elif in_m3u_section and not line.startswith('#'):
                if not line.startswith('http'):
                    in_m3u_section = False
        
        if m3u_lines:
            result = '\n'.join(m3u_lines)
            self.logger.info(f"Извлечено {len(m3u_lines)} M3U строк из HTML")
            return result
        
        self.logger.warning("Не удалось извлечь M3U данные из HTML")
        return ""

    def download_playlist(self, url: str) -> str:
        """Умная загрузка плейлиста с тихим режимом"""
        max_retries = 1 if self.config.get('quiet_mode', True) else 2
        
        for attempt in range(max_retries):
            try:
                if not self.config.get('quiet_mode', True):
                    self.logger.info(f"Загрузка плейлиста: {url}")
                
                timeout = self.get_request_timeout(url)
                response = self.session.get(
                    url, 
                    timeout=timeout,
                    verify=self.config.get('verify_ssl', False)
                )
                
                if response.status_code == 200:
                    content = response.text
                    
                    if (self.config.get('clean_html_files', True) and 
                        ('<!DOCTYPE html>' in content or '<html' in content)):
                        
                        clean_content = self.clean_html_content(content)
                        if clean_content and '#EXTINF:' in clean_content:
                            content = clean_content
                            if not self.config.get('quiet_mode', True):
                                self.logger.info("HTML контент успешно очищен")
                        else:
                            self.logger.warning("Не удалось очистить HTML или не найдены M3U данные")
                            continue
                    
                    if '#EXTINF:' not in content:
                        self.logger.warning(f"Контент не содержит M3U данных: {url}")
                        continue
                    
                    temp_dir = "temp"
                    os.makedirs(temp_dir, exist_ok=True)
                    
                    filename = hashlib.md5(url.encode()).hexdigest() + ".m3u"
                    filepath = os.path.join(temp_dir, filename)
                    
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    
                    channel_count = content.count('#EXTINF:')
                    self.logger.info(f"Загружен: {url} ({channel_count} каналов)")
                    return filepath
                    
                else:
                    if not self.config.get('quiet_mode', True):
                        self.logger.warning(f"HTTP ошибка {response.status_code} для {url}")
                    
            except requests.exceptions.Timeout:
                if not self.config.get('quiet_mode', True):
                    self.logger.warning(f"Таймаут при загрузке {url}")
            except requests.exceptions.ConnectionError as e:
                if not self.config.get('quiet_mode', True):
                    self.logger.warning(f"Ошибка соединения с {url}")
            except Exception as e:
                if not self.config.get('quiet_mode', True):
                    self.logger.warning(f"Ошибка загрузки {url}: {str(e)}")
                
            if attempt < max_retries - 1:
                time.sleep(1)  # Короткая пауза между попытками
        
        self.logger.error(f"Не удалось загрузить: {url}")
        return None

    def parse_playlist(self, filepath: str) -> List[Dict]:
        """Парсинг M3U плейлиста"""
        channels = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = [line.strip() for line in content.split('\n') if line.strip()]
            
            i = 0
            channel_count = 0
            while i < len(lines):
                line = lines[i]
                
                if line.startswith('#EXTINF:'):
                    extinf_line = line
                    
                    i += 1
                    url_line = None
                    while i < len(lines) and (not lines[i] or lines[i].startswith('#')):
                        i += 1
                    
                    if i < len(lines) and lines[i].startswith(('http://', 'https://', 'rtmp://', 'rtsp://')):
                        url_line = lines[i]
                    
                    if url_line:
                        name = self.extract_field(extinf_line, 'tvg-name') or self.extract_name(extinf_line)
                        group = self.extract_field(extinf_line, 'group-title') or 'Other'
                        logo = self.extract_field(extinf_line, 'tvg-logo') or ''
                        
                        min_length = self.config.get('min_channel_name_length', 2)
                        if not name or len(name.strip()) < min_length:
                            name = f"Channel_{channel_count}"
                        
                        channel = {
                            'name': name.strip(),
                            'url': url_line.strip(),
                            'group': group.strip(),
                            'logo': logo.strip(),
                            'source_file': os.path.basename(filepath)
                        }
                        
                        channels.append(channel)
                        channel_count += 1
                
                i += 1
                
            self.logger.info(f"Извлечено {len(channels)} каналов")
            return channels
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга {filepath}: {str(e)}")
            return []

    def extract_field(self, extinf: str, field: str) -> str:
        """Извлечение поля из EXTINF строки"""
        try:
            pattern = f'{field}="([^"]*)"'
            match = re.search(pattern, extinf)
            if match:
                return match.group(1)
        except:
            pass
        return ""

    def extract_name(self, extinf: str) -> str:
        """Извлечение имени канала из EXTINF"""
        try:
            last_comma = extinf.rfind(',')
            if last_comma != -1:
                name = extinf[last_comma + 1:].strip()
                name = re.sub(r'^["\']|["\']$', '', name)
                return name
        except:
            pass
        return ""

    def check_channel_availability(self, channel: Dict) -> tuple:
        """Проверка доступности канала в тихом режиме"""
        url = channel['url']
        
        if not self.config.get('enable_availability_check', True):
            return channel, 0.0, True
        
        try:
            response_time = float('inf')
            available = False
            
            start_time = time.time()
            try:
                timeout = self.get_request_timeout(url)
                response = self.session.head(
                    url,
                    timeout=timeout,
                    allow_redirects=True,
                    verify=self.config.get('verify_ssl', False)
                )
                available = response.status_code in [200, 302, 301, 307]
                response_time = (time.time() - start_time) * 1000
                
            except Exception:
                available = False
            
            return channel, response_time, available
            
        except Exception:
            return channel, float('inf'), False

    def process_playlists(self):
        """Основной процесс обработки плейлистов"""
        playlist_urls = self.load_playlist_urls()
        
        if not playlist_urls:
            self.logger.error("Нет URL плейлистов для обработки")
            return
        
        self.logger.info(f"Начинаем обработку {len(playlist_urls)} плейлистов...")
        
        downloaded_files = []
        for url in playlist_urls:
            filepath = self.download_playlist(url)
            if filepath and os.path.exists(filepath):
                downloaded_files.append(filepath)
        
        all_channels = []
        for filepath in downloaded_files:
            channels = self.parse_playlist(filepath)
            all_channels.extend(channels)
        
        if not all_channels:
            self.logger.error("Не найдено каналов для обработки")
            return
        
        self.logger.info(f"Найдено {len(all_channels)} каналов, проверка доступности...")
        
        max_workers = self.config.get('max_concurrent_checks', 20)
        successful_channels = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.check_channel_availability, channel) 
                      for channel in all_channels]
            
            for future in as_completed(futures):
                channel, ping, available = future.result()
                if available:
                    self.update_channel_db(channel, ping)
                    successful_channels += 1
                
                # Прогресс-отчет только если включен
                if self.config.get('show_progress', True) and successful_channels % 100 == 0:
                    self.logger.info(f"Проверено {successful_channels} рабочих каналов")
        
        self.logger.info(f"Обработка завершена: {successful_channels} рабочих каналов из {len(all_channels)}")

    def update_channel_db(self, channel: Dict, ping: float):
        """Обновление базы каналов"""
        channel_key = channel['name'].lower().strip()
        
        if self.config.get('remove_duplicates', True):
            if channel_key in self.channels_db:
                if ping < self.channels_db[channel_key]['ping']:
                    self.channels_db[channel_key] = {
                        'name': channel['name'],
                        'url': channel['url'],
                        'group': channel['group'],
                        'logo': channel['logo'],
                        'ping': ping,
                        'source': channel.get('source_file', 'unknown')
                    }
            else:
                self.channels_db[channel_key] = {
                    'name': channel['name'],
                    'url': channel['url'],
                    'group': channel['group'],
                    'logo': channel['logo'],
                    'ping': ping,
                    'source': channel.get('source_file', 'unknown')
                }

    def load_playlist_urls(self) -> List[str]:
        """Загрузка списка плейлистов"""
        urls = []
        try:
            with open('playlists.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        urls.append(line)
            self.logger.info(f"Загружено {len(urls)} URL плейлистов")
        except FileNotFoundError:
            self.logger.error("Файл playlists.txt не найден")
        return urls

    def generate_playlist(self):
        """Генерация финального плейлиста"""
        if not self.channels_db:
            self.logger.error("Нет каналов для генерации плейлиста")
            return
        
        try:
            channels = list(self.channels_db.values())
            sorted_channels = self.sort_channels(channels)
            
            if self.config.get('backup_playlists', True):
                self.backup_playlist()
            
            output_file = self.config.get('output_playlist', 'local_playlist.m3u')
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('#EXTM3U\n')
                f.write(f'# Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
                f.write(f'# Total channels: {len(sorted_channels)}\n')
                f.write(f'# Average ping: {self.calculate_average_ping(sorted_channels):.2f}ms\n\n')
                
                current_group = ""
                for channel in sorted_channels:
                    if channel['group'] != current_group:
                        if current_group != "":
                            f.write('\n')
                        f.write(f'#GROUP: {channel["group"]}\n')
                        current_group = channel['group']
                    
                    f.write(f'#EXTINF:-1 tvg-id="{channel["name"]}" ')
                    f.write(f'tvg-name="{channel["name"]}" ')
                    f.write(f'tvg-logo="{channel["logo"]}" ')
                    f.write(f'group-title="{channel["group"]}",{channel["name"]}\n')
                    f.write(f'{channel["url"]}\n')
            
            self.logger.info(f"Плейлист создан: {output_file} ({len(sorted_channels)} каналов)")
            
            self.generate_stats(sorted_channels)
            
        except Exception as e:
            self.logger.error(f"Ошибка генерации плейлиста: {str(e)}")

    def calculate_average_ping(self, channels: List[Dict]) -> float:
        if not channels:
            return 0.0
        return sum(channel['ping'] for channel in channels) / len(channels)

    def sort_channels(self, channels: List[Dict]) -> List[Dict]:
        method = self.config.get('sorting', 'ping')
        
        if method == 'ping':
            return sorted(channels, key=lambda x: x['ping'])
        elif method == 'name':
            return sorted(channels, key=lambda x: x['name'].lower())
        elif method == 'group':
            return sorted(channels, key=lambda x: (x['group'], x['name'].lower()))
        else:
            return sorted(channels, key=lambda x: x['ping'])

    def backup_playlist(self):
        try:
            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(backup_dir, f"playlist_backup_{timestamp}.m3u")
            
            output_file = self.config.get('output_playlist', 'local_playlist.m3u')
            if os.path.exists(output_file):
                import shutil
                shutil.copy2(output_file, backup_file)
                self.cleanup_old_backups(backup_dir)
                
        except Exception as e:
            self.logger.warning(f"Не удалось создать бэкап: {str(e)}")

    def cleanup_old_backups(self, backup_dir: str):
        try:
            max_backups = self.config.get('max_backup_files', 5)
            files = [os.path.join(backup_dir, f) for f in os.listdir(backup_dir) 
                    if f.startswith('playlist_backup_') and f.endswith('.m3u')]
            
            files.sort(key=os.path.getmtime)
            
            while len(files) > max_backups:
                old_file = files.pop(0)
                os.remove(old_file)
                
        except Exception as e:
            self.logger.warning(f"Ошибка очистки бэкапов: {str(e)}")

    def generate_stats(self, channels: List[Dict]):
        try:
            stats = {
                'total_channels': len(channels),
                'groups': {},
                'average_ping': self.calculate_average_ping(channels),
                'min_ping': min(channel['ping'] for channel in channels) if channels else 0,
                'max_ping': max(channel['ping'] for channel in channels) if channels else 0,
                'generated': datetime.now().isoformat(),
                'sources': {}
            }
            
            for channel in channels:
                group = channel['group']
                source = channel.get('source', 'unknown')
                
                stats['groups'][group] = stats['groups'].get(group, 0) + 1
                stats['sources'][source] = stats['sources'].get(source, 0) + 1
            
            with open('stats.json', 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Статистика: {len(channels)} каналов, {len(stats['groups'])} групп")
            
        except Exception as e:
            self.logger.warning(f"Не удалось сохранить статистику: {str(e)}")

    def cleanup_resources(self):
        try:
            self.session.close()
        except Exception:
            pass

    def run(self):
        self.logger.info("=== Запуск IPTV Manager ===")
        
        try:
            while True:
                start_time = time.time()
                
                self.process_playlists()
                self.generate_playlist()
                
                duration = time.time() - start_time
                self.logger.info(f"Цикл завершен за {duration:.1f} секунд")
                
                frequency = self.config.get('update_frequency_hours', 6)
                self.logger.info(f"Ожидание {frequency} часов...")
                time.sleep(frequency * 3600)
                
        except KeyboardInterrupt:
            self.logger.info("Остановлено пользователем")
        except Exception as e:
            self.logger.error(f"Ошибка: {str(e)}")
            time.sleep(60)
        finally:
            self.cleanup_resources()

def main():
    os.makedirs('temp', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    os.makedirs('backups', exist_ok=True)
    
    manager = IPTVManager()
    
    try:
        manager.run()
    except Exception as e:
        manager.logger.error(f"Фатальная ошибка: {str(e)}")
    finally:
        manager.logger.info("Работа завершена")

if __name__ == "__main__":
    main()
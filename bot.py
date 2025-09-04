import os
import yaml
import asyncio
import logging
import re
import time
import sqlite3
import random
import psutil
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from telethon import TelegramClient, events
from telethon.tl.types import PeerChannel, MessageMediaWebPage
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.errors import FloodWaitError
from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QWidget, QTabWidget,
                             QLineEdit, QLabel, QPushButton, QTextEdit, QCheckBox, QSpinBox,
                             QListWidget, QListWidgetItem, QMessageBox, QFileDialog, QHBoxLayout,
                             QComboBox, QDialog, QDialogButtonBox, QFormLayout, QProgressBar)
from PyQt5.QtCore import QThread, pyqtSignal, Qt, QMutex, QTimer
from PyQt5.QtGui import QFont, QColor, QPalette

# Настройка ротации логов
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler("bot.log", maxBytes=10 * 1024 * 1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

db_lock = QMutex()


class FloodWaitDialog(QDialog):
    def __init__(self, wait_time, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Flood Wait Required")
        self.setModal(True)
        self.wait_time = wait_time

        layout = QVBoxLayout()

        self.message = QLabel(f"Telegram requires waiting {wait_time} seconds before next attempt.")
        self.progress = QProgressBar()
        self.progress.setMaximum(wait_time)
        self.progress.setValue(0)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)

        layout.addWidget(self.message)
        layout.addWidget(self.progress)
        layout.addWidget(self.cancel_btn)

        self.setLayout(layout)
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_progress)
        self.timer.start(1000)

    def update_progress(self):
        current = self.progress.value() + 1
        self.progress.setValue(current)
        if current >= self.progress.maximum():
            self.timer.stop()
            self.accept()

    def closeEvent(self, event):
        self.timer.stop()
        super().closeEvent(event)


class DarkTheme:
    @staticmethod
    def apply(app):
        app.setStyle("Fusion")
        dark_palette = QPalette()
        dark_palette.setColor(QPalette.Window, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.WindowText, QColor(255, 255, 255))
        dark_palette.setColor(QPalette.Base, QColor(35, 35, 35))
        dark_palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ToolTipBase, QColor(25, 25, 25))
        dark_palette.setColor(QPalette.ToolTipText, QColor(255, 255, 255))
        dark_palette.setColor(QPalette.Text, QColor(255, 255, 255))
        dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ButtonText, QColor(255, 255, 255))
        dark_palette.setColor(QPalette.BrightText, QColor(255, 0, 0))
        dark_palette.setColor(QPalette.Link, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
        dark_palette.setColor(QPalette.HighlightedText, QColor(35, 35, 35))
        dark_palette.setColor(QPalette.Disabled, QPalette.Text, QColor(127, 127, 127))
        dark_palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor(127, 127, 127))

        app.setPalette(dark_palette)
        app.setStyleSheet("""
            QToolTip { color: #ffffff; background-color: #2a82da; border: 1px solid white; }
            QTabWidget::pane { border: 1px solid #444; }
            QTabBar::tab { padding: 8px; background: #353535; color: white; }
            QTabBar::tab:selected { background: #2a82da; }
            QPushButton { background: #353535; border: 1px solid #555; padding: 5px; min-width: 80px; color: white; }
            QPushButton:hover { background: #454545; }
            QPushButton:pressed { background: #2a82da; }
            QLineEdit, QTextEdit, QSpinBox { background: #252525; border: 1px solid #444; padding: 3px; color: white; }
            QListWidget { background: #252525; border: 1px solid #444; color: white; }
            QCheckBox { color: white; spacing: 5px; }
            QCheckBox::indicator { width: 16px; height: 16px; }
            QComboBox { background: #252525; border: 1px solid #444; padding: 3px; color: white; min-height: 20px; }
            QComboBox::drop-down { border: 0px; }
            QComboBox QAbstractItemView { background: #252525; color: white; selection-background-color: #2a82da; }
        """)


class StyledLabel(QLabel):
    def __init__(self, text, bold=False, font_size=9):
        super().__init__(text)
        font = QFont()
        font.setPointSize(font_size)
        if bold:
            font.setBold(True)
        self.setFont(font)


class StyledButton(QPushButton):
    def __init__(self, text, min_width=80):
        super().__init__(text)
        self.setMinimumWidth(min_width)
        self.setStyleSheet("""
            QPushButton {
                padding: 6px;
                border-radius: 3px;
            }
        """)


class ResourceMonitor(QThread):
    update_signal = pyqtSignal(str, float)

    def __init__(self):
        super().__init__()
        self._is_running = True

    def run(self):
        while self._is_running:
            try:
                process = psutil.Process()
                memory_usage = process.memory_info().rss / 1024 / 1024
                self.update_signal.emit("memory", memory_usage)
                cpu_percent = process.cpu_percent(interval=1)
                self.update_signal.emit("cpu", cpu_percent)
                time.sleep(5)
            except Exception as e:
                logging.error(f"Resource monitoring error: {e}")
                time.sleep(5)

    def stop(self):
        self._is_running = False


class HumanActivitySimulator:
    def __init__(self):
        self.last_activity_time = 0
        self.visited_entities = set()

    async def simulate_human_activity(self, client, source_entities):
        if not source_entities:
            return
        if random.random() < 0.4:
            activity_type = random.choice(['scroll_chat', 'view_profile', 'read_channel'])
            try:
                if activity_type == 'scroll_chat':
                    await self.simulate_chat_scroll(client, source_entities)
                elif activity_type == 'view_profile':
                    await self.simulate_profile_view(client, source_entities)
                elif activity_type == 'read_channel':
                    await self.simulate_channel_reading(client, source_entities)
                await asyncio.sleep(random.uniform(2, 8))
            except Exception:
                pass

    async def simulate_chat_scroll(self, client, source_entities):
        if not source_entities:
            return
        entity = random.choice(list(source_entities.values()))
        try:
            messages = await client(GetHistoryRequest(
                peer=entity,
                limit=random.randint(5, 15),
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=random.randint(0, 50),
                hash=0
            ))
            await asyncio.sleep(random.uniform(3, 12))
        except Exception:
            pass

    async def simulate_profile_view(self, client, source_entities):
        if not source_entities:
            return
        entity = random.choice(list(source_entities.values()))
        try:
            if hasattr(entity, 'username') and entity.username:
                await client(GetFullUserRequest(entity))
                await asyncio.sleep(random.uniform(2, 6))
            elif hasattr(entity, 'title'):
                await client(GetFullChannelRequest(entity))
                await asyncio.sleep(random.uniform(3, 8))
        except Exception:
            pass

    async def simulate_channel_reading(self, client, source_entities):
        if not source_entities:
            return
        entity = random.choice(list(source_entities.values()))
        try:
            messages = await client.get_messages(entity, limit=random.randint(3, 8))
            read_time = len(messages) * random.uniform(1.5, 4.0)
            await asyncio.sleep(read_time)
        except Exception:
            pass


class BotThread(QThread):
    log_signal = pyqtSignal(str)
    auth_required_signal = pyqtSignal(bool)
    resource_signal = pyqtSignal(str, float)
    flood_wait_signal = pyqtSignal(int)

    def __init__(self, config, parent_window):
        super().__init__()
        self.config = config
        self._is_running = True
        self.client = None
        self.processed_group_ids = set()
        self.processed_chat_ids = set()
        self.MAX_CAPTION_LENGTH = 1000
        self.MAX_FILE_SIZE = 50 * 1024 * 1024
        self.db_conn = None
        self.db_path = 'forward_bot.db'
        self.source_entities = {}
        self.activity_simulator = HumanActivitySimulator()
        self.parent_window = parent_window
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.update_signal.connect(self.resource_signal)
        self.connection_attempts = 0
        self.max_connection_attempts = 5
        self.loop = None
        self.last_code_request = None

    def init_db(self):
        try:
            db_lock.lock()
            if self.db_conn:
                try:
                    self.db_conn.close()
                except:
                    pass
            self.db_conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
            self.db_conn.execute("PRAGMA journal_mode=WAL")
            cur = self.db_conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS forwarded_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    group_source TEXT NOT NULL,
                    grouped_id INTEGER,
                    forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(message_id, chat_id, group_source)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    date TEXT PRIMARY KEY,
                    forwards_count INTEGER NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS last_processed (
                    group_source TEXT PRIMARY KEY,
                    last_message_id INTEGER NOT NULL
                )
            """)
            cur.execute("DELETE FROM forwarded_messages WHERE forwarded_at < datetime('now', '-30 days')")
            cur.execute("DELETE FROM daily_stats WHERE date < datetime('now', '-90 days')")
            self.db_conn.commit()
            self.log_signal.emit("Database initialized successfully")
        except Exception as e:
            self.log_signal.emit(f"Error initializing database: {str(e)}")
            raise
        finally:
            db_lock.unlock()

    def get_last_processed_id(self, group_source):
        try:
            db_lock.lock()
            cur = self.db_conn.cursor()
            cur.execute("SELECT last_message_id FROM last_processed WHERE group_source = ?", (group_source,))
            result = cur.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.log_signal.emit(f"Error getting last processed ID: {str(e)}")
            return None
        finally:
            db_lock.unlock()

    def save_last_processed_id(self, group_source, message_id):
        try:
            db_lock.lock()
            cur = self.db_conn.cursor()
            cur.execute("INSERT OR REPLACE INTO last_processed (group_source, last_message_id) VALUES (?, ?)",
                        (group_source, message_id))
            self.db_conn.commit()
        except Exception as e:
            self.log_signal.emit(f"Error saving last processed ID: {str(e)}")
            raise
        finally:
            db_lock.unlock()

    def is_message_forwarded(self, message_id, chat_id, group_source, grouped_id=None):
        try:
            db_lock.lock()
            cur = self.db_conn.cursor()
            if grouped_id:
                cur.execute("SELECT 1 FROM forwarded_messages WHERE grouped_id=? AND chat_id=? AND group_source=?",
                            (grouped_id, chat_id, group_source))
                if cur.fetchone() is not None:
                    return True
            cur.execute("SELECT 1 FROM forwarded_messages WHERE message_id=? AND chat_id=? AND group_source=?",
                        (message_id, chat_id, group_source))
            return cur.fetchone() is not None
        except Exception as e:
            self.log_signal.emit(f"Error checking message: {str(e)}")
            return True
        finally:
            db_lock.unlock()

    def save_forwarded_message(self, message_id, chat_id, group_source, grouped_id=None):
        try:
            db_lock.lock()
            cur = self.db_conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO forwarded_messages (message_id, chat_id, group_source, grouped_id) VALUES (?, ?, ?, ?)",
                (message_id, chat_id, group_source, grouped_id))
            today = time.strftime("%Y-%m-%d")
            cur.execute("INSERT OR IGNORE INTO daily_stats (date, forwards_count) VALUES (?, 0)", (today,))
            cur.execute("UPDATE daily_stats SET forwards_count = forwards_count + 1 WHERE date = ?", (today,))
            self.db_conn.commit()
        except Exception as e:
            self.log_signal.emit(f"Error saving message: {str(e)}")
            raise
        finally:
            db_lock.unlock()

    def check_daily_limit(self):
        try:
            db_lock.lock()
            cur = self.db_conn.cursor()
            today = time.strftime("%Y-%m-%d")
            cur.execute("SELECT forwards_count FROM daily_stats WHERE date = ?", (today,))
            result = cur.fetchone()
            count = result[0] if result else 0
            max_forwards = self.config["telegram"].get("max_daily_forwards", 200)
            return count >= max_forwards
        except Exception as e:
            self.log_signal.emit(f"Error checking daily limit: {str(e)}")
            return False
        finally:
            db_lock.unlock()

    async def resolve_entity(self, entity_id):
        try:
            if entity_id not in self.source_entities:
                try:
                    if entity_id.startswith('@'):
                        entity = await self.client.get_entity(entity_id)
                    elif entity_id.startswith('-100'):
                        entity = await self.client.get_entity(int(entity_id))
                    elif entity_id.startswith('https://t.me/'):
                        entity = await self.client.get_entity(entity_id)
                    else:
                        try:
                            entity = await self.client.get_entity(int(entity_id))
                        except ValueError:
                            entity = await self.client.get_entity(entity_id)
                    self.source_entities[entity_id] = entity
                except Exception as e:
                    self.log_signal.emit(f"Error resolving entity {entity_id}: {str(e)}")
                    return None
            return self.source_entities[entity_id]
        except Exception as e:
            self.log_signal.emit(f"Error resolving entity {entity_id}: {str(e)}")
            return None

    async def safe_forward(self, message, target_entity, max_retries=3):
        for attempt in range(max_retries):
            try:
                if await self.is_chat_protected(message.chat_id):
                    text_to_send = message.text[:self.MAX_CAPTION_LENGTH] if message.text else ""
                    await self.client.send_message(target_entity, text_to_send)
                else:
                    await message.forward_to(target_entity)
                return True
            except (ConnectionError, TimeoutError) as e:
                if attempt == max_retries - 1:
                    raise
                delay = (attempt + 1) * 5
                self.log_signal.emit(f"Network error, retrying in {delay} seconds...")
                await asyncio.sleep(delay)
            except Exception as e:
                self.log_signal.emit(f"Forward error: {str(e)}")
                return False
        return False

    async def check_channel_access(self, entity_id, is_target=False):
        try:
            entity = await self.resolve_entity(entity_id)
            if not entity:
                return False, f"Failed to resolve entity {entity_id}"
            if is_target:
                try:
                    test_msg = await self.client.send_message(entity, "test message")
                    await test_msg.delete()
                except Exception as e:
                    return False, f"No write access to target {entity_id}: {str(e)}"
            else:
                try:
                    async for message in self.client.iter_messages(entity, limit=1):
                        pass
                except Exception as e:
                    return False, f"No read access to source {entity_id}: {str(e)}"
            return True, "Access confirmed"
        except Exception as e:
            return False, f"Access check error for {entity_id}: {str(e)}"

    async def parse_history_messages(self):
        self.log_signal.emit("Starting history parsing...")
        for group in self.config["groups"]:
            try:
                source = group["source"]
                target = group["target"]
                self.log_signal.emit(f"Parsing history for group {source} -> {target}")
                access, msg = await self.check_channel_access(source, False)
                if not access:
                    self.log_signal.emit(f"Access error for source {source}: {msg}")
                    continue
                access, msg = await self.check_channel_access(target, True)
                if not access:
                    self.log_signal.emit(f"Access error for target {target}: {msg}")
                    continue
                source_entity = await self.resolve_entity(source)
                if not source_entity:
                    self.log_signal.emit(f"Failed to resolve source entity {source}")
                    continue
                last_processed_id = self.get_last_processed_id(source)
                if last_processed_id:
                    self.log_signal.emit(f"Resuming from message ID {last_processed_id}")
                else:
                    self.log_signal.emit("No previous progress found, starting from beginning")
                self.log_signal.emit(f"Successfully accessed source channel {source}")
                message_count = 0
                last_message_id = None
                kwargs = {'entity': source_entity, 'reverse': True, 'limit': None}
                if last_processed_id:
                    kwargs['min_id'] = last_processed_id
                async for message in self.client.iter_messages(**kwargs):
                    if not self._is_running:
                        self.log_signal.emit("Parsing stopped by user")
                        break
                    if random.random() < 0.3:
                        await self.activity_simulator.simulate_human_activity(self.client, self.source_entities)
                    await self.process_message(message, group)
                    message_count += 1
                    last_message_id = message.id
                    if message_count % 10 == 0:
                        self.save_last_processed_id(source, last_message_id)
                        self.log_signal.emit(
                            f"Processed {message_count} messages from {source} (last ID: {last_message_id})")
                    if self.check_daily_limit():
                        self.log_signal.emit("Daily forward limit reached")
                        break
                    base_delay = self.config["telegram"].get("safety_delay", 3.0)
                    random_delay = max(1.0, base_delay * random.uniform(0.5, 1.5))
                    await asyncio.sleep(random_delay)
                if last_message_id:
                    self.save_last_processed_id(source, last_message_id)
                self.log_signal.emit(f"Finished parsing {message_count} messages from {source}")
            except Exception as e:
                self.log_signal.emit(f"Error parsing history for group {source}: {str(e)}")
                continue
        self.log_signal.emit("History parsing completed")

    async def process_message(self, message, group_config):
        try:
            if not self._is_running or self.check_daily_limit() or not message:
                return
            try:
                chat_id = message.chat_id if hasattr(message, 'chat_id') else None
                if not chat_id:
                    return
            except Exception as e:
                self.log_signal.emit(f"Error getting chat ID: {str(e)}")
                return
            try:
                chat = await message.get_chat()
                group_source = chat.title if hasattr(chat, 'title') else str(chat_id)
            except Exception as e:
                group_source = str(chat_id)
            grouped_id = getattr(message, 'grouped_id', None)
            if grouped_id and grouped_id in self.processed_group_ids:
                return
            target = group_config["target"]
            try:
                if self.is_message_forwarded(message.id, chat_id, group_source, grouped_id):
                    return
            except Exception as e:
                self.log_signal.emit(f"Error checking duplicate: {str(e)}")
                return
            message_text = message.text or ""
            if not message_text and not message.media:
                return
            if group_config.get("keywords"):
                try:
                    if not self.contains_keywords(group_config, message_text):
                        return
                except Exception as e:
                    self.log_signal.emit(f"Error checking keywords: {str(e)}")
                    return
            if group_config.get("skip_links", False):
                try:
                    if self.contains_links(message_text):
                        return
                except Exception as e:
                    self.log_signal.emit(f"Error checking links: {str(e)}")
                    return
            if group_config.get("skip_media", False) and message.media and not isinstance(message.media,
                                                                                          MessageMediaWebPage):
                return
            if (message.media and not isinstance(message.media, MessageMediaWebPage) and
                    hasattr(message.media, 'size') and message.media.size > self.MAX_FILE_SIZE):
                self.log_signal.emit(f"Skipping large file: {message.media.size} bytes")
                return
            try:
                if group_config.get("skip_protected", False) and await self.is_chat_protected(chat_id):
                    return
            except Exception as e:
                self.log_signal.emit(f"Error checking protected chat: {str(e)}")
                return
            try:
                base_delay = self.config["telegram"].get("safety_delay", 8.0)
                random_delay = max(1.5, base_delay * random.uniform(1.5, 2.5))
                self.log_signal.emit(f"Waiting {random_delay:.2f} seconds before forwarding...")
                await asyncio.sleep(random_delay)
            except Exception as e:
                self.log_signal.emit(f"Error with delay: {str(e)}")
                return
            try:
                target_entity = await self.resolve_entity(target)
                if not target_entity:
                    self.log_signal.emit(f"Failed to resolve target entity {target}")
                    return
                success = await self.safe_forward(message, target_entity)
                if not success:
                    self.log_signal.emit(f"Failed to forward message {message.id} to {target}")
                    return
                try:
                    self.save_forwarded_message(message.id, chat_id, group_source, grouped_id)
                    if grouped_id:
                        self.processed_group_ids.add(grouped_id)
                except Exception as e:
                    self.log_signal.emit(f"Error saving forwarded message: {str(e)}")
                self.log_signal.emit(f"Message {message.id} forwarded to {target}")
            except Exception as e:
                self.log_signal.emit(f"Forward error: {str(e)}")
        except Exception as e:
            self.log_signal.emit(f"Process message error: {str(e)}")

    async def is_chat_protected(self, chat_id):
        try:
            chat = await self.client.get_entity(chat_id)
            return getattr(chat, 'noforwards', False)
        except Exception as e:
            self.log_signal.emit(f"Error checking chat protection status: {str(e)}")
            return False

    def contains_keywords(self, group_config, text):
        if not text or not group_config.get("keywords"):
            return False
        keywords = group_config["keywords"]
        return any(re.search(rf'\b{re.escape(kw)}\b', text, re.IGNORECASE) for kw in keywords)

    def contains_links(self, text):
        if not text:
            return False
        return bool(
            re.search(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text))

    async def setup_handlers(self):
        try:
            self.log_signal.emit("Setting up message handlers...")
            for group in self.config["groups"]:
                source = group["source"]
                try:
                    if source not in self.source_entities:
                        entity = await self.resolve_entity(source)
                        if entity:
                            self.source_entities[source] = entity
                except Exception as e:
                    self.log_signal.emit(f"Error resolving source entity {source}: {str(e)}")
                    continue
            if not self.source_entities:
                self.log_signal.emit("No valid source entities found for handlers")
                return

            @self.client.on(events.NewMessage(chats=list(self.source_entities.values())))
            async def handler(event):
                try:
                    if random.random() < 0.25:
                        await self.activity_simulator.simulate_human_activity(self.client, self.source_entities)
                    chat_id = event.chat_id
                    for group in self.config["groups"]:
                        source = group["source"]
                        if ((source.startswith('-100') and int(source) == chat_id) or source == str(chat_id)):
                            await self.process_message(event.message, group)
                            break
                except Exception as e:
                    self.log_signal.emit(f"Handler error: {str(e)}")

            self.log_signal.emit("Message handlers setup completed")
        except Exception as e:
            self.log_signal.emit(f"Error setting up handlers: {str(e)}")

    async def check_access_rights(self):
        try:
            for group in self.config["groups"]:
                source = group["source"]
                target = group["target"]
                try:
                    access, msg = await self.check_channel_access(source, False)
                    if not access:
                        self.log_signal.emit(f"No access to source channel {source}: {msg}")
                        continue
                    access, msg = await self.check_channel_access(target, True)
                    if not access:
                        self.log_signal.emit(f"No access to target channel {target}: {msg}")
                        continue
                    self.log_signal.emit(f"Access confirmed for {source} -> {target}")
                except Exception as e:
                    self.log_signal.emit(f"Access check failed for {source} -> {target}: {str(e)}")
        except Exception as e:
            self.log_signal.emit(f"Error checking access rights: {str(e)}")

    async def safe_connect(self):
        for attempt in range(self.max_connection_attempts):
            try:
                if self.client.is_connected():
                    await self.client.disconnect()
                await self.client.connect()
                self.connection_attempts = 0
                return True
            except Exception as e:
                self.connection_attempts += 1
                self.log_signal.emit(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_connection_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return False

    async def handle_flood_wait(self, wait_time):
        """Обработка флуд-контроля"""
        self.log_signal.emit(f"Flood wait required: {wait_time} seconds")
        self.flood_wait_signal.emit(wait_time)
        await asyncio.sleep(wait_time)

    async def authenticate(self):
        try:
            if not await self.safe_connect():
                self.log_signal.emit("Failed to connect to Telegram")
                return False

            if not await self.client.is_user_authorized():
                self.log_signal.emit("Starting authentication...")
                self.auth_required_signal.emit(True)

                try:
                    # Отправляем запрос на код
                    await self.client.send_code_request(self.config["telegram"]["phone"])
                    self.log_signal.emit("Verification code sent to Telegram")
                    self.log_signal.emit("Please enter the code you received:")

                    # Простой ввод кода через консоль
                    code = input("Enter verification code: ").strip()

                    if not code:
                        self.log_signal.emit("No code entered, authentication cancelled")
                        return False

                    # Входим с полученным кодом
                    await self.client.sign_in(self.config["telegram"]["phone"], code)
                    self.log_signal.emit("Successfully signed in with code")

                except FloodWaitError as e:
                    wait_time = e.seconds
                    self.log_signal.emit(f"Need to wait {wait_time} seconds: {e}")
                    await self.handle_flood_wait(wait_time)
                    return False
                except Exception as e:
                    self.log_signal.emit(f"Authentication error: {str(e)}")
                    return False

                self.auth_required_signal.emit(False)
            else:
                self.log_signal.emit("Already authorized, using existing session...")
                try:
                    me = await self.client.get_me()
                    self.log_signal.emit(f"Logged in as: {me.first_name} ({me.phone})")
                except Exception as e:
                    self.log_signal.emit(f"Session validation failed: {str(e)}")
                    return False

            return True

        except FloodWaitError as e:
            await self.handle_flood_wait(e.seconds)
            return False
        except Exception as e:
            self.log_signal.emit(f"Authentication error: {str(e)}")
            return False

    async def main(self):
        try:
            device_model = 'iPhone 13 Pro Max'
            system_version = 'iOS 16.2'
            app_version = '10.0.0'

            self.client = TelegramClient(
                self.config["telegram"]["session_name"],
                int(self.config["telegram"]["api_id"]),
                self.config["telegram"]["api_hash"],
                device_model=device_model,
                system_version=system_version,
                app_version=app_version,
                lang_code='en',
                system_lang_code='en-US'
            )

            self.resource_monitor.start()
            auth_success = await self.authenticate()
            if not auth_success:
                self.log_signal.emit("Authentication failed")
                return

            self.log_signal.emit("Bot authorized successfully")
            await self.check_access_rights()
            self.init_db()

            if self.config["telegram"].get("parse_history", False):
                await self.parse_history_messages()

            await self.setup_handlers()
            self.log_signal.emit("Bot is now running and listening for messages...")

            while self._is_running:
                try:
                    if not self.client.is_connected():
                        self.log_signal.emit("Client disconnected, reconnecting...")
                        if not await self.safe_connect():
                            self.log_signal.emit("Failed to reconnect, stopping bot")
                            break

                    if random.random() < 0.2:
                        await self.activity_simulator.simulate_human_activity(self.client, self.source_entities)

                    await asyncio.sleep(30)

                except Exception as e:
                    self.log_signal.emit(f"Main loop error: {str(e)}")
                    await asyncio.sleep(10)

        except Exception as e:
            self.log_signal.emit(f"Main error: {str(e)}")
        finally:
            try:
                if self.client and self.client.is_connected():
                    await self.client.disconnect()
            except:
                pass
            if self.db_conn:
                self.db_conn.close()
            self.resource_monitor.stop()
            self.log_signal.emit("Telegram client disconnected")

    def close_db(self):
        try:
            db_lock.lock()
            if self.db_conn:
                self.db_conn.close()
                self.db_conn = None
        except Exception as e:
            self.log_signal.emit(f"Error closing database: {str(e)}")
        finally:
            db_lock.unlock()

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.main())
        except asyncio.CancelledError:
            self.log_signal.emit("Bot task cancelled")
        except Exception as e:
            self.log_signal.emit(f"Bot error: {str(e)}")
        finally:
            self.close_db()
            try:
                if not self.loop.is_closed():
                    self.loop.close()
            except:
                pass
            self.log_signal.emit("Bot thread finished")

    def stop(self):
        self._is_running = False
        self.close_db()
        self.resource_monitor.stop()
        if self.client and self.client.is_connected():
            try:
                async def disconnect():
                    await self.client.disconnect()

                asyncio.run_coroutine_threadsafe(disconnect(), self.loop)
            except Exception as e:
                self.log_signal.emit(f"Error during disconnect: {str(e)}")
        self.log_signal.emit("Stop signal processed")


class ConfigWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Telegram Forward Bot")
        self.setGeometry(100, 100, 900, 700)
        self.config = {
            "telegram": {
                "session_name": "session_name",
                "api_id": "",
                "api_hash": "",
                "phone": "",
                "parse_history": False,
                "safety_delay": 8,
                "max_daily_forwards": 200
            },
            "groups": []
        }
        self.init_ui()
        self.load_config()
        self.bot_thread = None
        self.refresh_sessions()
        self.cpu_usage = 0.0
        self.memory_usage = 0.0

    def init_ui(self):
        main_widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(15)

        header = StyledLabel("Telegram Forward Bot", bold=True, font_size=12)
        header.setStyleSheet("color: #2a82da;")
        header.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header)

        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("QTabWidget::pane { border-top: 2px solid #2a82da; }")

        telegram_tab = QWidget()
        telegram_layout = QVBoxLayout()
        telegram_layout.setContentsMargins(10, 10, 10, 10)
        telegram_layout.setSpacing(10)

        session_layout = QHBoxLayout()
        session_layout.addWidget(StyledLabel("Session:"))
        self.session_selector = QComboBox()
        self.refresh_sessions_btn = StyledButton("Refresh")
        self.refresh_sessions_btn.clicked.connect(self.refresh_sessions)
        session_layout.addWidget(self.session_selector)
        session_layout.addWidget(self.refresh_sessions_btn)
        session_layout.addStretch()

        self.session_name = QLineEdit(self.config["telegram"]["session_name"])
        self.api_id = QLineEdit(self.config["telegram"]["api_id"])
        self.api_hash = QLineEdit(self.config["telegram"]["api_hash"])
        self.phone = QLineEdit(self.config["telegram"]["phone"])
        self.parse_history = QCheckBox("Parse message history")
        self.parse_history.setChecked(self.config["telegram"].get("parse_history", False))
        self.safety_delay = QSpinBox()
        self.safety_delay.setMinimum(1)
        self.safety_delay.setMaximum(30)
        self.safety_delay.setValue(int(self.config["telegram"].get("safety_delay", 8)))
        self.max_daily_forwards = QSpinBox()
        self.max_daily_forwards.setMinimum(10)
        self.max_daily_forwards.setMaximum(1000)
        self.max_daily_forwards.setValue(self.config["telegram"].get("max_daily_forwards", 200))

        telegram_layout.addLayout(session_layout)
        telegram_layout.addWidget(StyledLabel("Session Name:"))
        telegram_layout.addWidget(self.session_name)
        telegram_layout.addWidget(StyledLabel("API ID:"))
        telegram_layout.addWidget(self.api_id)
        telegram_layout.addWidget(StyledLabel("API Hash:"))
        telegram_layout.addWidget(self.api_hash)
        telegram_layout.addWidget(StyledLabel("Phone:"))
        telegram_layout.addWidget(self.phone)
        telegram_layout.addWidget(StyledLabel("Safety Delay (seconds):"))
        telegram_layout.addWidget(self.safety_delay)
        telegram_layout.addWidget(StyledLabel("Max Daily Forwards:"))
        telegram_layout.addWidget(self.max_daily_forwards)
        telegram_layout.addWidget(self.parse_history)
        telegram_layout.addStretch()

        telegram_tab.setLayout(telegram_layout)

        groups_tab = QWidget()
        groups_layout = QVBoxLayout()
        groups_layout.setContentsMargins(10, 10, 10, 10)
        groups_layout.setSpacing(10)

        self.groups_list = QListWidget()
        self.source_channel = QLineEdit()
        self.target_channel = QLineEdit()
        self.keywords = QTextEdit()
        self.keywords.setPlaceholderText("Enter keywords separated by commas")
        self.skip_links = QCheckBox("Skip messages with links")
        self.skip_media = QCheckBox("Skip media messages")
        self.skip_protected = QCheckBox("Skip protected chats")

        add_group_btn = StyledButton("Add Group", 100)
        add_group_btn.clicked.connect(self.add_group)
        remove_group_btn = StyledButton("Remove Selected", 100)
        remove_group_btn.clicked.connect(self.remove_group)

        form_layout = QVBoxLayout()
        form_layout.setSpacing(8)
        form_layout.addWidget(StyledLabel("Source Channel ID:"))
        form_layout.addWidget(self.source_channel)
        form_layout.addWidget(StyledLabel("Target Channel ID:"))
        form_layout.addWidget(self.target_channel)
        form_layout.addWidget(StyledLabel("Keywords (optional):"))
        form_layout.addWidget(self.keywords)
        form_layout.addWidget(self.skip_links)
        form_layout.addWidget(self.skip_media)
        form_layout.addWidget(self.skip_protected)

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(add_group_btn)
        btn_layout.addWidget(remove_group_btn)
        btn_layout.addStretch()

        groups_layout.addLayout(form_layout)
        groups_layout.addLayout(btn_layout)
        groups_layout.addWidget(StyledLabel("Configured Groups:"))
        groups_layout.addWidget(self.groups_list)

        groups_tab.setLayout(groups_layout)

        log_tab = QWidget()
        log_layout = QVBoxLayout()
        log_layout.setContentsMargins(10, 10, 10, 10)
        log_layout.setSpacing(5)

        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setStyleSheet("""
            QTextEdit {
                background: #252525;
                border: 1px solid #444;
                font-family: Consolas, Courier New;
                font-size: 9pt;
            }
        """)

        self.resource_label = StyledLabel("Resources: CPU: 0.0% | Memory: 0.0 MB")
        log_layout.addWidget(self.resource_label)
        log_layout.addWidget(StyledLabel("Activity Log:"))
        log_layout.addWidget(self.log_display)

        log_tab.setLayout(log_layout)

        self.tabs.addTab(telegram_tab, "Telegram Settings")
        self.tabs.addTab(groups_tab, "Groups")
        self.tabs.addTab(log_tab, "Log")

        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)

        save_btn = StyledButton("Save Config")
        save_btn.clicked.connect(self.save_config)
        load_btn = StyledButton("Load Config")
        load_btn.clicked.connect(self.load_config_dialog)
        self.start_btn = StyledButton("Start Bot")
        self.start_btn.clicked.connect(self.start_bot)
        self.stop_btn = StyledButton("Stop Bot")
        self.stop_btn.clicked.connect(self.stop_bot)
        self.stop_btn.setEnabled(False)

        button_layout.addWidget(save_btn)
        button_layout.addWidget(load_btn)
        button_layout.addStretch()
        button_layout.addWidget(self.start_btn)
        button_layout.addWidget(self.stop_btn)

        main_layout.addWidget(self.tabs)
        main_layout.addLayout(button_layout)

        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)

    def refresh_sessions(self):
        self.session_selector.clear()
        sessions = [f for f in os.listdir('.') if f.endswith('.session')]
        self.session_selector.addItems(sessions)
        if sessions and not self.session_name.text():
            self.session_name.setText(sessions[0].replace('.session', ''))

    def validate_config(self):
        errors = []
        tg_config = self.config["telegram"]
        if not tg_config.get("api_id") or not str(tg_config["api_id"]).isdigit():
            errors.append("Invalid API ID")
        if not tg_config.get("api_hash"):
            errors.append("API Hash is required")
        if not tg_config.get("phone"):
            errors.append("Phone number is required")
        if not self.config.get("groups"):
            errors.append("At least one group must be configured")
        else:
            for i, group in enumerate(self.config["groups"]):
                if not group.get("source"):
                    errors.append(f"Group {i + 1}: Source is required")
                if not group.get("target"):
                    errors.append(f"Group {i + 1}: Target is required")
        return errors

    def add_group(self):
        source = self.source_channel.text().strip()
        target = self.target_channel.text().strip()
        if not source or not target:
            QMessageBox.warning(self, "Error", "Source and Target channels are required!")
            return
        keywords_text = self.keywords.toPlainText().strip()
        keywords = [kw.strip() for kw in keywords_text.split(",")] if keywords_text else []
        group = {
            "source": source,
            "target": target,
            "keywords": keywords,
            "skip_links": self.skip_links.isChecked(),
            "skip_media": self.skip_media.isChecked(),
            "skip_protected": self.skip_protected.isChecked()
        }
        self.config["groups"].append(group)
        self.update_groups_list()
        self.source_channel.clear()
        self.target_channel.clear()
        self.keywords.clear()

    def remove_group(self):
        selected = self.groups_list.currentRow()
        if selected >= 0:
            self.config["groups"].pop(selected)
            self.update_groups_list()

    def update_groups_list(self):
        self.groups_list.clear()
        for group in self.config["groups"]:
            item = QListWidgetItem(f"From: {group['source']} → To: {group['target']}")
            self.groups_list.addItem(item)

    def save_config(self, silent=False):
        try:
            self.config["telegram"] = {
                "session_name": self.session_name.text(),
                "api_id": self.api_id.text(),
                "api_hash": self.api_hash.text(),
                "phone": self.phone.text(),
                "parse_history": self.parse_history.isChecked(),
                "safety_delay": float(self.safety_delay.value()),
                "max_daily_forwards": self.max_daily_forwards.value()
            }
            with open("config.yaml", "w", encoding='utf-8') as f:
                yaml.dump(self.config, f)
            if not silent:
                QMessageBox.information(self, "Success", "Configuration saved!")
            self.log("Configuration saved successfully")
        except Exception as e:
            self.log(f"Error saving config: {str(e)}")
            if not silent:
                QMessageBox.critical(self, "Error", f"Failed to save config: {str(e)}")

    def load_config(self):
        try:
            if os.path.exists("config.yaml"):
                with open("config.yaml", "r", encoding='utf-8') as f:
                    self.config = yaml.safe_load(f)
                tg = self.config.get("telegram", {})
                self.session_name.setText(tg.get("session_name", ""))
                self.api_id.setText(str(tg.get("api_id", "")))
                self.api_hash.setText(tg.get("api_hash", ""))
                self.phone.setText(tg.get("phone", ""))
                self.parse_history.setChecked(tg.get("parse_history", False))
                self.safety_delay.setValue(int(tg.get("safety_delay", 8)))
                self.max_daily_forwards.setValue(tg.get("max_daily_forwards", 200))
                self.update_groups_list()
                self.log("Configuration loaded successfully.")
        except Exception as e:
            self.log(f"Error loading config: {str(e)}")

    def load_config_dialog(self):
        try:
            file_name, _ = QFileDialog.getOpenFileName(self, "Load Config", "", "YAML Files (*.yaml *.yml)")
            if not file_name:
                return
            with open(file_name, "r", encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            tg = self.config.get("telegram", {})
            self.session_name.setText(tg.get("session_name", ""))
            self.api_id.setText(str(tg.get("api_id", "")))
            self.api_hash.setText(tg.get("api_hash", ""))
            self.phone.setText(tg.get("phone", ""))
            self.parse_history.setChecked(tg.get("parse_history", False))
            self.safety_delay.setValue(int(tg.get("safety_delay", 8)))
            self.max_daily_forwards.setValue(tg.get("max_daily_forwards", 200))
            self.update_groups_list()
            self.log(f"Configuration loaded from {file_name}")
            QMessageBox.information(self, "Success", "Configuration loaded successfully!")
        except Exception as e:
            self.log(f"Error loading config: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to load config: {str(e)}")

    def show_flood_wait_dialog(self, wait_time):
        dialog = FloodWaitDialog(wait_time, self)
        dialog.exec_()

    def start_bot(self):
        try:
            if self.bot_thread and self.bot_thread.isRunning():
                self.log("Bot is already running")
                return
            errors = self.validate_config()
            if errors:
                QMessageBox.warning(self, "Configuration Error", "\n".join(errors))
                return
            if not self.config.get("groups"):
                QMessageBox.warning(self, "Warning", "No groups configured!")
                return
            if not all(field in self.config["telegram"] for field in ["api_id", "api_hash", "phone"]):
                QMessageBox.warning(self, "Warning", "Telegram credentials not set!")
                return
            self.log("Initializing bot thread...")
            self.save_config(silent=True)
            self.bot_thread = BotThread(self.config, self)
            self.bot_thread.log_signal.connect(self.log)
            self.bot_thread.auth_required_signal.connect(self.handle_auth_required)
            self.bot_thread.resource_signal.connect(self.update_resource_usage)
            self.bot_thread.flood_wait_signal.connect(self.show_flood_wait_dialog)
            self.bot_thread.finished.connect(self.on_bot_finished)
            self.bot_thread.start()
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.log("Bot started successfully")
        except Exception as e:
            self.log(f"Failed to start bot: {str(e)}")
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)

    def handle_auth_required(self, required):
        if required:
            self.log("Authentication required")
        else:
            self.log("Authentication completed")

    def update_resource_usage(self, resource_type, value):
        if resource_type == "cpu":
            self.cpu_usage = value
        elif resource_type == "memory":
            self.memory_usage = value
        self.resource_label.setText(f"Resources: CPU: {self.cpu_usage:.1f}% | Memory: {self.memory_usage:.1f} MB")

    def on_bot_finished(self):
        self.log("Bot thread finished")
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.bot_thread = None

    def stop_bot(self):
        try:
            if self.bot_thread and self.bot_thread.isRunning():
                self.log("Stopping bot...")
                self.bot_thread.stop()
                if not self.bot_thread.wait(5000):
                    self.log("Force terminating bot thread...")
                    self.bot_thread.terminate()
                    self.bot_thread.wait(2000)
                self.log("Bot stopped successfully")
            else:
                self.log("Bot is not running")
        except Exception as e:
            self.log(f"Error stopping bot: {str(e)}")
        finally:
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)

    def log(self, message):
        if self.log_display.document().blockCount() > 1000:
            cursor = self.log_display.textCursor()
            cursor.movePosition(cursor.Start)
            cursor.movePosition(cursor.Down, cursor.KeepAnchor, 100)
            cursor.removeSelectedText()
        self.log_display.append(message)
        self.log_display.verticalScrollBar().setValue(self.log_display.verticalScrollBar().maximum())
        logger.info(message)

    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Exit Confirmation', 'Are you sure you want to exit?',
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            if hasattr(self, 'bot_thread') and self.bot_thread and self.bot_thread.isRunning():
                self.log("Stopping bot before exit...")
                self.stop_bot()
                self.bot_thread.wait(3000)
            event.accept()
        else:
            event.ignore()


if __name__ == "__main__":
    app = QApplication([])
    DarkTheme.apply(app)
    window = ConfigWindow()
    window.show()
    try:
        app.exec_()
    except Exception as e:
        logging.error(f"Application error: {e}")
        if hasattr(window, 'bot_thread') and window.bot_thread and window.bot_thread.isRunning():
            window.bot_thread.stop()
            window.bot_thread.wait(2000)
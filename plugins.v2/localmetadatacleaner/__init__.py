import copy
import json
import math
import os
import re
import time
import traceback
from datetime import datetime
from pathlib import Path
from threading import RLock, Timer
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.core.config import settings
from app.core.event import eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

try:
    from app.chain.storage import StorageChain
except Exception:  # pragma: no cover
    StorageChain = None

try:
    from app.chain.media import MediaChain
except Exception:  # pragma: no cover
    MediaChain = None

try:
    from app.chain.mediaserver import MediaServerChain
except Exception:  # pragma: no cover
    MediaServerChain = None

try:
    from app.helper.mediaserver import MediaServerHelper
except Exception:  # pragma: no cover
    MediaServerHelper = None


class LocalMetadataCleaner(_PluginBase):
    """监控 STRM 入库，根据 STRM 库判断刮削完整度，通过网盘真实路径触发 MP 刮削。"""

    plugin_name = "监控strm刮削网盘"
    plugin_desc = "复用 MP 全局媒体库入库事件：检查 STRM 库刮削信息，缺失时通过网盘真实路径触发 MP 刮削。"
    plugin_icon = "https://movie-pilot.org/assets/icon.png"
    plugin_version = "2.7.12"
    plugin_author = "jidian"
    author_url = ""
    plugin_config_prefix = "localmetadatacleaner_"
    plugin_order = 99
    auth_level = 1

    DEFAULT_STRM_CHECK_ROOT = "/media"
    DEFAULT_SCRAPE_TARGET_ROOT = "/CD2/115/CMS影库/影视"
    DEFAULT_CRON = "*/1 * * * *"
    DEFAULT_INITIAL_CHECK_DELAY_SECONDS = 10
    DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS = 30
    DEFAULT_POST_SCRAPE_CHECK_MINUTES = 10
    DEFAULT_TV_RECHECK_DAYS = 10
    DEFAULT_MOVIE_RETRY_DELAYS_SECONDS = [30, 120, 300, 900, 3600]
    DEFAULT_SCRAPE_PATH_REFRESH_INTERVAL_SECONDS = 600
    DEFAULT_QUEUE_EXCEPTION_RETRY_DELAYS_SECONDS = [60, 300, 900]
    DEFAULT_SCOPE_ACCESS_RETRY_DELAYS_SECONDS = [60, 300, 900]
    MAX_SEASON_POSTCHECK_RESCRAPES = 1
    DEFAULT_TARGET_DEPTH = 3
    DEFAULT_STORAGE = "local"
    HISTORY_LIMIT = 100

    STATUS_LABELS = {
        "waiting_initial_check": "等待入库检查",
        "waiting_movie_retry": "等待电影文件重试",
        "waiting_episode_scrape": "等待单集刮削",
        "waiting_tv_postcheck": "等待刮削后检查",
        "waiting_tv_recheck": "等待长期复查",
        "waiting_tv_metadata_retry": "等待剧季路径重试",
        "waiting_task_exception_retry": "等待异常重试",
    }
    TASK_TYPE_LABELS = {
        "initial": "入库检查",
        "movie_retry": "电影重试",
        "episode_scrape": "单集刮削",
        "tv_postcheck": "刮削后检查",
        "tv_recheck": "长期复查",
        "tv_metadata_retry": "剧季重试",
    }
    TASK_COLORS = {
        "initial": "success",
        "movie_retry": "warning",
        "episode_scrape": "primary",
        "tv_postcheck": "info",
        "tv_recheck": "warning",
        "tv_metadata_retry": "warning",
    }
    DETAIL_COLOR_MODE = "color"
    ACTION_LABELS = {
        "movie_complete_skip": "电影完整跳过",
        "movie_incomplete_scrape": "电影补刮削",
        "tv_root_no_metadata_scrape_whole_show": "整剧刮削",
        "tv_root_no_metadata_merge_existing_whole_show_scrape": "整剧事件合并",
        "tv_root_postcheck_incomplete": "剧信息检查失败",
        "tv_root_postcheck_complete": "剧信息检查完成",
        "tv_root_postcheck_manual_incomplete_keep_waiting": "剧信息手动检查失败",
        "tv_season_metadata_incomplete_scrape": "季信息补刮削",
        "tv_season_metadata_merge_existing_scrape": "季信息事件合并",
        "tv_season_metadata_wait_root_scrape": "季信息等待剧刮削",
        "tv_season_postcheck_incomplete": "季信息检查失败",
        "tv_season_postcheck_failed": "季信息补刮失败",
        "tv_season_postcheck_complete": "季信息检查完成",
        "episode_missing_image_schedule_scrape": "单集缺图",
        "episode_scrape_done": "单集刮削完成",
        "episode_scrape_failed": "单集刮削失败",
        "episode_delayed_scrape": "单集延迟刮削",
        "tv_episode_missing_image_schedule_scrape": "单集缺图",
        "tv_episode_image_exists_skip_initial": "单集完整跳过",
        "tv_postcheck_missing_schedule_recheck": "刮削后缺图",
        "tv_postcheck_missing_schedule_episode_scrape": "目录刮削后补单集",
        "tv_postcheck_complete": "刮削后完整",
        "tv_postcheck_show_missing": "刮削后检查失败",
        "movie_metadata_complete_skip": "电影完整跳过",
        "movie_metadata_incomplete_scrape": "电影补刮削",
        "movie_scrape_target_missing": "电影刮削文件缺失",
        "movie_scrape_retry_scheduled": "电影等待重试",
        "movie_retry_scrape_success": "电影重试成功",
        "movie_retry_scrape_wait": "电影继续等待",
        "movie_retry_scrape_failed": "电影重试失败",
        "tv_metadata_retry_scheduled": "剧季等待重试",
        "tv_metadata_retry_success": "剧季重试成功",
        "tv_metadata_retry_wait": "剧季继续等待",
        "tv_metadata_retry_failed": "剧季重试失败",
        "episode_scrape_retry_wait": "单集继续等待",
        "episode_scrape_retry_failed": "单集重试失败",
        "episode_nfo_delete_retry_wait": "单集 nfo 删除等待",
        "episode_nfo_delete_failed": "单集 nfo 删除失败",
        "episode_scrape_target_missing": "单集刮削文件缺失",
        "episode_scrape_target_rejected": "单集刮削目标拒绝",
        "tv_recheck_missing_episodes_schedule_scrape": "10天复查缺图",
        "tv_recheck_complete": "10天复查完成",
        "queue_cleared_by_user": "清空队列",
        "queue_deleted_by_user": "删除任务",
        "tv_postcheck_manual_missing_keep_waiting": "手动检查仍缺图",
        "tv_season_postcheck_manual_incomplete_keep_waiting": "季信息手动检查失败",
        "tv_recheck_manual_missing_keep_waiting": "10天手动复查仍缺图",
        "tv_recheck_show_missing": "10天复查剧名目录缺失",
        "skip_unknown_media_type": "未知类型跳过",
        "drop_unknown_task": "未知任务丢弃",
        "queue_task_exception_wait": "任务异常等待重试",
        "queue_task_exception_failed": "任务异常已停止",
        "tv_postcheck_show_retry": "刮削后目录等待重试",
        "tv_postcheck_show_failed": "刮削后目录检查失败",
        "tv_postcheck_no_episode_keep_waiting": "手动检查未读取到单集",
        "tv_postcheck_no_episode_retry": "单集读取等待重试",
        "tv_postcheck_no_episode_failed": "单集读取检查失败",
        "tv_recheck_show_retry": "复查目录等待重试",
        "tv_recheck_show_failed": "复查目录检查失败",
        "tv_recheck_no_episode_keep_waiting": "手动复查未读取到单集",
        "tv_recheck_no_episode_retry": "复查单集读取等待",
        "tv_recheck_no_episode_failed": "复查单集读取失败",
        "tv_season_postcheck_no_scope_retry": "季信息范围等待重试",
        "tv_season_postcheck_no_scope_failed": "季信息范围检查失败",
        "episode_strm_missing_skip": "单集已不存在",
        "tv_recheck_stale_complete": "复查目标已不存在",
        "movie_strm_missing_skip": "电影 STRM 已不存在",
        "tv_initial_no_existing_episode_skip": "入库单集已不存在",
        "tv_metadata_retry_strm_missing_skip": "剧集来源已不存在",
        "tv_metadata_retry_scope_rejected": "剧季范围被拒绝",
    }

    # 基础开关
    _enabled: bool = False
    _notify: bool = True
    _onlyonce: bool = False

    # 媒体服务器 / 媒体库过滤
    _media_server: str = ""
    _include_libraries: List[str] = []
    _all_libraries: List[Dict[str, Any]] = []
    _library_path_mapping: str = ""
    _library_mapping_check_once: bool = False

    # 路径：检查看 STRM 库，刮削走网盘目标根路径
    _strm_check_root: str = DEFAULT_STRM_CHECK_ROOT
    _scrape_target_root: str = DEFAULT_SCRAPE_TARGET_ROOT
    _target_depth: int = DEFAULT_TARGET_DEPTH

    # 时间配置
    _cron: str = DEFAULT_CRON
    _initial_check_delay_seconds: int = DEFAULT_INITIAL_CHECK_DELAY_SECONDS
    _episode_scrape_delay_seconds: float = DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS
    _post_scrape_check_delay_minutes: float = DEFAULT_POST_SCRAPE_CHECK_MINUTES
    _tv_recheck_days: float = DEFAULT_TV_RECHECK_DAYS

    # 刮削配置
    _scrape: bool = True
    _storage: str = DEFAULT_STORAGE
    _scrape_path_refresh_interval_seconds: float = DEFAULT_SCRAPE_PATH_REFRESH_INTERVAL_SECONDS
    # 队列操作
    _queue_delete_items: List[str] = []
    _queue_delete_confirm: bool = False
    _queue_clear_all: bool = False
    _lock = RLock()
    _timers: List[Timer] = []
    _next_timer_due_ts: float = 0
    _storagechain = None
    _scrape_path_refresh_cache: Optional[Dict[str, Any]] = None
    _active_notify_results: Optional[List[Dict[str, Any]]] = None
    mschain = None
    mediaserver_helper = None

    def init_plugin(self, config: dict = None):
        self.stop_service()
        # 路径刷新缓存只属于当前插件实例；重载后重新探测，避免复用旧实例结果。
        self._scrape_path_refresh_cache = {}
        try:
            self.mschain = MediaServerChain() if MediaServerChain else None
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：初始化 MediaServerChain 失败：{err}")
            self.mschain = None
        try:
            self.mediaserver_helper = MediaServerHelper() if MediaServerHelper else None
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：初始化 MediaServerHelper 失败：{err}")
            self.mediaserver_helper = None

        if config:
            self._enabled = bool(config.get("enabled", False))
            self._notify = bool(config.get("notify", True))
            self._onlyonce = bool(config.get("onlyonce", False))
            self._media_server = str(config.get("media_server") or "").strip()
            self._include_libraries = self._to_list(config.get("include_libraries") or [])
            self._all_libraries = config.get("all_libraries") or []
            if not isinstance(self._all_libraries, list):
                self._all_libraries = []
            self._library_path_mapping = str(config.get("library_path_mapping") or "").strip()
            self._library_mapping_check_once = bool(config.get("library_mapping_check_once", False))

            self._strm_check_root = self._normalise_config_root(
                config.get("strm_check_root"), self.DEFAULT_STRM_CHECK_ROOT, allow_filesystem_root=True
            )
            self._scrape_target_root = self._normalise_config_root(
                config.get("scrape_target_root"), self.DEFAULT_SCRAPE_TARGET_ROOT, allow_filesystem_root=False
            )
            self._target_depth = self.DEFAULT_TARGET_DEPTH

            self._cron = str(config.get("cron") or self.DEFAULT_CRON).strip() or self.DEFAULT_CRON
            self._initial_check_delay_seconds = int(self._to_float(config.get("initial_check_delay_seconds"), self.DEFAULT_INITIAL_CHECK_DELAY_SECONDS))
            delay_seconds_value = config.get("episode_scrape_delay_seconds")
            if delay_seconds_value is None:
                # 兼容旧版本：旧配置单位是分钟。
                delay_seconds_value = self._to_float(config.get("episode_scrape_delay_minutes"), self.DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS / 60) * 60
            self._episode_scrape_delay_seconds = max(self._to_float(delay_seconds_value, self.DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS), 0)
            self._post_scrape_check_delay_minutes = self.DEFAULT_POST_SCRAPE_CHECK_MINUTES
            recheck_days = self._to_float(config.get("tv_recheck_days"), self.DEFAULT_TV_RECHECK_DAYS)
            self._tv_recheck_days = min(recheck_days if recheck_days >= 0 else self.DEFAULT_TV_RECHECK_DAYS, 3650)
            # v2.6.2 起刮削是插件的固定核心功能。保留旧配置字段，但不再允许关闭，
            # 避免旧配置为 false 时单集任务先删 NFO、随后又跳过 MP 刮削。
            self._scrape = True
            self._scrape_path_refresh_interval_seconds = max(
                self._to_float(
                    config.get("scrape_path_refresh_interval_seconds"),
                    self.DEFAULT_SCRAPE_PATH_REFRESH_INTERVAL_SECONDS
                ),
                0
            )
            # 页面不再显示存储标识，普通本地映射固定使用 local。
            self._storage = self.DEFAULT_STORAGE

            self._queue_delete_items = self._to_list(config.get("queue_delete_items") or [])
            self._queue_delete_confirm = bool(config.get("queue_delete_confirm", False))
            self._queue_clear_all = bool(config.get("queue_clear_all", False))

        queue_action_done = False
        if self._queue_clear_all:
            deleted = self._clear_queue_items()
            logger.info(f"监控strm刮削网盘：已清空待处理队列，共删除 {deleted} 个任务")
            self._queue_clear_all = False
            self._queue_delete_confirm = False
            self._queue_delete_items = []
            queue_action_done = True
        elif self._queue_delete_confirm and self._queue_delete_items:
            deleted, missing = self._delete_queue_items(self._queue_delete_items)
            logger.info(f"监控strm刮削网盘：已删除所选队列任务 {deleted} 个，未找到 {missing} 个")
            self._queue_delete_items = []
            self._queue_delete_confirm = False
            queue_action_done = True
        elif self._queue_delete_confirm:
            logger.warning("监控strm刮削网盘：已开启删除所选任务，但没有选择任何队列任务")
            self._queue_delete_confirm = False
            queue_action_done = True

        mapping_check_done = False
        if self._library_mapping_check_once:
            report = self._library_mapping_check_report()
            try:
                self.save_data("library_mapping_check", report)
            except Exception as err:
                logger.warning(f"监控strm刮削网盘：保存媒体库路径映射检测结果失败：{err}")
            logger.info(f"监控strm刮削网盘：媒体库路径映射检测：{report.get('title')}")
            for line in self._to_list(report.get("lines") or [])[:8]:
                logger.info(f"监控strm刮削网盘：媒体库路径映射检测明细：{line}")
            self._library_mapping_check_once = False
            mapping_check_done = True

        if self._media_server and self._library_cache_needs_refresh():
            self._refresh_library_cache()
            self.__update_config()

        if self._onlyonce:
            logger.info("监控strm刮削网盘：运行到期任务一次")
            self.run_once()
            self._onlyonce = False
            self.__update_config()

        if queue_action_done or mapping_check_done:
            self.__update_config()

        # 保存配置或 MP 重启后，Timer 会被 stop_service 取消；这里按队列里最近到期任务恢复。
        # 10 天复查任务按用户要求仍由兜底检查周期处理，不创建长期 Timer。
        if self._enabled:
            self._restore_queue_timers()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/queue_delete",
                "endpoint": self.api_delete_queue,
                "methods": ["GET"],
                "summary": "删除待处理队列任务"
            },
            {
                "path": "/queue_delete_episode_group",
                "endpoint": self.api_delete_episode_group,
                "methods": ["GET"],
                "summary": "删除同剧单集待刮削任务组"
            },
            {
                "path": "/queue_run",
                "endpoint": self.api_run_queue,
                "methods": ["GET"],
                "summary": "立即执行所选待处理任务"
            },
            {
                "path": "/queue_run_episode_group",
                "endpoint": self.api_run_episode_group,
                "methods": ["GET"],
                "summary": "立即执行同剧单集待刮削任务组"
            },
            {
                "path": "/queue_clear",
                "endpoint": self.api_clear_queue,
                "methods": ["GET"],
                "summary": "清空待处理队列"
            },
            {
                "path": "/history_clear",
                "endpoint": self.api_clear_history,
                "methods": ["GET"],
                "summary": "清空历史记录"
            },
            {
                "path": "/library_mapping_check",
                "endpoint": self.api_library_mapping_check,
                "methods": ["GET"],
                "summary": "检测媒体库路径映射"
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron or self.DEFAULT_CRON)
        except Exception as err:
            logger.error(f"监控strm刮削网盘：cron 表达式错误，使用默认 {self.DEFAULT_CRON}：{err}")
            trigger = CronTrigger.from_crontab(self.DEFAULT_CRON)
        return [{
            "id": "LocalMetadataCleaner",
            "name": "监控strm刮削网盘",
            "trigger": trigger,
            "func": self.run_once,
            "kwargs": {}
        }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置页：常用项平铺，保持页面简洁。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "运行到期任务一次"}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSelect", "props": {"model": "media_server", "label": "媒体服务器", "items": self._get_media_server_select_items(), "clearable": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "cron", "label": "兜底检查周期", "placeholder": self.DEFAULT_CRON}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "initial_check_delay_seconds", "label": "入库后检查延迟秒", "placeholder": "10"}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VSelect", "props": {"model": "include_libraries", "label": "触发媒体库", "placeholder": "留空表示所有媒体库；选择媒体服务器并保存后自动刷新", "items": self._get_library_select_items(), "multiple": True, "chips": True, "closable-chips": True, "clearable": True}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VTextarea", "props": {"model": "library_path_mapping", "label": "媒体库路径映射", "placeholder": "动漫|tv|/media/电视剧/国漫,/media/电视剧/日番\n电影|movie|/media/电影/华语电影,/media/电影/外语电影", "rows": 3, "auto-grow": True, "clearable": True, "hint": "可选。用于媒体库名称和 STRM 实际路径不一致的情况；格式：媒体库名称|类型|路径1,路径2，类型 movie 或 tv。", "persistent-hint": True}},
                                {"component": "VSwitch", "props": {"model": "library_mapping_check_once", "label": "保存后检测媒体库映射", "hint": "打开后保存配置，会立即检测已保存映射并在日志和详情页显示结果；检测完成后自动关闭。", "persistent-hint": True}},
                                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": "修改映射后打开此开关并保存配置，检测结果会写入插件日志和详情页。"}
                            ]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "strm_check_root", "label": "STRM 检查根路径", "placeholder": self.DEFAULT_STRM_CHECK_ROOT}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "scrape_target_root", "label": "MP 刮削目标根路径", "placeholder": self.DEFAULT_SCRAPE_TARGET_ROOT}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "episode_scrape_delay_seconds", "label": "单集刮削等待秒", "placeholder": "30"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "scrape_path_refresh_interval_seconds", "label": "刮削前刷新间隔秒", "placeholder": "600"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "tv_recheck_days", "label": "复查天数", "placeholder": "10"}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "mt-2"},
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": self._help_text()}}]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "media_server": "",
            "include_libraries": [],
            "all_libraries": [],
            "library_path_mapping": "",
            "library_mapping_check_once": False,
            "strm_check_root": self.DEFAULT_STRM_CHECK_ROOT,
            "scrape_target_root": self.DEFAULT_SCRAPE_TARGET_ROOT,
            "cron": self.DEFAULT_CRON,
            "initial_check_delay_seconds": self.DEFAULT_INITIAL_CHECK_DELAY_SECONDS,
            "episode_scrape_delay_seconds": self.DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS,
            "scrape_path_refresh_interval_seconds": self.DEFAULT_SCRAPE_PATH_REFRESH_INTERVAL_SECONDS,
            "tv_recheck_days": self.DEFAULT_TV_RECHECK_DAYS,
            "scrape": True,
            "queue_delete_items": [],
            "queue_delete_confirm": False,
            "queue_clear_all": False
        }

    def get_page(self) -> List[dict]:
        """插件详情页：任务工作台 + 折叠明细。"""
        # 详情页只读取状态。展示预览在深拷贝上刷新，避免覆盖正在运行的队列状态，
        # 也避免用户仅打开页面就提前删除尚未到期的 10 天复查任务。
        with self._lock:
            state = copy.deepcopy(self._load_state())
        self._refresh_queue_preview_cache_for_display(state)
        queue = state.get("queue") or {}
        history = state.get("history") or []
        queue_count = len(queue)
        history_display = self._history_for_display(history, limit=30)
        failure_display = self._history_failure_for_display(history, limit=10)

        task_stats = self._queue_stats(queue)
        last_success = self._last_success_record(history)
        cards: List[Dict[str, Any]] = []
        cards.append(self._dashboard_header())
        cards.append(self._dashboard_metric_row(queue_count, task_stats, last_success))
        cards.append(self._queue_section(queue, queue_count))
        cards.append(self._history_section(history_display, failure_display))

        return [{"component": "div", "props": {"class": "pa-4", "style": self._page_style()}, "content": cards}]

    @staticmethod
    def _dashboard_header() -> Dict[str, Any]:
        return {"component": "div", "props": {"class": "mb-4"}, "content": [
            {"component": "div", "props": {"class": "text-h5 font-weight-bold"}, "text": "监控 STRM 刮削网盘"},
            {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis mt-1"}, "text": "新增 STRM 元数据检查与补全"}
        ]}

    def _dashboard_metric_row(self, queue_count: int, task_stats: Dict[str, int], last_success: Dict[str, Any]) -> Dict[str, Any]:
        success_title = "暂无"
        success_subtitle = "最近成功"
        if last_success:
            action = str(last_success.get("action") or "")
            _media_type, media_name, _category, _scope_label, _identity = self._notification_media(last_success, action)
            success_title = media_name or self._short_path(str(last_success.get("scope") or last_success.get("folder") or "已完成"))
        return {"component": "VRow", "props": {"dense": True, "class": "mb-3"}, "content": [
            {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3}, "content": [
                self._dashboard_metric_card("mdi-folder-outline", "待处理", str(queue_count), "success")
            ]},
            {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3}, "content": [
                self._dashboard_metric_card("mdi-clock-check-outline", "10分钟检查", str(task_stats.get("tv_postcheck", 0)), "info")
            ]},
            {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3}, "content": [
                self._dashboard_metric_card("mdi-clock-time-eight-outline", "长期复查", str(task_stats.get("tv_recheck", 0)), "warning")
            ]},
            {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3}, "content": [
                self._dashboard_metric_card("mdi-check-circle-outline", success_subtitle, success_title, "primary")
            ]},
        ]}

    def _dashboard_metric_card(self, icon: str, label: str, value: str, color: str, subtitle: str = "") -> Dict[str, Any]:
        return {"component": "VCard", "props": {"variant": "tonal", "color": color, "class": "h-100"}, "content": [
            {"component": "VCardText", "props": {"class": "d-flex align-center"}, "content": [
                {"component": "VIcon", "props": {"size": "large", "class": "mr-3"}, "text": icon},
                {"component": "div", "props": {"class": "min-w-0"}, "content": [
                    {"component": "div", "props": {"class": "text-caption"}, "text": label},
                    {"component": "div", "props": {"class": "text-h6 font-weight-bold text-truncate"}, "text": value},
                    *([{"component": "div", "props": {"class": "text-caption text-medium-emphasis text-truncate mt-1"}, "text": subtitle}] if subtitle else [])
                ]}
            ]}
        ]}

    def _last_success_record(self, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        for item in reversed(history or []):
            if not isinstance(item, dict):
                continue
            outcome, color = self._history_outcome(item)
            if color == "success" or outcome in {"刮削成功", "复查完成", "已完整"}:
                return item
        return {}

    def _queue_section(self, queue: Dict[str, Any], queue_count: int) -> Dict[str, Any]:
        display_items = self._queue_display_items(queue)
        actions = []
        if display_items:
            actions.append({"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("warning"), "size": "small", "prepend-icon": "mdi-delete-outline"}, "text": "清空队列", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_clear", "method": "get", "params": {"apikey": getattr(settings, "API_TOKEN", "")}}}})
        content: List[Dict[str, Any]] = [
            self._workbench_section_header("1.", "待处理任务", "", actions)
        ]
        if display_items:
            grouped = self._queue_grouped_display_items(display_items)
            panels: List[Dict[str, Any]] = []
            for group in grouped:
                items = group.get("items") or []
                if not items:
                    continue
                panels.extend([self._queue_display_panel(display) for display in items])
            content.append({"component": "VExpansionPanels", "props": {"variant": "accordion", "class": "mt-3"}, "content": panels})
        else:
            content.append(self._empty_state("mdi-check-circle-outline", "当前没有待处理任务。", "新增入库后，这里会按类型和剧名聚合显示任务摘要。", "success"))
        return {"component": "VCard", "props": {"variant": "flat", "color": "surface", "class": "pa-4 mb-4 elevation-2", "style": self._section_card_style()}, "content": content}

    def _history_section(self, history_display: List[Dict[str, Any]], failure_display: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        failure_display = failure_display or []
        actions = [
            {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("warning"), "size": "small", "prepend-icon": "mdi-broom"}, "text": "清空历史记录", "events": {"click": {"api": "plugin/LocalMetadataCleaner/history_clear", "method": "get", "params": {"apikey": getattr(settings, "API_TOKEN", "")}}}}
        ]
        content: List[Dict[str, Any]] = [
            self._workbench_section_header("2.", "最近记录 / 历史记录", "", actions)
        ]
        panels: List[Dict[str, Any]] = []
        if failure_display:
            failure_groups = self._history_display_groups(failure_display)
            panels.extend([self._history_group_panel(group) for group in failure_groups])
        if history_display:
            history_groups = self._history_display_groups(history_display)
            panels.extend([self._history_group_panel(group) for group in history_groups])
        if panels:
            content.append({"component": "VExpansionPanels", "props": {"variant": "accordion", "class": "mt-3"}, "content": panels})
        else:
            content.append(self._empty_state("mdi-history", "暂无最近处理记录。", "完成刮削、检查或复查后，这里会按媒体聚合展示历史。", "info"))
        return {"component": "VCard", "props": {"variant": "flat", "color": "surface", "class": "pa-4 mb-4 elevation-2", "style": self._section_card_style()}, "content": content}

    @staticmethod
    def _workbench_section_header(number: str, title: str, subtitle: str = "", actions: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        is_history = "历史" in str(title) or "记录" in str(title)
        icon = "mdi-table-clock" if is_history else "mdi-playlist-check"
        color = "primary" if is_history else "info"
        icon_color = "rgb(var(--v-theme-primary))" if is_history else "#16B1FF"
        return {"component": "div", "props": {"class": "mb-2"}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-3"}, "content": [
                {"component": "div", "props": {"class": "d-flex align-center ga-2"}, "content": [
                    {"component": "VIcon", "props": {"color": color, "size": "default", "class": "mr-1", "style": f"color:{icon_color};"}, "text": icon},
                    {"component": "span", "props": {"class": "text-h6 font-weight-bold", "style": f"color:{icon_color};"}, "text": number},
                    {"component": "span", "props": {"class": "text-h6 font-weight-bold"}, "text": title}
                ]},
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": actions or []}
            ]},
            *([{"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": subtitle}] if subtitle else [])
        ]}

    def _empty_state(self, icon: str, title: str, subtitle: str = "", color: str = "info") -> Dict[str, Any]:
        tone = self._tone_palette(color)
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 rounded-lg mt-3", "elevation": 0, "style": self._empty_card_style(color)}, "content": [
            {"component": "div", "props": {"class": "d-flex align-center ga-3"}, "content": [
                {"component": "VIcon", "props": {"color": self._ui_color(color), "size": "large", "style": f"color:{tone['text']};"}, "text": icon},
                {"component": "div", "content": [
                    {"component": "div", "props": {"class": "text-body-2 font-weight-medium"}, "text": title},
                    *([{"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": subtitle}] if subtitle else [])
                ]}
            ]}
        ]}

    @staticmethod
    def _queue_stats(queue: Dict[str, Any]) -> Dict[str, int]:
        stats: Dict[str, int] = {}
        for item in (queue or {}).values():
            if not isinstance(item, dict):
                stats["unknown"] = stats.get("unknown", 0) + 1
                continue
            task_type = str(item.get("task_type") or "unknown")
            stats[task_type] = stats.get(task_type, 0) + 1
        return stats

    def _queue_display_items(self, queue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """把队列转换成页面展示项。

        实际队列仍保持单集独立任务；这里只把同一部剧、同一状态的单集刮削
        任务合并成一个页面卡片，避免一次入库多集时详情页过长。
        """
        if not isinstance(queue, dict) or not queue:
            return []
        sorted_items = sorted(
            queue.items(),
            key=lambda kv: self._to_float((kv[1] or {}).get("due_ts"), 0) if isinstance(kv[1], dict) else 0,
        )
        groups: Dict[str, Dict[str, Any]] = {}
        display: List[Dict[str, Any]] = []
        for key, item in sorted_items:
            if not isinstance(item, dict):
                item = {}
            task_type = str(item.get("task_type") or "")
            if task_type == "episode_scrape":
                show_root = str(item.get("show_root") or "").strip()
                status = str(item.get("status") or "")
                group_key = f"episode_group::{show_root}"
                group = groups.get(group_key)
                if not group:
                    group = {
                        "display_type": "episode_group",
                        "group_key": group_key,
                        "show_root": show_root,
                        "status": status,
                        "items": [],
                        "keys": [],
                        "due_values": [],
                        "min_due_ts": self._to_float(item.get("due_ts"), 0),
                    }
                    groups[group_key] = group
                    display.append(group)
                group["items"].append(item)
                group["keys"].append(str(key))
                if item.get("due_at"):
                    group["due_values"].append(str(item.get("due_at")))
                due_ts = self._to_float(item.get("due_ts"), 0)
                if due_ts and (not group.get("min_due_ts") or due_ts < self._to_float(group.get("min_due_ts"), 0)):
                    group["min_due_ts"] = due_ts
                continue
            display.append({"display_type": "single", "key": str(key), "item": item, "min_due_ts": self._to_float(item.get("due_ts"), 0)})
        return sorted(display, key=lambda x: self._to_float(x.get("min_due_ts"), 0))

    def _queue_grouped_display_items(self, display_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        configs = {
            "initial": {"label": "等待入库检查", "icon": "mdi-magnify-scan", "desc": "下一步会检查 STRM 目录刮削信息，缺失时再触发电影/电视剧刮削。"},
            "movie_retry": {"label": "等待电影文件重试", "icon": "mdi-movie-search-outline", "desc": "下一步会继续尝试 CD2 同名真实视频文件；仍不可见则按短期重试间隔继续等待。"},
            "tv_metadata_retry": {"label": "等待剧/季路径重试", "icon": "mdi-folder-refresh-outline", "desc": "下一步会继续预热 CD2 剧名目录或 Season 目录；就绪后触发目录刮削。"},
            "episode_scrape": {"label": "等待刮削任务", "icon": "mdi-play-circle-outline", "desc": "下一步会触发真实媒体文件刮削；成功后再创建10分钟检查。"},
            "tv_postcheck": {"label": "等待10分钟检查", "icon": "mdi-timer-check-outline", "desc": "下一步会确认图片/季信息是否已生成；提前手动检查仍缺图时会保留原到期时间。"},
            "tv_recheck": {"label": "等待长期复查", "icon": "mdi-calendar-clock", "desc": "下一步会再次确认缺图集，仍缺图才重新排单集刮削；实际刮削前删除 CD2 同名 nfo。"},
            "other": {"label": "其他任务", "icon": "mdi-dots-horizontal-circle-outline", "desc": "未知或兼容旧版本的任务。"},
        }
        order = ["initial", "movie_retry", "tv_metadata_retry", "episode_scrape", "tv_postcheck", "tv_recheck", "other"]
        bucket = {key: {**configs[key], "type": key, "items": [], "task_count": 0} for key in order}
        for display in display_items or []:
            task_type = self._display_task_type(display)
            key = task_type if task_type in bucket else "other"
            bucket[key]["items"].append(display)
            bucket[key]["task_count"] += self._display_task_count(display)
        return [bucket[key] for key in order if bucket[key].get("items")]

    @staticmethod
    def _display_task_type(display: Dict[str, Any]) -> str:
        if not isinstance(display, dict):
            return "other"
        if display.get("display_type") == "episode_group":
            return "episode_scrape"
        item = display.get("item") or {}
        if isinstance(item, dict):
            return str(item.get("task_type") or "other")
        return "other"

    @staticmethod
    def _display_task_count(display: Dict[str, Any]) -> int:
        if isinstance(display, dict) and display.get("display_type") == "episode_group":
            return len(display.get("items") or []) or 1
        return 1

    def _task_next_step(self, task_type: str, item: Dict[str, Any]) -> str:
        if task_type == "initial":
            media_type = str(item.get("media_type") or "").lower()
            if media_type == "movie":
                return "检查电影目录是否有 poster/fanart/backdrop 和 nfo；缺失时查找真实视频文件并触发 MP 刮削。"
            if media_type == "tv":
                return "检查本次入库季/集的刮削信息；缺图时排单集刮削任务，实际刮削前删除 CD2 同名 nfo。"
            return "检查 STRM 路径类型和刮削信息，符合条件后再登记后续任务。"
        if task_type == "movie_retry":
            return "继续尝试 CD2 同名真实视频文件的不同后缀；成功后触发电影刮削，失败则继续短期等待或最终记录失败。"
        if task_type == "episode_scrape":
            return "触发这一集真实媒体文件刮削；发送成功后再创建10分钟检查任务。"
        if task_type == "tv_postcheck":
            mode = str(item.get("mode") or "episodes")
            if mode == "season":
                return "检查本次涉及季的 season 海报、Season 目录 poster 和 season.nfo；仍缺失时记录异常但不提前进入10天复查。"
            return "检查本批单集图片是否已生成；到期检查仍缺图时才加入10天复查，手动提前检查仍缺图会继续等待原时间。"
        if task_type == "tv_recheck":
            return "重新确认缺图集；到期或手动立即复查时，仍缺图会重新排单集刮削任务，实际刮削前删除 CD2 同名 nfo。"
        if task_type == "tv_metadata_retry":
            return "重新预热/探测 CD2 剧名目录或 Season 目录；就绪后触发对应目录刮削。"
        return "按兼容逻辑处理该任务。"

    def _queue_display_panel(self, display: Dict[str, Any]) -> Dict[str, Any]:
        title, chips = self._queue_panel_summary(display)
        actions: List[Dict[str, Any]] = []
        if display.get("display_type") == "episode_group":
            show_root = str(display.get("show_root") or "")
            actions = [
                {"component": "VBtn", "props": {"variant": "flat", "color": self._ui_color("primary"), "size": "small", "prepend-icon": "mdi-play-circle-outline"}, "text": "立即执行整组", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_run_episode_group", "method": "get", "params": {"show_root": show_root, "status": "", "apikey": getattr(settings, "API_TOKEN", "")}}}},
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("warning"), "size": "small", "prepend-icon": "mdi-delete-outline"}, "text": "删除整组", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_delete_episode_group", "method": "get", "params": {"show_root": show_root, "status": "", "apikey": getattr(settings, "API_TOKEN", "")}}}},
            ]
        else:
            key = str(display.get("key") or "")
            item = display.get("item") or {}
            task_type = str(item.get("task_type") or "")
            run_tone = "warning" if task_type in ("tv_recheck", "movie_retry", "tv_metadata_retry") else "info"
            actions = [
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color(run_tone), "size": "small", "prepend-icon": "mdi-play-circle-outline"}, "text": ("立即检查" if task_type == "tv_postcheck" else ("立即复查" if task_type == "tv_recheck" else ("立即重试" if task_type in ("movie_retry", "tv_metadata_retry") else "立即执行"))), "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_run", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("warning"), "size": "small", "prepend-icon": "mdi-delete-outline"}, "text": "删除任务", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_delete", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
            ]
        return {"component": "VExpansionPanel", "props": {"style": self._expansion_panel_style()}, "content": [
            {"component": "VExpansionPanelTitle", "content": [
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2 w-100"}, "content": [
                    {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": [
                        {"component": "span", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                        {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-1 ml-2"}, "content": chips},
                    ]},
                    {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": actions},
                ]}
            ]},
            {"component": "VExpansionPanelText", "content": [self._queue_display_card(display)]}
        ]}

    def _queue_panel_summary(self, display: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
        if display.get("display_type") == "episode_group":
            items = [x for x in (display.get("items") or []) if isinstance(x, dict)]
            show_root = Path(str(display.get("show_root") or ""))
            show_name = show_root.name or str(show_root) or "同剧单集"
            status = str(display.get("status") or "")
            due_values = sorted(set([str(x) for x in (display.get("due_values") or []) if str(x or "").strip()]))
            chips = [self._chip("单集刮削", "primary")]
            if status:
                chips.append(self._chip(self._status_label(status), self._status_color(status)))
            if due_values:
                chips.append(self._chip(f"到期 {due_values[0]}" if len(due_values) == 1 else f"最早 {due_values[0]}", "info"))
            return f"{show_name}：单集刮削 {len(items)} 集", chips

        key = str(display.get("key") or "")
        item = display.get("item") or {}
        task_type = str(item.get("task_type") or "")
        status = str(item.get("status") or "")
        due_at = str(item.get("due_at") or "")
        chips = [self._chip(self._task_type_label(task_type), self._task_color(task_type))]
        if status:
            chips.append(self._chip(self._status_label(status), self._status_color(status)))
        if due_at:
            chips.append(self._chip(f"到期 {due_at}", "info"))
        return self._queue_title(key, item), chips

    def _queue_display_card(self, display: Dict[str, Any]) -> Dict[str, Any]:
        if display.get("display_type") == "episode_group":
            return self._episode_group_card(display)
        return self._task_row_card(str(display.get("key") or ""), display.get("item") or {})

    def _episode_group_card(self, group: Dict[str, Any]) -> Dict[str, Any]:
        items = [x for x in (group.get("items") or []) if isinstance(x, dict)]
        show_root = Path(str(group.get("show_root") or ""))

        keys = [str(x) for x in (group.get("keys") or [])]
        visible_items = items[:4]
        table_rows = [
            self._episode_table_row(keys[index] if index < len(keys) else "", item, show_root)
            for index, item in enumerate(visible_items)
        ]
        if len(items) > len(visible_items):
            table_rows.append({
                "component": "div",
                "props": {"style": self._table_footer_style()},
                "content": [
                    {"component": "span", "text": f"其余 {len(items) - len(visible_items)} 集已折叠，可展开查看全部"},
                    {"component": "VIcon", "props": {"size": "small", "class": "ml-1"}, "text": "mdi-chevron-down"}
                ]
            })

        return {"component": "div", "props": {"style": self._queue_table_style()}, "content": [
            {"component": "div", "props": {"style": self._queue_table_header_style()}, "content": [
                {"component": "div", "text": "集数"},
                {"component": "div", "text": "到期时间"},
                {"component": "div", "text": "刮削目标"},
                {"component": "div", "text": "状态"},
                {"component": "div", "text": "失败原因"},
                {"component": "div", "props": {"class": "text-center"}, "text": "操作"},
            ]},
            *table_rows
        ]}

    def _episode_table_row(self, key: str, item: Dict[str, Any], show_root: Path) -> Dict[str, Any]:
        ep = Path(str(item.get("episode_strm") or ""))
        label = self._episode_label(ep, show_root) if ep else (key or "未记录")
        status = str(item.get("status") or "")
        due_at = str(item.get("due_at") or "-")
        target = str(item.get("scrape_target") or item.get("scrape_dir") or "")
        reason = str(item.get("last_reason") or item.get("last_msg") or item.get("reason") or item.get("scrape_msg") or "").strip() or "-"
        return {"component": "div", "props": {"style": self._queue_table_row_style()}, "content": [
            {"component": "div", "props": {"class": "text-body-2"}, "text": label},
            {"component": "div", "props": {"class": "text-body-2"}, "text": due_at},
            {"component": "div", "props": {"class": "text-body-2 text-truncate"}, "text": self._short_path(target, 34) if target else "-"},
            {"component": "div", "content": [self._chip(self._status_label(status), self._status_color(status))]},
            {"component": "div", "props": {"class": "text-body-2 text-truncate"}, "text": self._short_path(reason, 34)},
            {"component": "div", "props": {"class": "d-flex justify-center ga-2"}, "content": [
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("info"), "size": "small"}, "text": "立即执行", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_run", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("warning"), "size": "small"}, "text": "删除任务", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_delete", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
            ] if key else []}
        ]}

    def _episode_task_detail_row(self, key: str, item: Dict[str, Any], show_root: Path) -> Dict[str, Any]:
        ep = Path(str(item.get("episode_strm") or ""))
        label = self._episode_label(ep, show_root) if ep else key
        status = str(item.get("status") or "")
        due_at = str(item.get("due_at") or "")
        target = str(item.get("scrape_target") or item.get("scrape_dir") or "")
        reason = str(item.get("last_reason") or item.get("last_msg") or item.get("reason") or item.get("scrape_msg") or "")
        detail_lines = [
            self._mini_line("到期时间", due_at or "未记录"),
            self._mini_line("刮削目标", self._short_path(target) if target else "未记录"),
            self._mini_line("当前状态", self._status_label(status)),
            self._mini_line("失败原因", reason or "无"),
        ]
        action_buttons: List[Dict[str, Any]] = []
        if key:
            action_buttons = [
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("info"), "size": "x-small", "prepend-icon": "mdi-play-circle-outline", "class": "ml-1"}, "text": "立即执行", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_run", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
                {"component": "VBtn", "props": {"variant": "tonal", "color": self._ui_color("warning"), "size": "x-small", "prepend-icon": "mdi-delete-outline", "class": "ml-1"}, "text": "删除任务", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_delete", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
            ]
        return {"component": "div", "props": {"class": "pa-2 rounded", "style": self._inline_row_style("info")}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2"}, "content": [
                {"component": "div", "props": {"class": "text-body-2 font-weight-medium"}, "text": label},
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-1"}, "content": [
                    self._chip(self._status_label(status), self._status_color(status)),
                    *([self._chip(f"到期 {due_at}", "info")] if due_at else []),
                    *action_buttons
                ]}
            ]},
            {"component": "div", "props": {"class": "mt-1"}, "content": detail_lines}
        ]}

    def _task_row_card(self, key: str, item: Dict[str, Any]) -> Dict[str, Any]:
        task_type = str(item.get("task_type") or "")
        status = str(item.get("status") or "")
        due_at = str(item.get("due_at") or "")
        strm = str(item.get("strm_path") or item.get("episode_strm") or item.get("show_root") or "")
        target = str(item.get("scrape_target") or item.get("scrape_dir") or "")

        detail_lines: List[Dict[str, Any]] = [
            self._mini_line("任务类型", self._task_type_label(task_type)),
        ]
        if status:
            detail_lines.append(self._mini_line("当前状态", self._status_label(status)))
        if due_at:
            detail_lines.append(self._mini_line("到期时间", due_at))
        if strm:
            detail_lines.append(self._mini_line("STRM", self._short_path(strm)))
        if target:
            detail_lines.append(self._mini_line("刮削目标", self._short_path(target)))

        if task_type == "tv_postcheck":
            mode = str(item.get("mode") or "")
            if mode:
                mode_label = "剧信息" if mode == "root" else ("整部剧" if mode == "show" else ("季信息" if mode == "season" else "入库单集"))
                detail_lines.append(self._mini_line("检查范围", mode_label))
            season_dirs = self._to_list(item.get("season_dirs") or [])
            if season_dirs:
                names = [Path(sd).name or str(sd) for sd in season_dirs]
                detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": "待检查季："})
                detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "info") for name in names]})
            episodes = self._to_list(item.get("episodes") or [])
            if episodes:
                show_root = Path(str(item.get("show_root") or ""))
                names = [self._episode_label(Path(ep), show_root) for ep in episodes]
                detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": "待检查集："})
                detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "info") for name in names]})

        if task_type == "tv_recheck":
            preview = item.get("missing_preview") or {}
            if preview.get("exists"):
                total = int(preview.get("total") or 0)
                detail_lines.append(self._mini_line("当前缺图", f"{total} 集" if total else "暂未发现，到期会复查全部 STRM"))
                names = preview.get("names") or []
                # 兼容旧数据：老版本可能只保存 total，没有保存 names。
                if total and not names:
                    names = preview.get("labels") or preview.get("items") or []
                if names:
                    detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": "缺图集："})
                    detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "warning") for name in names]})
                    if preview.get("truncated"):
                        detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": "当前任务来自旧缓存，已展示保存的缺图集；到期仍会检查全部 STRM。"})
                elif total:
                    detail_lines.append({"component": "VAlert", "props": {**self._ui_alert("warning"), "density": "compact", "class": "mt-2", "text": "当前任务来自旧缓存，只记录了缺图数量；保存一次配置或等待下次事件后会刷新具体集数。"}})
            else:
                detail_lines.append(self._mini_line("当前缺图", "剧名目录暂不可访问，到期会再次检查"))

        next_step = self._task_next_step(task_type, item)
        if next_step:
            detail_lines.append(self._mini_line("下一步", next_step))
        msg = str(item.get("last_msg") or "")
        if msg:
            detail_lines.append(self._mini_line("说明", msg))

        return {"component": "div", "props": {"style": self._queue_detail_body_style()}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-column ga-2"}, "content": detail_lines}
        ]}

    def _history_row_card(self, item: Dict[str, Any]) -> Dict[str, Any]:
        title = self._short_path(str(item.get("scope") or item.get("folder") or "处理记录"))
        action = self._action_label(str(item.get("action") or ""))
        t = str(item.get("time") or "")
        outcome, outcome_color = self._history_outcome(item)
        result_chip = self._chip(outcome, outcome_color)

        detail_lines: List[Dict[str, Any]] = []
        folder = str(item.get("folder") or "")
        if folder:
            detail_lines.append(self._mini_line("目标", self._short_path(folder)))
        scrape_msg = str(item.get("scrape_msg") or "")
        if scrape_msg:
            detail_lines.append(self._mini_line("刮削", scrape_msg))
        note = str(item.get("note") or "")
        if note:
            detail_lines.append(self._mini_line("说明", note))
        detail_lines.extend(self._history_episode_lines(item))
        dup = int(item.get("duplicate_count") or 0)
        if dup > 1:
            detail_lines.append(self._mini_line("重复合并", f"{dup} 次"))

        return {"component": "VCard", "props": {"variant": "flat", "color": "surface", "class": "pa-3 rounded-lg", "elevation": 0, "style": self._detail_card_style(outcome_color)}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2"}, "content": [
                {"component": "div", "content": [
                    {"component": "div", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                    {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": f"{t} · {action}"}
                ]},
                {"component": "div", "content": [result_chip]}
            ]},
            {"component": "div", "props": {"class": "mt-2"}, "content": detail_lines}
        ]}

    def _history_display_groups(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        groups: List[Dict[str, Any]] = []
        by_key: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        aggregate_outcomes = {"刮削成功", "复查完成", "复查结束", "等待长期复查", "待补刮削", "已完整", "已触发刮削", "需关注", "刮削失败"}
        for index, item in enumerate(items or []):
            if not isinstance(item, dict):
                continue
            action = str(item.get("action") or "")
            outcome, outcome_color = self._history_outcome(item)
            media_type, media_name, category, scope_label, identity = self._notification_media(item, action)
            labels, episode_total = self._history_item_episode_summary(item)
            if labels:
                category = "episode"
            if identity and outcome in aggregate_outcomes:
                key = (outcome, media_type, media_name, identity)
            else:
                key = (action, str(item.get("scope") or ""), str(item.get("folder") or ""), str(index))
            group = by_key.get(key)
            if not group:
                group = {
                    "items": [],
                    "media_type": media_type,
                    "media_name": media_name,
                    "identity": identity,
                    "outcome": outcome,
                    "outcome_color": outcome_color,
                    "categories": [],
                    "scope_labels": [],
                    "episode_labels": [],
                    "episode_total": 0,
                    "time": str(item.get("time") or ""),
                    "fallback_title": self._short_path(str(item.get("scope") or item.get("folder") or "处理记录")),
                }
                by_key[key] = group
                groups.append(group)
            group["items"].append(item)
            categories = group.get("categories") or []
            if category and category not in categories:
                categories.append(category)
            group["categories"] = categories
            scope_labels = group.get("scope_labels") or []
            if scope_label and scope_label not in scope_labels:
                scope_labels.append(scope_label)
            group["scope_labels"] = scope_labels
            old_labels = group.get("episode_labels") or []
            for label in labels:
                if label and label not in old_labels:
                    old_labels.append(label)
            group["episode_labels"] = old_labels
            group["episode_total"] = max(int(group.get("episode_total") or 0), int(episode_total or 0), len(old_labels))
        return groups

    def _history_item_episode_summary(self, item: Dict[str, Any]) -> Tuple[List[str], int]:
        labels: List[str] = []
        total = 0
        for prefix in ("missing", "checked", "scraped"):
            current = self._history_episode_labels(item, prefix, limit=200)
            if current:
                labels = current
                try:
                    total = int(item.get(f"{prefix}_episode_total") or len(current) or 0)
                except Exception:
                    total = len(current)
                break
        if not labels:
            inferred = self._infer_history_checked_episode_labels(item, limit=200)
            if inferred:
                total, labels = inferred
        return labels, max(total, len(labels))

    def _history_group_panel(self, group: Dict[str, Any]) -> Dict[str, Any]:
        title = self._history_group_title(group)
        outcome = str(group.get("outcome") or "记录")
        color = str(group.get("outcome_color") or "info")
        items = [x for x in (group.get("items") or []) if isinstance(x, dict)]
        chips = [
            self._chip(outcome, color),
        ]
        episode_total = int(group.get("episode_total") or 0)
        if episode_total:
            chips.append(self._chip(f"{episode_total}/{episode_total}", "info"))
        categories = self._to_list(group.get("categories") or [])
        merged = len(items) > 1 or episode_total > 1 or len(categories) > 1
        if merged and self._history_outcome_sends_notification(outcome):
            chips.append(self._chip("已合并通知", "primary"))
        elif len(items) > 1:
            chips.append(self._chip(f"{len(items)} 条记录", "info"))
        elif not episode_total:
            chips.append(self._chip(f"{len(items)} 条", "info"))
        t = str(group.get("time") or "")
        return {"component": "VExpansionPanel", "props": {"style": self._expansion_panel_style()}, "content": [
            {"component": "VExpansionPanelTitle", "content": [
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2 w-100"}, "content": [
                    {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": [
                        {"component": "span", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                        *([{"component": "span", "props": {"class": "text-caption text-medium-emphasis ml-2"}, "text": t}] if t else [])
                    ]},
                    {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-1"}, "content": chips}
                ]}
            ]},
            {"component": "VExpansionPanelText", "content": [self._history_group_detail(group, items)]}
        ]}

    @staticmethod
    def _history_outcome_sends_notification(outcome: str) -> bool:
        return str(outcome or "") in {
            "刮削成功",
            "复查完成",
            "复查结束",
            "等待长期复查",
            "待补刮削",
            "需关注",
            "刮削失败",
        }

    def _history_group_detail(self, group: Dict[str, Any], items: List[Dict[str, Any]]) -> Dict[str, Any]:
        lines = [self._history_detail_line(item) for item in items[:4]]
        labels = [str(x) for x in (group.get("episode_labels") or []) if str(x or "").strip()]
        chips = self._episode_chip_strip(labels, int(group.get("episode_total") or len(labels) or 0))
        return {"component": "div", "props": {"style": self._history_body_style()}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-column ga-2"}, "content": lines},
            *([{"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-3 mt-2 pl-9"}, "content": chips}] if chips else [])
        ]}

    def _history_detail_line(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {"component": "div", "props": {"class": "d-flex align-center ga-2"}, "content": [
            {"component": "VIcon", "props": {"color": self._ui_color("success"), "size": "small"}, "text": "mdi-check-circle"},
            {"component": "span", "props": {"class": "text-body-2"}, "text": self._history_detail_line_text(item)}
        ]}

    def _history_detail_line_text(self, item: Dict[str, Any]) -> str:
        action = self._action_label(str(item.get("action") or ""))
        labels = []
        for prefix in ("checked", "scraped", "missing"):
            labels = self._history_episode_labels(item, prefix, limit=200)
            if labels:
                break
        if labels:
            return f"{action}：{self._episode_range_text(labels)} 已完成"
        msg = str(item.get("scrape_msg") or item.get("note") or "").strip()
        return f"{action}：{msg}" if msg else f"{action}：完成"

    @staticmethod
    def _episode_range_text(labels: List[str]) -> str:
        values = [str(x) for x in labels or [] if str(x or "").strip()]
        if not values:
            return ""
        if len(values) == 1:
            return values[0]
        return f"{values[0]}-{values[-1]}"

    def _episode_chip_strip(self, labels: List[str], total: int = 0) -> List[Dict[str, Any]]:
        values = [str(x) for x in labels or [] if str(x or "").strip()]
        if not values:
            return []
        if len(values) > 6:
            shown = values[:5] + ["..."] + values[-1:]
        else:
            shown = values
        chips: List[Dict[str, Any]] = []
        for value in shown:
            if value == "...":
                chips.append({"component": "span", "props": {"class": "font-weight-bold mx-2"}, "text": "..."})
            else:
                chips.append(self._chip(value, "success"))
        return chips

    def _history_group_title(self, group: Dict[str, Any]) -> str:
        media_name = str(group.get("media_name") or group.get("fallback_title") or "处理记录")
        outcome = str(group.get("outcome") or "记录")
        categories = self._to_list(group.get("categories") or [])
        episode_total = int(group.get("episode_total") or 0)
        parts: List[str] = []
        for category in categories:
            if category == "episode" and episode_total:
                continue
            label = self._notification_category_label(category)
            if label and label not in parts:
                parts.append(label)
        if episode_total:
            parts.append(f"{episode_total} 集")
        if not parts:
            parts.append(outcome)
        suffix_map = {
            "刮削成功": "检查完成",
            "复查完成": "复查完成",
            "复查结束": "复查结束",
            "等待长期复查": "等待长期复查",
            "待补刮削": "已排队补刮",
            "已完整": "已完整",
            "已触发刮削": "已触发刮削",
            "需关注": "需关注",
            "刮削失败": "失败",
        }
        suffix = suffix_map.get(outcome, outcome)
        return f"{media_name}：{' + '.join(parts)}{suffix}"

    def _ui_color(self, color: str = "info") -> str:
        if str(getattr(self, "DETAIL_COLOR_MODE", "color") or "color") == "plain":
            return "info"
        palette = {
            "primary": "primary",
            "info": "info",
            "success": "success",
            "warning": "warning",
            "orange": "warning",
            "purple": "primary",
            "error": "warning",
            "blue": "info",
            "green": "success",
            "indigo": "primary",
            "blue-grey": "info",
            "amber": "warning",
            "teal": "success",
            "red": "warning",
            "deep-purple": "primary",
            "grey": "info",
        }
        key = str(color or "info")
        return palette.get(key, key)

    def _tone_palette(self, color: str = "info") -> Dict[str, str]:
        key = self._ui_color(color)
        palettes = {
            "info": {"soft": "#e7f7ff", "tint": "#e7f7ff", "border": "#b8e7ff", "text": "#16b1ff", "shadow": "rgba(22, 177, 255, 0.12)"},
            "success": {"soft": "#eef9e5", "tint": "#eef9e5", "border": "#c9edb8", "text": "#43c431", "shadow": "rgba(67, 196, 49, 0.12)"},
            "warning": {"soft": "#fff6e4", "tint": "#fff0d1", "border": "#ffdfa3", "text": "#ff9800", "shadow": "rgba(255, 152, 0, 0.12)"},
            "primary": {"soft": "rgba(var(--v-theme-primary),0.12)", "tint": "rgba(var(--v-theme-primary),0.12)", "border": "rgba(var(--v-theme-primary),0.26)", "text": "rgb(var(--v-theme-primary))", "shadow": "rgba(var(--v-theme-primary),0.12)"},
        }
        return palettes.get(key, palettes["info"])

    def _page_style(self) -> str:
        if str(getattr(self, "DETAIL_COLOR_MODE", "color") or "color") == "plain":
            return "background:#ffffff;border-radius:10px;"
        return "background:#f7f7f9;border-radius:10px;color:#3f3b48;"

    def _section_card_style(self, color: str = "info") -> str:
        return "background:#ffffff;border:0;box-shadow:0 2px 10px rgba(15,23,42,0.06);border-radius:16px;"

    def _detail_card_style(self, color: str = "info") -> str:
        tone = self._tone_palette(color)
        return f"background:#ffffff;border:1px solid {tone['border']};box-shadow:none;"

    def _empty_card_style(self, color: str = "info") -> str:
        tone = self._tone_palette(color)
        return f"background:{tone['soft']};border:0;color:#4f4a57;"

    def _inline_row_style(self, color: str = "info") -> str:
        tone = self._tone_palette(color)
        return f"background:#ffffff;border:1px solid {tone['border']};"

    @staticmethod
    def _expansion_panel_style() -> str:
        return "background:#ffffff;border:1px solid #e1e6ef;border-radius:6px;overflow:hidden;margin-bottom:8px;"

    @staticmethod
    def _panel_header_style() -> str:
        return "min-height:56px;padding:10px 16px;background:#ffffff;"

    @staticmethod
    def _queue_detail_body_style() -> str:
        return "padding:12px 22px;background:#ffffff;border-top:1px solid #e1e6ef;"

    @staticmethod
    def _queue_table_style() -> str:
        return "border-top:1px solid #e1e6ef;background:#ffffff;"

    @staticmethod
    def _queue_table_header_style() -> str:
        return "display:grid;grid-template-columns:1.1fr 1.2fr 2.5fr 1.4fr 2.2fr 2.5fr;gap:8px;align-items:center;padding:12px 22px;background:#fbfcfe;border-bottom:1px solid #e1e6ef;color:#334155;font-size:13px;font-weight:500;"

    @staticmethod
    def _queue_table_row_style() -> str:
        return "display:grid;grid-template-columns:1.1fr 1.2fr 2.5fr 1.4fr 2.2fr 2.5fr;gap:8px;align-items:center;min-height:46px;padding:6px 22px;border-bottom:1px solid #e8ecf3;"

    @staticmethod
    def _table_footer_style() -> str:
        return "display:flex;align-items:center;justify-content:center;min-height:42px;color:#334155;border-bottom:1px solid #e8ecf3;font-size:14px;"

    @staticmethod
    def _history_body_style() -> str:
        return "padding:0 24px 12px 54px;background:#ffffff;"

    def _chip_style(self, color: str = "info") -> str:
        tone = self._tone_palette(color)
        return f"background:{tone['soft']};border:0;color:{tone['text']};"

    def _ui_alert(self, color: str = "info") -> Dict[str, Any]:
        return {"color": self._ui_color(color), "variant": "tonal"}

    def _chip(self, text: str, color: str = "info") -> Dict[str, Any]:
        return {"component": "VChip", "props": {"color": self._ui_color(color), "variant": "tonal", "size": "small", "class": "mr-1 mb-1 font-weight-medium", "style": self._chip_style(color)}, "text": text}

    @staticmethod
    def _mini_line(label: str, value: str) -> Dict[str, Any]:
        return {"component": "div", "props": {"class": "text-caption mb-1"}, "content": [
            {"component": "span", "props": {"class": "text-medium-emphasis"}, "text": f"{label}："},
            {"component": "span", "text": value}
        ]}

    def _history_episode_lines(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """历史记录里展示具体集数，避免只看到“检查 X 集”但不知道是哪几集。"""
        if not isinstance(item, dict):
            return []

        groups = [
            ("missing", "缺图集", "warning"),
            ("checked", "检查集", "info"),
            ("scraped", "刮削集", "primary"),
        ]
        for prefix, label, color in groups:
            labels = self._history_episode_labels(item, prefix, limit=200)
            total = int(item.get(f"{prefix}_episode_total") or len(labels) or 0)
            if not labels:
                continue
            title = label if total <= len(labels) else f"{label}（前 {len(labels)} / 共 {total} 集）"
            lines: List[Dict[str, Any]] = [
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": f"{title}："},
                {"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), color) for name in labels]},
            ]
            if total > len(labels):
                lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": f"仅展示前 {len(labels)} 集。"})
            return lines

        # 兼容旧历史：旧版本没有保存具体集数。只有当当前目录 STRM 总数与记录里的“检查 X 集”一致时，才推断展示，避免误导。
        inferred = self._infer_history_checked_episode_labels(item, limit=200)
        if inferred:
            total, labels = inferred
            title = "检查集（按当前目录推断）" if total <= len(labels) else f"检查集（按当前目录推断，前 {len(labels)} / 共 {total} 集）"
            return [
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": f"{title}："},
                {"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "info") for name in labels]},
            ]
        return []

    def _history_episode_labels(self, item: Dict[str, Any], prefix: str, limit: int = 12) -> List[str]:
        labels = self._to_list(item.get(f"{prefix}_episode_labels") or [])
        if labels:
            return labels[:max(int(limit or 0), 0)]
        show_root = Path(str(item.get("scope") or ""))
        paths = self._unique_episode_paths(item.get(f"{prefix}_episodes") or [])
        if paths:
            return [self._episode_label(ep, show_root) for ep in paths[:max(int(limit or 0), 0)]]
        return []

    def _infer_history_checked_episode_labels(self, item: Dict[str, Any], limit: int = 12) -> Optional[Tuple[int, List[str]]]:
        action = str(item.get("action") or "")
        if action not in {"tv_postcheck_complete", "tv_recheck_complete"}:
            return None
        msg = str(item.get("scrape_msg") or "")
        match = re.search(r"检查\s*(\d+)\s*集", msg)
        if not match:
            return None
        checked_count = int(match.group(1) or 0)
        if checked_count <= 0 or checked_count > 50:
            return None
        show_root = Path(str(item.get("scope") or ""))
        if not show_root.exists() or not show_root.is_dir():
            return None
        episodes = self._list_strm_files(show_root)
        if len(episodes) != checked_count:
            return None
        take = max(int(limit or 0), 0)
        return len(episodes), [self._episode_label(ep, show_root) for ep in episodes[:take]]

    def _episode_history_payload(self, show_root: Path, episodes: Any, prefix: str = "checked", limit: int = 200) -> Dict[str, Any]:
        eps = self._unique_episode_paths(episodes)
        take = max(int(limit or 0), 0)
        return {
            f"{prefix}_episodes": [str(ep) for ep in eps[:take]],
            f"{prefix}_episode_labels": [self._episode_label(ep, show_root) for ep in eps[:take]],
            f"{prefix}_episode_total": len(eps),
            f"{prefix}_episode_truncated": len(eps) > take,
        }

    def _status_label(self, status: str) -> str:
        return self.STATUS_LABELS.get(status, status or "等待中")

    @staticmethod
    def _status_color(status: str) -> str:
        palette = {
            "waiting_initial_check": "success",
            "waiting_episode_scrape": "success",
            "waiting_tv_postcheck": "warning",
            "waiting_tv_recheck": "warning",
            "waiting_movie_retry": "warning",
            "waiting_tv_metadata_retry": "warning",
            "waiting_task_exception_retry": "warning",
        }
        return palette.get(str(status or ""), "info")

    def _task_type_label(self, task_type: str) -> str:
        return self.TASK_TYPE_LABELS.get(task_type, task_type or "任务")

    def _task_color(self, task_type: str) -> str:
        return self.TASK_COLORS.get(task_type, "info")

    def _action_label(self, action: str) -> str:
        return self.ACTION_LABELS.get(action, action or "记录")

    def _history_outcome(self, item: Dict[str, Any]) -> Tuple[str, str]:
        """统一详情页结果语义，并兼容没有新字段的旧历史记录。"""
        if not isinstance(item, dict):
            return "记录", "info"
        scrape = item.get("scrape")
        action = str(item.get("action") or "").lower()
        if scrape is True:
            return "已触发刮削", "info"
        if scrape is False:
            return "刮削失败", "warning"
        if action in {"tv_postcheck_missing_schedule_recheck"}:
            return "等待长期复查", "warning"
        if action in {"tv_recheck_missing_episodes_schedule_scrape"}:
            return "待补刮削", "warning"
        if action in {"tv_postcheck_complete", "tv_root_postcheck_complete", "tv_season_postcheck_complete"}:
            return "刮削成功", "success"
        if action == "tv_recheck_complete":
            return "复查完成", "success"
        if action == "tv_recheck_stale_complete":
            return "复查结束", "info"
        if "retry_wait" in action or "retry_scheduled" in action or "nfo_delete_retry_wait" in action or action.endswith("_retry"):
            return "等待重试", "warning"
        if self._is_history_failure(item):
            return "需关注", "warning"
        if "complete_skip" in action or "image_exists_skip" in action:
            return "已完整", "success"
        return "记录", "info"

    @staticmethod
    def _short_path(path_text: str, max_len: int = 86) -> str:
        text = str(path_text or "")
        if len(text) <= max_len:
            return text
        return "…" + text[-max_len:]

    @staticmethod
    def _history_for_display(history: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        seen = set()
        for item in reversed(history or []):
            key = (
                str(item.get("action") or ""),
                str(item.get("scope") or ""),
                str(item.get("folder") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
            if len(result) >= limit:
                break
        return result

    def _history_failure_for_display(self, history: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        seen = set()
        for item in reversed(history or []):
            if not isinstance(item, dict) or not self._is_history_failure(item):
                continue
            key = (
                str(item.get("action") or ""),
                str(item.get("scope") or ""),
                str(item.get("folder") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
            if len(result) >= limit:
                break
        return result

    @staticmethod
    def _is_history_failure(item: Dict[str, Any]) -> bool:
        if item.get("scrape") is False:
            return True
        action = str(item.get("action") or "").lower()
        failure_words = ["failed", "target_missing", "show_missing", "manual_missing", "postcheck_incomplete", "recheck_missing", "missing_schedule_recheck"]
        return any(word in action for word in failure_words)

    def stop_service(self):
        try:
            for timer in list(getattr(self, "_timers", []) or []):
                try:
                    timer.cancel()
                except Exception:
                    pass
            self._timers = []
            self._next_timer_due_ts = 0
        except Exception:
            pass
        self._storagechain = None

    @eventmanager.register(EventType.WebhookMessage)
    def on_webhook_message(self, event):
        """监听 MP 全局 WebhookMessage。媒体库服务器通知插件也监听这个事件。"""
        if not self._enabled:
            return
        try:
            event_info = getattr(event, "event_data", None)
            if not event_info:
                return
            payload = self._object_to_dict(event_info)
            event_name = str(self._get_obj_value(event_info, "event") or payload.get("event") or "").strip()
            channel_raw = self._get_obj_value(event_info, "channel") or payload.get("channel") or ""
            channel = str(channel_raw or "").strip().lower()
            if event_name and not self._is_item_added_event(event_name):
                logger.debug(f"监控strm刮削网盘：收到非入库 Webhook 事件，已忽略：event={event_name or '-'}，channel={channel or '-'}")
                return

            # 不再只限制 channel=emby。部分媒体库或媒体服务器插件可能传入 jellyfin、plex、media_server 等 channel。
            # 是否登记任务由后续路径、媒体库映射和 STRM 根路径判断决定，并在 INFO 日志中给出原因。
            raw_path = self._event_info_path(event_info, payload)
            if not raw_path:
                item_id = str(self._get_obj_value(event_info, "item_id") or self._get_obj_value(event_info, "itemid") or payload.get("item_id") or payload.get("itemid") or "").strip()
                server_name = str(self._get_obj_value(event_info, "server_name") or self._get_obj_value(event_info, "source") or payload.get("server_name") or payload.get("source") or self._media_server or "").strip()
                item_info = self._query_media_item_info(item_id=item_id, server_name=server_name)
                if item_info:
                    payload.setdefault("_mp_item_info", item_info)
                    raw_path = self._extract_payload_path(item_info)

            library_candidates = self._extract_library_candidates(payload)
            library_text = "、".join([str(x) for x in library_candidates if str(x or "").strip()]) or "-"
            event_debug = (
                f"event={event_name or '-'}，channel={channel or '-'}，"
                f"path={raw_path or '-'}，library={library_text}"
            )
            logger.debug(f"监控strm刮削网盘：收到入库事件：{event_debug}")

            if not raw_path:
                logger.info(f"监控strm刮削网盘：入库事件判断结果：已忽略，原因：未取得媒体路径（{event_debug}）")
                return

            result = self._register_incoming_path(payload=payload, raw_path=raw_path, event_name=event_name or "library.new", source="mp_webhook_event")
            if result.get("duplicate"):
                logger.debug(f"监控strm刮削网盘：入库事件判断结果：已登记（重复合并），任务：{result.get('task')}（{event_debug}）")
            elif result.get("success") and not result.get("ignored"):
                logger.info(f"监控strm刮削网盘：入库事件判断结果：已登记，任务：{result.get('task')}")
            else:
                reason = self._event_ignore_reason(result)
                logger.info(f"监控strm刮削网盘：入库事件判断结果：已忽略，原因：{reason}（{event_debug}）")
        except Exception as err:
            logger.error(f"监控strm刮削网盘：处理 MP 全局 Webhook 事件失败：{err}\n{traceback.format_exc()}")

    def _register_incoming_path(self, payload: Any, raw_path: str, event_name: str = "", source: str = "") -> Dict[str, Any]:
        lib_allowed, lib_msg, lib_info = self._library_allowed(payload, raw_path)
        if not lib_allowed:
            logger.debug(f"监控strm刮削网盘：媒体库过滤忽略：{lib_msg}，路径：{raw_path}")
            return {"success": True, "ignored": True, "message": lib_msg, "library_info": lib_info}

        raw_path = self._normalise_path_text(raw_path)
        info = self._analyse_incoming_strm_path(raw_path)
        if not info.get("success"):
            return {"success": False, "message": info.get("message") or "路径分析失败", "raw_path": raw_path}

        now_ts = time.time()
        now_iso = self._now_iso()

        # 电影按影片路径登记；电视剧按“剧名目录 + 季目录”合并。
        # 这样先入第一季、后入第二季时，第一季的刮削后检查不会重新扫描整部剧而把第二季一起算进去。
        season_scope = ""
        if info.get("media_type") == "tv":
            show_root_path = Path(str(info.get("show_root") or ""))
            episode_path = Path(str(info.get("episode_strm") or raw_path))
            season_scope = str(self._tv_episode_season_scope(show_root_path, episode_path))
            task_key = f"initial_tv::{info.get('show_root')}::{season_scope}"
        else:
            task_key = f"initial::{raw_path}"

        with self._lock:
            state = self._load_state()
            queue = state.setdefault("queue", {})

            # 电影真实文件等待期间可能再次收到入库事件。直接唤醒已有重试任务，
            # 不再额外创建 initial 任务，避免同一轮向 MP 发送两次刮削事件。
            if info.get("media_type") == "movie":
                movie_identity = self._normalise_path_text(str(info.get("strm_path") or raw_path))
                retry_key = ""
                retry_task = None
                for candidate_key, candidate_task in queue.items():
                    if not isinstance(candidate_task, dict) or str(candidate_task.get("task_type") or "") != "movie_retry":
                        continue
                    candidate_identity = self._normalise_path_text(str(
                        candidate_task.get("movie_strm")
                        or candidate_task.get("strm_path")
                        or candidate_task.get("raw_path")
                        or ""
                    ))
                    if candidate_identity == movie_identity:
                        retry_key = str(candidate_key)
                        retry_task = candidate_task
                        break
                if retry_task is not None:
                    incoming_due_ts = now_ts + max(self._initial_check_delay_seconds, 0)
                    old_due_ts = self._to_float(retry_task.get("due_ts"), incoming_due_ts)
                    due_ts = min(old_due_ts if old_due_ts > 0 else incoming_due_ts, incoming_due_ts)
                    retry_task.update({
                        "raw_path": raw_path,
                        "movie_strm": str(info.get("strm_path") or raw_path),
                        "strm_path": str(info.get("strm_path") or raw_path),
                        "movie_dir": str(info.get("movie_dir") or retry_task.get("movie_dir") or ""),
                        "scrape_dir": str(info.get("scrape_dir") or retry_task.get("scrape_dir") or ""),
                        "last_webhook_at": now_iso,
                        "library_info": lib_info,
                        "due_ts": due_ts,
                        "due_at": self._ts_to_str(due_ts),
                        "status": "waiting_movie_retry",
                        "last_msg": "收到新的电影入库事件，已合并到现有真实文件重试任务。",
                    })
                    retry_task["duplicate_count"] = int(retry_task.get("duplicate_count") or 0) + 1
                    self._drop_competing_movie_tasks(state, Path(movie_identity), keep_task=retry_task)
                    self.save_data("state", state)
                    self._restore_queue_timers(state)
                    return {"success": True, "duplicate": True, "message": "电影入库事件已合并到重试任务", "task": retry_key}

            existing = queue.get(task_key)
            if existing:
                existing["last_webhook_at"] = now_iso
                existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
                episodes = existing.setdefault("episodes", [])
                ep = str(info.get("episode_strm") or "")
                if ep and ep not in episodes:
                    episodes.append(ep)
                if info.get("media_type") == "tv":
                    existing["missing_preview"] = self._preview_from_episodes(Path(str(info.get("show_root") or "")), episodes, limit=8)
                # 保留最早的初次检查时间，不因重复入库事件反复后延。
                self.save_data("state", state)
                self._restore_queue_timers(state)
                return {"success": True, "duplicate": True, "message": "重复入库事件已合并", "task": task_key}

            task = {
                "task_type": "initial",
                "key": task_key,
                "raw_path": raw_path,
                "media_type": info.get("media_type"),
                "strm_path": info.get("strm_path"),
                "check_dir": info.get("check_dir"),
                "show_root": info.get("show_root"),
                "season_scope": season_scope,
                "movie_dir": info.get("movie_dir"),
                "scrape_dir": info.get("scrape_dir"),
                "episode_strm": info.get("episode_strm"),
                "episodes": [info.get("episode_strm")] if info.get("episode_strm") else [],
                "first_seen_ts": now_ts,
                "first_seen": now_iso,
                "due_ts": now_ts + max(self._initial_check_delay_seconds, 0),
                "due_at": self._ts_to_str(now_ts + max(self._initial_check_delay_seconds, 0)),
                "status": "waiting_initial_check",
                "source": source or "mp_webhook_event",
                "event": event_name,
                "library_info": lib_info,
                "last_msg": f"等待 {max(self._initial_check_delay_seconds, 0)} 秒后检查 STRM 库刮削信息"
            }
            if info.get("media_type") == "tv":
                # 初始任务只展示本次季/批次的入库集数，不再用整部剧缺图缓存误导。
                task["missing_preview"] = self._preview_from_episodes(Path(str(info.get("show_root") or "")), task.get("episodes") or [], limit=8)
            queue[task_key] = task
            self.save_data("state", state)
        self._schedule_delayed_check(max(self._initial_check_delay_seconds, 0))
        return {"success": True, "message": "已登记入库任务", "task": task_key, "info": info}

    def _drop_competing_movie_tasks(self, state: Dict[str, Any], movie_strm: Path, keep_task: Dict[str, Any] = None) -> int:
        """清理同一电影的其他旧任务，兼容升级前已经重复存在的队列。"""
        queue = state.setdefault("queue", {})
        identity = self._normalise_path_text(str(movie_strm or ""))
        if not identity or not isinstance(queue, dict):
            return 0
        removed = 0
        for key, item in list(queue.items()):
            if not isinstance(item, dict):
                continue
            if item is keep_task:
                continue
            task_type = str(item.get("task_type") or "initial")
            if task_type not in {"initial", "movie_retry"}:
                continue
            if str(item.get("media_type") or "movie").lower() != "movie":
                continue
            candidate = self._normalise_path_text(str(
                item.get("movie_strm") or item.get("strm_path") or item.get("raw_path") or ""
            ))
            if candidate == identity:
                queue.pop(key, None)
                removed += 1
        return removed

    def _process_queue_task(self, key: str, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        task_type = str(task.get("task_type") or "initial")
        if task_type == "initial":
            return self._process_initial_task(task, state)
        if task_type == "movie_retry":
            return self._process_movie_retry_task(task, state)
        if task_type == "tv_metadata_retry":
            return self._process_tv_metadata_retry_task(task, state)
        if task_type == "episode_scrape":
            return self._process_episode_scrape_task(task, state)
        if task_type == "tv_postcheck":
            return self._process_tv_postcheck_task(task, state, manual=manual)
        if task_type == "tv_recheck":
            return self._process_tv_recheck_task(task, state, manual=manual)
        return {"success": True, "action": "drop_unknown_task", "scope": key, "folder": key, "message": f"未知任务类型：{task_type}"}

    def _queue_task_exception_result(self, key: str, task: Dict[str, Any], state: Dict[str, Any], err: Exception) -> Dict[str, Any]:
        """隔离单个自动任务异常；短期重试耗尽后记录并通知最终失败。"""
        delays = self.DEFAULT_QUEUE_EXCEPTION_RETRY_DELAYS_SECONDS
        retry_count = max(int(self._to_float(task.get("exception_retry_count"), 0)), 0)
        scope = str(
            task.get("episode_strm")
            or task.get("movie_dir")
            or task.get("show_root")
            or task.get("scope_dir")
            or task.get("check_dir")
            or task.get("strm_path")
            or task.get("raw_path")
            or key
        )
        folder = str(task.get("scrape_target") or task.get("scope_dir") or scope)
        task_type = str(task.get("task_type") or "")
        media_type = str(task.get("media_type") or "").lower()
        if media_type not in {"movie", "tv"}:
            media_type = "movie" if task_type == "movie_retry" or task.get("movie_dir") else "tv"
        error_text = f"{type(err).__name__}: {err}"[:240]
        if retry_count < len(delays):
            due_ts = time.time() + delays[retry_count]
            task["exception_retry_count"] = retry_count + 1
            task["exception_last_error"] = error_text
            task.setdefault("status_before_exception", str(task.get("status") or ""))
            task["status"] = "waiting_task_exception_retry"
            task["due_ts"] = due_ts
            task["due_at"] = self._ts_to_str(due_ts)
            task["last_msg"] = f"任务执行异常，等待自动重试：{task['due_at']}"
            result = {
                "time": self._now_iso(), "action": "queue_task_exception_wait", "scope": scope, "folder": folder,
                "scrape": None, "scrape_msg": f"任务执行异常，已安排第 {retry_count + 1} 次自动重试：{error_text}",
                "exception": error_text,
                "media_type": media_type,
            }
            self._append_history(state, result)
            return {"remove": False, **result}

        result = {
            "time": self._now_iso(), "action": "queue_task_exception_failed", "scope": scope, "folder": folder,
            "scrape": False, "scrape_msg": f"任务连续异常，已停止自动重试：{error_text}",
            "exception": error_text,
            "media_type": media_type,
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    @staticmethod
    def _restore_task_status_before_exception_retry(task: Dict[str, Any]):
        previous_status = str(task.pop("status_before_exception", "") or "")
        if previous_status:
            task["status"] = previous_status

    @staticmethod
    def _clear_task_exception_retry(task: Dict[str, Any]):
        task.pop("exception_retry_count", None)
        task.pop("exception_last_error", None)

    def run_once(self):
        if not self._enabled:
            return
        notify_results: List[Dict[str, Any]] = []
        with self._lock:
            self._active_notify_results = notify_results
            try:
                state = self._load_state()
                queue = state.setdefault("queue", {})
                now_ts = time.time()
                if getattr(self, "_next_timer_due_ts", 0) and self._to_float(self._next_timer_due_ts, 0) <= now_ts:
                    self._next_timer_due_ts = 0
                done = 0
                changed = bool(self._prune_stale_markers(state, now_ts=now_ts))
                remove_keys: List[str] = []
                for key, task in list(queue.items()):
                    # 队列处理过程中，前面的任务可能已经同步删除了后面的旧复查任务；
                    # 快照里的陈旧对象必须跳过，避免已经清理的 10 天复查又被执行。
                    if queue.get(key) is not task:
                        continue
                    if not isinstance(task, dict):
                        remove_keys.append(key)
                        changed = True
                        continue
                    due_ts = self._to_float(task.get("due_ts"), 0)
                    if due_ts and now_ts < due_ts:
                        continue

                    try:
                        self._restore_task_status_before_exception_retry(task)
                        result = self._process_queue_task(key, task, state, manual=False)
                        self._clear_task_exception_retry(task)
                    except Exception as err:
                        logger.error(f"监控strm刮削网盘：自动队列任务异常 {key}：{err}\n{traceback.format_exc()}")
                        result = self._queue_task_exception_result(key, task, state, err)
                    task["last_result"] = result
                    changed = True
                    if result.get("remove", True):
                        remove_keys.append(key)
                        done += 1
                for key in remove_keys:
                    queue.pop(key, None)
                if changed:
                    self.save_data("state", state)
                if done > 0:
                    logger.info(f"监控strm刮削网盘：队列检查完成，本次完成 {done} 个任务，剩余 {len(queue)} 个")
                else:
                    logger.debug(f"监控strm刮削网盘：队列检查完成，本次完成 0 个任务，剩余 {len(queue)} 个")
                # 本轮处理结束后，重新安排最近一个短期到期任务，避免只依赖兜底检查周期。
                self._restore_queue_timers(state)
            except Exception as err:
                logger.error(f"监控strm刮削网盘：队列检查失败：{err}\n{traceback.format_exc()}")
            finally:
                self._active_notify_results = None
        if self._notify and notify_results:
            self._send_notify_results(notify_results)

    def _process_initial_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        raw_path = str(task.get("raw_path") or "")
        media_type = str(task.get("media_type") or "")
        if media_type == "movie":
            movie_dir = Path(str(task.get("movie_dir") or task.get("check_dir") or ""))
            movie_strm = Path(str(task.get("strm_path") or raw_path or ""))
            scrape_dir = Path(str(task.get("scrape_dir") or self._map_strm_path_to_scrape_path(str(movie_dir))))
            self._drop_competing_movie_tasks(state, movie_strm, keep_task=task)
            status = self._movie_metadata_status(movie_dir)
            if status.get("complete"):
                result = {
                    "time": self._now_iso(), "action": "movie_metadata_complete_skip", "scope": str(movie_dir), "folder": str(scrape_dir),
                    "scrape": None, "scrape_msg": "电影刮削信息完整，跳过刮削", "metadata": status
                }
                self._append_history(state, result)
                return {"remove": True, **result}

            movie_source = self._existing_movie_strm_source(movie_strm, movie_dir)
            if not movie_source:
                result = {
                    "time": self._now_iso(), "action": "movie_strm_missing_skip", "scope": str(movie_dir),
                    "folder": str(scrape_dir), "scrape": None,
                    "scrape_msg": "电影 STRM 已不存在、已改名或不在配置根路径内；任务已移除，未触发 CD2 刮削。",
                    "media_type": "movie",
                }
                self._append_history(state, result)
                return {"remove": True, **result}
            movie_strm = movie_source

            # 电影补刮削只使用 CD2 同名真实视频文件。
            # STRM 只用于提供文件名 stem；候选路径逐个交给 MP StorageChain 获取 fileitem，
            # 不再用 Path.exists() 作为前置判断，也不扫描/刮削 STRM 目录。
            ok, msg, scrape_target, candidates = self._trigger_movie_same_name_scrape(movie_strm, movie_dir)
            if ok:
                result = {
                    "time": self._now_iso(), "action": "movie_metadata_incomplete_scrape", "scope": str(movie_dir), "folder": str(scrape_target),
                    "scrape": True, "scrape_msg": msg, "metadata": status,
                    "note": "电影缺少完整刮削信息，已使用 CD2 同名真实视频文件触发一次刮削；电影不进入10天复查。"
                }
                self._append_history(state, result)
                return {"remove": True, **result}

            result = self._schedule_movie_retry_task(state, task, movie_dir, movie_strm, scrape_dir, status, candidates, reason=msg)
            self._append_history(state, result)
            return {"remove": True, **result}

        if media_type == "tv":
            return self._process_initial_tv_task(task, state, raw_path)

        result = {"time": self._now_iso(), "action": "skip_unknown_media_type", "scope": raw_path, "folder": raw_path, "scrape": None, "scrape_msg": "无法判断电影/电视剧类型，已跳过"}
        self._append_history(state, result)
        return {"remove": True, **result}


    def _schedule_movie_retry_task(
        self,
        state: Dict[str, Any],
        source_task: Dict[str, Any],
        movie_dir: Path,
        movie_strm: Path,
        scrape_dir: Path,
        metadata_status: Dict[str, Any],
        candidates: List[str],
        reason: str = "",
    ) -> Dict[str, Any]:
        """电影真实文件暂不可见时，创建短期重试任务。

        只重试 CD2 同名真实视频文件；不会退回刮削 /media STRM 文件或 STRM 目录。
        """
        queue = state.setdefault("queue", {})
        now_ts = time.time()
        delays = self._movie_retry_delays()
        first_delay = delays[0] if delays else 30
        key_base = str(movie_strm or movie_dir or "").strip() or str(movie_dir)
        task_key = f"movie_retry::{key_base}"
        due_ts = now_ts + first_delay
        candidate_preview = candidates[:8] if candidates else []
        existing = queue.get(task_key)
        if isinstance(existing, dict):
            old_due = self._to_float(existing.get("due_ts"), 0)
            if old_due and old_due > now_ts:
                due_ts = old_due
            existing.update({
                "movie_dir": str(movie_dir),
                "movie_strm": str(movie_strm),
                "strm_path": str(movie_strm),
                "scrape_dir": str(scrape_dir),
                "candidates": candidate_preview,
                "last_reason": reason or existing.get("last_reason") or "CD2 同名真实视频文件暂不可见",
                "last_msg": f"电影真实文件暂未就绪，等待重试；下次检查：{self._ts_to_str(due_ts)}",
            })
            existing["due_ts"] = due_ts
            existing["due_at"] = self._ts_to_str(due_ts)
            existing["status"] = "waiting_movie_retry"
            self._schedule_delayed_check_until(due_ts)
        else:
            queue[task_key] = {
                "task_type": "movie_retry",
                "key": task_key,
                "raw_path": str(source_task.get("raw_path") or movie_strm or movie_dir),
                "media_type": "movie",
                "movie_dir": str(movie_dir),
                "movie_strm": str(movie_strm),
                "strm_path": str(movie_strm),
                "scrape_dir": str(scrape_dir),
                "candidates": candidate_preview,
                "retry_index": 0,
                "retry_count": 0,
                "first_seen_ts": self._to_float(source_task.get("first_seen_ts"), now_ts),
                "first_seen": str(source_task.get("first_seen") or self._now_iso()),
                "due_ts": due_ts,
                "due_at": self._ts_to_str(due_ts),
                "status": "waiting_movie_retry",
                "source": "movie_file_wait_retry",
                "last_reason": reason or "CD2 同名真实视频文件暂不可见",
                "last_msg": f"电影真实文件暂未就绪，等待 {first_delay:g} 秒后重试",
            }
            self._schedule_delayed_check(first_delay)

        result = {
            "time": self._now_iso(),
            "action": "movie_scrape_retry_scheduled",
            "scope": str(movie_dir),
            "folder": str(scrape_dir),
            "scrape": None,
            "scrape_msg": "电影 CD2 同名真实视频文件暂未就绪，已加入短期重试队列。",
            "metadata": metadata_status,
            "candidates": candidate_preview,
            "note": (
                f"原因：{reason or 'MP 暂时无法获取 fileitem'}；"
                f"下次重试：{self._ts_to_str(due_ts)}。"
                "只会尝试 /CD2 下同名真实视频文件，不会使用 STRM 文件或 STRM 目录作为刮削目标。"
            ),
        }
        return result

    def _process_movie_retry_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        movie_dir = Path(str(task.get("movie_dir") or task.get("check_dir") or ""))
        movie_strm = Path(str(task.get("movie_strm") or task.get("strm_path") or task.get("raw_path") or ""))
        scrape_dir = Path(str(task.get("scrape_dir") or self._map_strm_path_to_scrape_path(str(movie_dir))))
        self._drop_competing_movie_tasks(state, movie_strm, keep_task=task)
        status = self._movie_metadata_status(movie_dir)
        if status.get("complete"):
            result = {
                "time": self._now_iso(),
                "action": "movie_metadata_complete_skip",
                "scope": str(movie_dir),
                "folder": str(scrape_dir),
                "scrape": None,
                "scrape_msg": "电影刮削信息已完整，电影重试任务完成并移除。",
                "metadata": status,
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        movie_source = self._existing_movie_strm_source(movie_strm, movie_dir)
        if not movie_source:
            result = {
                "time": self._now_iso(), "action": "movie_strm_missing_skip", "scope": str(movie_dir),
                "folder": str(scrape_dir), "scrape": None,
                "scrape_msg": "电影 STRM 已不存在、已改名或不在配置根路径内；重试任务已移除，未触发 CD2 刮削。",
                "media_type": "movie",
            }
            self._append_history(state, result)
            return {"remove": True, **result}
        movie_strm = movie_source

        ok, msg, scrape_target, candidates = self._trigger_movie_same_name_scrape(movie_strm, movie_dir)
        if ok:
            result = {
                "time": self._now_iso(),
                "action": "movie_retry_scrape_success",
                "scope": str(movie_dir),
                "folder": str(scrape_target),
                "scrape": True,
                "scrape_msg": msg,
                "metadata": status,
                "candidates": candidates[:8],
                "note": "电影短期重试已获取到 CD2 同名真实视频文件，并已触发 MP 刮削。",
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        delays = self._movie_retry_delays()
        retry_index = int(task.get("retry_index") or 0)
        retry_count = int(task.get("retry_count") or 0) + 1
        if retry_index + 1 < len(delays):
            next_index = retry_index + 1
            next_delay = delays[next_index]
            due_ts = time.time() + next_delay
            task["retry_index"] = next_index
            task["retry_count"] = retry_count
            task["due_ts"] = due_ts
            task["due_at"] = self._ts_to_str(due_ts)
            task["status"] = "waiting_movie_retry"
            task["candidates"] = candidates[:8]
            task["last_reason"] = msg
            task["last_msg"] = f"电影真实文件仍未就绪，等待 {next_delay:g} 秒后第 {retry_count + 1} 次重试"
            self._schedule_delayed_check_until(due_ts)
            result = {
                "time": self._now_iso(),
                "action": "movie_retry_scrape_wait",
                "scope": str(movie_dir),
                "folder": str(scrape_dir),
                "scrape": None,
                "scrape_msg": f"电影 CD2 同名真实视频文件仍未就绪，已安排下次重试：{self._ts_to_str(due_ts)}。",
                "metadata": status,
                "candidates": candidates[:8],
                "note": "只会尝试 /CD2 下同名真实视频文件；不会使用 STRM 文件或 STRM 目录作为刮削目标。",
            }
            self._append_history(state, result)
            return {"remove": False, **result}

        result = {
            "time": self._now_iso(),
            "action": "movie_retry_scrape_failed",
            "scope": str(movie_dir),
            "folder": str(scrape_dir),
            "scrape": False,
            "scrape_msg": "电影刮削失败：多次等待后仍无法获取 CD2 同名真实视频文件 fileitem。",
            "metadata": status,
            "candidates": candidates[:12],
            "note": (
                f"最后原因：{msg}；已重试 {retry_count} 次。"
                "不会使用 STRM 文件、STRM 目录或 /media 路径作为刮削目标。"
            ),
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    def _movie_retry_delays(self) -> List[float]:
        result: List[float] = []
        for value in self.DEFAULT_MOVIE_RETRY_DELAYS_SECONDS:
            try:
                number = float(value)
                if number >= 0:
                    result.append(number)
            except Exception:
                continue
        return result or [30, 120, 300, 900, 3600]



    def _tv_cd2_retry_delays(self) -> List[float]:
        """电视剧 CD2 就绪重试沿用电影短期重试节奏。"""
        return self._movie_retry_delays()

    def _advance_retry_task(self, task: Dict[str, Any], delays: List[float], reason: str = "") -> Tuple[bool, float, int]:
        """推进短期重试任务。返回：(是否还有下次, due_ts, retry_count)。"""
        raw_retry_index = task.get("retry_index")
        if raw_retry_index is None or raw_retry_index == "":
            # movie_retry / tv_metadata_retry 创建时已经用过第 0 档延迟；
            # episode_scrape 的初始等待只是刮削前等待，不算 CD2 重试，
            # 所以第一次失败后仍从 30 秒这一档开始。
            retry_index = -1 if str(task.get("task_type") or "") == "episode_scrape" else 0
        else:
            retry_index = int(raw_retry_index or 0)
        retry_count = int(task.get("retry_count") or 0) + 1
        if retry_index + 1 < len(delays):
            next_index = retry_index + 1
            next_delay = float(delays[next_index])
            due_ts = time.time() + next_delay
            task["retry_index"] = next_index
            task["retry_count"] = retry_count
            task["due_ts"] = due_ts
            task["due_at"] = self._ts_to_str(due_ts)
            if reason:
                task["last_reason"] = reason
            self._schedule_delayed_check_until(due_ts)
            return True, due_ts, retry_count
        task["retry_count"] = retry_count
        if reason:
            task["last_reason"] = reason
        return False, 0, retry_count

    def _ensure_tv_metadata_retry_task(
        self,
        state: Dict[str, Any],
        show_root: Path,
        scope_dir: Path,
        purpose: str = "剧/季信息",
        mode: str = "root",
        season_dirs: List[Path] = None,
        episodes: List[Path] = None,
        reason: str = "",
        batch_id: str = "",
        postcheck_on_missing: str = "",
        season_rescrape_count: int = 0,
    ):
        """剧名目录/Season 目录在 CD2 暂不可访问时，创建短期就绪重试任务。"""
        queue = state.setdefault("queue", {})
        mode = str(mode or "root")
        if not self._is_safe_tv_source_scope(show_root, scope_dir, mode=mode, require_exists=True):
            logger.warning(f"监控strm刮削网盘：拒绝创建越界的剧/季重试任务：show={show_root}，scope={scope_dir}，mode={mode}")
            return False
        key = f"tv_metadata_retry::{mode}::{scope_dir}"
        delays = self._tv_cd2_retry_delays()
        first_delay = delays[0] if delays else 30
        due_ts = time.time() + first_delay
        existing = queue.get(key)
        season_strings = [str(x) for x in (season_dirs or []) if str(x or "").strip()]
        episode_strings = [str(x) for x in self._unique_episode_paths(episodes or [])]
        season_rescrape_count = max(int(self._to_float(season_rescrape_count, 0)), 0)
        if isinstance(existing, dict):
            old_due = self._to_float(existing.get("due_ts"), 0)
            if old_due and old_due > time.time():
                due_ts = old_due
            old_eps = self._to_list(existing.get("episodes") or [])
            for ep in episode_strings:
                if ep not in old_eps:
                    old_eps.append(ep)
            old_seasons = self._to_list(existing.get("season_dirs") or [])
            for sd in season_strings:
                if sd not in old_seasons:
                    old_seasons.append(sd)
            existing.update({
                "show_root": str(show_root),
                "scope_dir": str(scope_dir),
                "purpose": str(purpose or existing.get("purpose") or "剧/季信息"),
                "mode": mode,
                "season_dirs": old_seasons,
                "episodes": old_eps,
                "batch_id": str(batch_id or existing.get("batch_id") or ""),
                "postcheck_on_missing": str(postcheck_on_missing or existing.get("postcheck_on_missing") or ""),
                "season_rescrape_count": max(
                    int(self._to_float(existing.get("season_rescrape_count"), 0)),
                    season_rescrape_count,
                ),
                "due_ts": due_ts,
                "due_at": self._ts_to_str(due_ts),
                "status": "waiting_tv_metadata_retry",
                "last_reason": reason or existing.get("last_reason") or "CD2 目录暂未就绪",
                "last_msg": f"CD2 剧/季目录暂未就绪，等待重试；下次检查：{self._ts_to_str(due_ts)}",
            })
        else:
            queue[key] = {
                "task_type": "tv_metadata_retry",
                "key": key,
                "show_root": str(show_root),
                "scope_dir": str(scope_dir),
                "purpose": str(purpose or "剧/季信息"),
                "mode": mode,
                "season_dirs": season_strings,
                "episodes": episode_strings,
                "batch_id": str(batch_id or self._make_task_batch_id("tvmeta")),
                "postcheck_on_missing": str(postcheck_on_missing or ""),
                "season_rescrape_count": season_rescrape_count,
                "retry_index": 0,
                "retry_count": 0,
                "first_seen_ts": time.time(),
                "first_seen": self._now_iso(),
                "due_ts": due_ts,
                "due_at": self._ts_to_str(due_ts),
                "status": "waiting_tv_metadata_retry",
                "last_reason": reason or "CD2 目录暂未就绪",
                "last_msg": f"CD2 剧/季目录暂未就绪，等待 {first_delay:g} 秒后重试",
            }
        self._schedule_delayed_check_until(due_ts)
        return True

    def _process_tv_metadata_retry_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        show_root = Path(str(task.get("show_root") or ""))
        scope_dir = Path(str(task.get("scope_dir") or ""))
        purpose = str(task.get("purpose") or "剧/季信息")
        mode = str(task.get("mode") or "root")
        refresh_batch = set()
        if not self._is_safe_tv_source_scope(show_root, scope_dir, mode=mode, require_exists=True):
            result = {
                "time": self._now_iso(), "action": "tv_metadata_retry_scope_rejected", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(scope_dir)), "scrape": False,
                "scrape_msg": "剧/季信息重试范围不在原 STRM 剧名目录内或目录已不存在；异常任务已移除，未触发 CD2 刮削。",
                "media_type": "tv",
            }
            self._append_history(state, result)
            return {"remove": True, **result}
        source_scope = scope_dir if mode == "season" else show_root
        episode_paths = self._unique_episode_paths([Path(x) for x in self._to_list(task.get("episodes") or [])])
        episode_paths = [
            ep for ep in episode_paths
            if self._is_safe_strm_episode_file(ep, show_root=source_scope, require_exists=True)
        ]
        if not episode_paths:
            episode_paths = [
                ep for ep in self._list_strm_files(source_scope)
                if self._is_safe_strm_episode_file(ep, show_root=source_scope, require_exists=True)
            ]
        if not episode_paths:
            result = {
                "time": self._now_iso(), "action": "tv_metadata_retry_strm_missing_skip", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(scope_dir)), "scrape": None,
                "scrape_msg": "剧/季信息重试时已没有有效 STRM 单集；任务已移除，未触发 CD2 目录刮削。",
                "media_type": "tv",
            }
            self._append_history(state, result)
            return {"remove": True, **result}
        ok, msg, scrape_target = self._trigger_tv_metadata_scrape(
            scope_dir,
            purpose=purpose,
            refresh_batch=refresh_batch,
        )
        if ok:
            due_ts = time.time() + max(self._post_scrape_check_delay_minutes, 0) * 60
            # 目录刮削成功后，这些集先等目录刮削后的 10 分钟检查；
            # 移除仍在等待的单集刮削任务，避免目录刮削和单集刮削重复触发。
            queue = state.setdefault("queue", {})
            for ep in episode_paths:
                queue.pop(f"episode::{ep}", None)
            if episode_paths:
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="episodes",
                    episodes=episode_paths,
                    reason="tv_metadata_retry_success_wait_episode_check",
                    batch_id=str(task.get("batch_id") or self._make_task_batch_id("metaeps")),
                    due_ts=due_ts,
                    on_missing="episode_scrape",
                )
            removed_season_retry = 0
            if mode == "season":
                season_dirs = [Path(x) for x in self._to_list(task.get("season_dirs") or [])] or [scope_dir]
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="season",
                    season_dirs=season_dirs,
                    reason="after_tv_metadata_retry_success",
                    batch_id=str(task.get("batch_id") or self._make_task_batch_id("seasonretry")),
                    due_ts=due_ts,
                    season_rescrape_count=max(int(self._to_float(task.get("season_rescrape_count"), 0)), 0),
                )
            else:
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="root",
                    reason="after_tv_metadata_retry_success",
                    batch_id="root",
                    due_ts=due_ts,
                )
                # root 目录重试成功后，整剧目录刮削会覆盖本次剧下 Season；
                # 删除同剧仍在等待的 Season 重试，避免后续重复刮削。
                season_dirs = self._unique_season_dirs(episode_paths, show_root)
                if season_dirs:
                    self._ensure_tv_postcheck_task(
                        state,
                        show_root,
                        mode="season",
                        season_dirs=season_dirs,
                        reason="after_root_tv_metadata_retry_success",
                        batch_id=str(task.get("batch_id") or self._make_task_batch_id("rootseason")),
                        due_ts=due_ts,
                    )
                current_key = str(task.get("key") or "")
                for qkey, qtask in list(queue.items()):
                    if qkey == current_key or not isinstance(qtask, dict):
                        continue
                    if str(qtask.get("task_type") or "") != "tv_metadata_retry":
                        continue
                    if str(qtask.get("mode") or "") != "season":
                        continue
                    q_show = str(qtask.get("show_root") or "")
                    q_scope = str(qtask.get("scope_dir") or "")
                    if q_show == str(show_root) or self._path_same_or_under(q_scope, str(show_root)):
                        queue.pop(qkey, None)
                        removed_season_retry += 1
            result = {
                "time": self._now_iso(),
                "action": "tv_metadata_retry_success",
                "scope": str(scope_dir),
                "folder": str(scrape_target),
                "scrape": True,
                "scrape_msg": f"{purpose}短期重试成功，已触发 MP 刮削事件。{msg}",
                "note": ("已创建 10 分钟后检查任务。" + (f" 已清理同剧 Season 重试任务 {removed_season_retry} 个。" if removed_season_retry else "")),
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        delays = self._tv_cd2_retry_delays()
        has_next, due_ts, retry_count = self._advance_retry_task(task, delays, reason=msg)
        if has_next:
            task["status"] = "waiting_tv_metadata_retry"
            task["last_msg"] = f"CD2 剧/季目录仍未就绪，等待下次重试：{self._ts_to_str(due_ts)}"
            result = {
                "time": self._now_iso(),
                "action": "tv_metadata_retry_wait",
                "scope": str(scope_dir),
                "folder": self._map_strm_path_to_scrape_path(str(scope_dir)),
                "scrape": None,
                "scrape_msg": f"{purpose}目录仍未就绪，已安排下次重试：{self._ts_to_str(due_ts)}。",
                "note": msg,
            }
            self._append_history(state, result)
            return {"remove": False, **result}

        result = {
            "time": self._now_iso(),
            "action": "tv_metadata_retry_failed",
            "scope": str(scope_dir),
            "folder": self._map_strm_path_to_scrape_path(str(scope_dir)),
            "scrape": False,
            "scrape_msg": f"{purpose}刮削失败：多次等待后 CD2 目录仍不可用或目录内真实媒体未就绪。",
            "note": f"最后原因：{msg}；已重试 {retry_count} 次。",
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    def _process_initial_tv_task(self, task: Dict[str, Any], state: Dict[str, Any], raw_path: str = "") -> Dict[str, Any]:
        """处理电视剧入库任务。

        电视剧按“剧信息 → 季信息 → 单集图片”三层处理：
        - 入库时先判断范围：整剧目录扫描整剧、整季/批量多集扫描当前季、追更单集只检查单集；
        - 剧信息/季信息缺失时，按 MP 原生模式分别触发剧名目录/Season 目录刮削；
        - 单集缺图时，使用真实单集媒体文件触发 MP 刮削；
        - 所有缺图集都进入 10 分钟检查，10 分钟后仍缺图才进入 10 天复查。
        """
        show_root = Path(str(task.get("show_root") or task.get("check_dir") or ""))
        episode_values = self._to_list(task.get("episodes") or [])
        if not episode_values:
            episode_values = [str(task.get("episode_strm") or task.get("strm_path") or raw_path)]
        episode_paths = self._unique_episode_paths(episode_values)
        episode_paths, scan_scope_type, scan_scope_path = self._expand_tv_initial_episode_scope(task, show_root, episode_paths, raw_path)
        episode_paths = [
            ep for ep in episode_paths
            if self._is_safe_strm_episode_file(ep, show_root=show_root, require_exists=True)
        ]
        task["resolved_scan_scope"] = scan_scope_type
        task["resolved_scan_path"] = str(scan_scope_path or "")

        if not episode_paths:
            result = {
                "time": self._now_iso(), "action": "tv_initial_no_existing_episode_skip", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                "scrape_msg": "本次入库范围内已没有有效 STRM 单集；任务已移除，未触发 CD2 刮削。",
                "media_type": "tv",
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        now_ts = time.time()
        postcheck_batch_id = self._make_task_batch_id("postcheck")
        delay_seconds = max(self._post_scrape_check_delay_minutes, 0) * 60
        postcheck_due_ts = now_ts + delay_seconds
        sent_targets = set()
        refresh_batch = set()
        episode_target_map: Dict[str, str] = {}
        # 剧/季目录刮削在 MP 原生流程里可能会同时处理目录下文件。
        # 为避免同一集在“目录刮削”和“单集刮削任务”里重复触发，
        # 被成功目录刮削覆盖的缺图集先等待 10 分钟检查；仍缺图时再创建单集刮削任务。
        metadata_scrape_episodes: List[Path] = []
        root_directory_scrape_active = False

        def target_key(value: str) -> str:
            return str(Path(str(value or ""))) if value else ""

        # 先检查本次快照内的每一集是否已有图片。
        # 注意：这里不立即删除 CD2 单集 nfo。若剧/季目录刮削会覆盖该集，先等待目录刮削后的10分钟检查；
        # 只有真正进入单集补刮削时，才删除该集 CD2 对应 nfo，避免目录刮削和单集刮削造成重复删除/重复刮削。
        missing_eps: List[Path] = []
        existing_count = 0
        for episode_strm in episode_paths:
            episode_status = self._episode_image_status(episode_strm)
            if episode_status.get("has_image"):
                existing_count += 1
                continue
            missing_eps.append(episode_strm)

        # 1. 检查剧名根目录基础信息。缺失时，按 MP 原生模式触发剧名目录刮削。
        root_status = self._tv_root_metadata_status(show_root)
        if not root_status.get("has_any_metadata"):
            scrape_dir = Path(self._map_strm_path_to_scrape_path(str(show_root)))
            markers = state.setdefault("markers", {})
            marker_key = f"tv_root_whole_scrape::{show_root}"
            marker = markers.get(marker_key) if isinstance(markers, dict) else None
            marker_age = now_ts - self._to_float(marker.get("ts"), 0) if isinstance(marker, dict) else -1
            recent_marker = isinstance(marker, dict) and 0 <= marker_age < 600
            ok = False
            msg = ""
            scrape_target = ""
            if recent_marker:
                scrape_target = str(marker.get("target") or "")
                if scrape_target:
                    sent_targets.add(target_key(scrape_target))
                marker_ts = self._to_float(marker.get("ts"), now_ts)
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="root",
                    reason="root_metadata_merged",
                    batch_id="root",
                    due_ts=marker_ts + delay_seconds,
                )
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="episodes",
                    episodes=episode_paths,
                    reason="root_no_metadata_merged",
                    batch_id=str(task.get("key") or postcheck_batch_id),
                    due_ts=marker_ts + delay_seconds,
                    on_missing="episode_scrape",
                )
                metadata_scrape_episodes.extend(episode_paths)
                root_directory_scrape_active = True
                result = {
                    "time": self._now_iso(),
                    "action": "tv_root_no_metadata_merge_existing_whole_show_scrape",
                    "scope": str(show_root),
                    "folder": str(scrape_target or scrape_dir),
                    "scrape": None,
                    "scrape_msg": "同一剧名根目录 10 分钟内已触发过剧信息刮削，本次不重复发送；继续检查季信息和本批次单集。",
                    "metadata": root_status,
                    **self._episode_history_payload(show_root, episode_paths, prefix="checked"),
                }
                self._append_history(state, result)
            else:
                ok, msg, scrape_target = self._trigger_tv_metadata_scrape(
                    show_root,
                    purpose="剧信息",
                    refresh_batch=refresh_batch,
                )
                if ok and isinstance(markers, dict):
                    markers[marker_key] = {"ts": now_ts, "time": self._now_iso(), "show_root": str(show_root), "target": str(scrape_target)}
                    self._ensure_tv_postcheck_task(
                        state,
                        show_root,
                        mode="root",
                        reason="after_root_scrape",
                        batch_id="root",
                        due_ts=postcheck_due_ts,
                    )
                    # 剧名目录刮削会覆盖本次快照内所有单集；这些单集先等待检查，不立即重复单集刮削。
                    metadata_scrape_episodes.extend(episode_paths)
                    root_directory_scrape_active = True
                result = {
                    "time": self._now_iso(), "action": "tv_root_no_metadata_scrape_whole_show", "scope": str(show_root), "folder": str(scrape_target or scrape_dir),
                    "scrape": ok, "scrape_msg": msg, "metadata": root_status,
                    "note": (
                        "剧名根目录没有图片/nfo，已按 MP 原生模式触发 CD2 剧名目录刮削；不会中断季信息和单集检查。"
                        if ok else
                        "剧名根目录没有图片/nfo，剧名目录刮削失败；仍继续检查季信息和单集。"
                    ),
                    **self._episode_history_payload(show_root, episode_paths, prefix="checked"),
                }
                if not ok:
                    self._ensure_tv_metadata_retry_task(
                        state,
                        show_root,
                        show_root,
                        purpose="剧信息",
                        mode="root",
                        episodes=episode_paths,
                        reason=msg,
                        batch_id="root",
                        postcheck_on_missing="episode_scrape",
                    )
                    result["action"] = "tv_metadata_retry_scheduled"
                    result["scrape"] = None
                    result["scrape_msg"] = "剧名目录暂未就绪或目录内真实媒体未刷新，已加入 CD2 短期重试队列。"
                    result["note"] = f"原始原因：{msg}；仍继续检查季信息和单集，插件不会立即判最终失败。"
                self._append_history(state, result)

        # 2. 检查本次入库涉及的季信息。缺失时，按 MP 原生模式触发 Season 目录刮削。
        season_dirs = self._unique_season_dirs(episode_paths, show_root)
        for season_dir in season_dirs:
            try:
                season_status = self._tv_season_metadata_status(show_root, season_dir)
            except Exception as err:
                logger.debug(f"监控strm刮削网盘：检查季信息状态失败：{season_dir} - {err}")
                continue
            if season_status.get("complete"):
                continue

            scrape_dir = Path(self._map_strm_path_to_scrape_path(str(season_dir)))
            markers = state.setdefault("markers", {})
            marker_key = f"tv_season_scrape::{season_dir}"
            marker = markers.get(marker_key) if isinstance(markers, dict) else None
            marker_age = now_ts - self._to_float(marker.get("ts"), 0) if isinstance(marker, dict) else -1
            recent_marker = isinstance(marker, dict) and 0 <= marker_age < 600
            season_episodes = [ep for ep in episode_paths if self._path_same_or_under(str(ep), str(season_dir))]
            if root_directory_scrape_active:
                # 剧名目录刮削已覆盖本次快照内的 Season 目录和单集文件；
                # 不再额外触发 Season 目录刮削，避免 MP 重复重写同一季的单集图片/nfo。
                metadata_scrape_episodes.extend(season_episodes)
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="season",
                    season_dirs=[season_dir],
                    reason="season_metadata_wait_root_scrape",
                    batch_id=f"season_{season_dir.name}",
                    due_ts=postcheck_due_ts,
                )
                result = {
                    "time": self._now_iso(),
                    "action": "tv_season_metadata_wait_root_scrape",
                    "scope": str(season_dir),
                    "folder": str(scrape_dir),
                    "scrape": None,
                    "scrape_msg": "剧名目录刮削已覆盖当前季，本次不重复触发 Season 目录刮削；10分钟后检查季信息和单集图片。",
                    "metadata": season_status,
                }
                self._append_history(state, result)
                continue
            scrape_target = ""
            ok = False
            msg = ""
            if recent_marker:
                scrape_target = str(marker.get("target") or "")
                if scrape_target:
                    sent_targets.add(target_key(scrape_target))
                marker_ts = self._to_float(marker.get("ts"), now_ts)
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="season",
                    season_dirs=[season_dir],
                    reason="season_metadata_merged",
                    batch_id=f"season_{season_dir.name}",
                    due_ts=marker_ts + delay_seconds,
                )
                metadata_scrape_episodes.extend(season_episodes)
                result = {
                    "time": self._now_iso(),
                    "action": "tv_season_metadata_merge_existing_scrape",
                    "scope": str(season_dir),
                    "folder": str(scrape_target or scrape_dir),
                    "scrape": None,
                    "scrape_msg": "同一季 10 分钟内已触发过季信息刮削，本次不重复发送；继续检查本批次单集。",
                    "metadata": season_status,
                }
                self._append_history(state, result)
                continue

            ok, msg, scrape_target = self._trigger_tv_metadata_scrape(
                season_dir,
                purpose=f"季信息 {season_dir.name}",
                refresh_batch=refresh_batch,
            )

            if ok and isinstance(markers, dict):
                markers[marker_key] = {"ts": now_ts, "time": self._now_iso(), "season_dir": str(season_dir), "target": str(scrape_target)}
            season_result = {
                "time": self._now_iso(),
                "action": "tv_season_metadata_incomplete_scrape",
                "scope": str(season_dir),
                "folder": str(scrape_target or scrape_dir),
                "scrape": ok,
                "scrape_msg": msg,
                "metadata": season_status,
                "note": "当前季缺少季信息，已按 MP 原生模式触发 CD2 Season 目录刮削；随后继续检查本次入库单集图片。" if ok else "当前季缺少季信息，Season 目录刮削失败。",
            }
            if not ok:
                self._ensure_tv_metadata_retry_task(
                    state,
                    show_root,
                    season_dir,
                    purpose=f"季信息 {season_dir.name}",
                    mode="season",
                    season_dirs=[season_dir],
                    episodes=season_episodes,
                    reason=msg,
                    batch_id=f"season_{season_dir.name}",
                )
                season_result["action"] = "tv_metadata_retry_scheduled"
                season_result["scrape"] = None
                season_result["scrape_msg"] = f"{season_dir.name} 目录暂未就绪或目录内真实媒体未刷新，已加入 CD2 短期重试队列。"
                season_result["note"] = f"原始原因：{msg}；不会立即判最终失败。"
            self._append_history(state, season_result)
            if ok:
                self._ensure_tv_postcheck_task(
                    state,
                    show_root,
                    mode="season",
                    season_dirs=[season_dir],
                    reason="after_season_scrape",
                    batch_id=f"season_{season_dir.name}",
                    due_ts=postcheck_due_ts,
                )
                # Season 目录刮削可能会同时处理本季文件；本季缺图集先等待检查，避免马上重复单集刮削。
                metadata_scrape_episodes.extend(season_episodes)

        # 3. 对所有缺图集触发单集补刮削。剧/季目录刮削不影响单集刮削和 10 分钟检查。
        scheduled_eps: List[Path] = []
        shared_trigger_eps: List[Path] = []
        metadata_wait_eps: List[Path] = []
        target_missing_eps: List[Path] = []
        metadata_wait_set = {str(x) for x in self._unique_episode_paths(metadata_scrape_episodes)}
        for episode_strm in missing_eps:
            if str(episode_strm) in metadata_wait_set:
                metadata_wait_eps.append(episode_strm)
                continue
            target = episode_target_map.get(str(episode_strm))
            if target is None:
                try:
                    target = self._map_episode_strm_to_scrape_target(
                        episode_strm,
                        refresh_batch=refresh_batch,
                    ) or ""
                except Exception as err:
                    logger.debug(f"监控strm刮削网盘：查找单集真实媒体失败：{episode_strm} - {err}")
                    target = ""
                episode_target_map[str(episode_strm)] = target
            key = target_key(target)
            if key and key in sent_targets:
                shared_trigger_eps.append(episode_strm)
                continue
            if not target:
                target_missing_eps.append(episode_strm)
            self._ensure_episode_scrape_task(
                state,
                episode_strm,
                show_root,
                reason="initial_missing_image",
                deleted_nfo=0,
                postcheck_batch_id=postcheck_batch_id,
                scrape_target=target,
                refresh_batch=refresh_batch,
            )
            scheduled_eps.append(episode_strm)

        wait_check_eps = self._unique_episode_paths(metadata_wait_eps + shared_trigger_eps)
        if wait_check_eps:
            self._ensure_tv_postcheck_task(
                state,
                show_root,
                mode="episodes",
                episodes=wait_check_eps,
                reason="metadata_directory_scrape_wait_check",
                batch_id=postcheck_batch_id,
                due_ts=postcheck_due_ts,
                on_missing="episode_scrape",
            )

        if missing_eps:
            names = "、".join([self._episode_label(ep, show_root) for ep in missing_eps[:8]])
            extra_parts = []
            if scheduled_eps:
                extra_parts.append(f"已创建 {len(scheduled_eps)} 个单集刮削任务")
            if metadata_wait_eps:
                extra_parts.append(f"其中 {len(metadata_wait_eps)} 集已由剧/季目录刮削覆盖，先等待10分钟检查，仍缺图再触发单集刮削")
            if shared_trigger_eps:
                extra_parts.append(f"其中 {len(shared_trigger_eps)} 集本批次已触发过同一真实文件刮削，不重复发送，只等待10分钟检查")
            if target_missing_eps:
                extra_parts.append(f"{len(target_missing_eps)} 集暂未找到真实媒体文件，后续单集任务会再次尝试并记录失败原因")
            extra = "；" + "；".join(extra_parts) if extra_parts else ""
            result = {
                "time": self._now_iso(), "action": "tv_episode_missing_image_schedule_scrape", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                "scrape_msg": f"本次按{self._tv_scan_scope_label(scan_scope_type)}检查 {len(episode_paths)} 集，其中 {len(missing_eps)} 集缺少对应图片；已为需要立即单集补刮削的集创建任务，实际刮削前会删除 CD2 同名 nfo。目录刮削覆盖的集先等待10分钟检查，仍缺图才触发单集刮削；单集刮削成功后再计时10分钟检查，仍缺图才加入10天复查。{extra}",
                "note": names,
                "missing_count": len(missing_eps),
                "existing_count": existing_count,
                "deleted_nfo": 0,
                **self._episode_history_payload(show_root, episode_paths, prefix="checked"),
                **self._episode_history_payload(show_root, missing_eps, prefix="missing"),
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        result = {
            "time": self._now_iso(), "action": "tv_episode_image_exists_skip_initial", "scope": str(show_root),
            "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
            "scrape_msg": f"本次按{self._tv_scan_scope_label(scan_scope_type)}检查 {len(episode_paths)} 集均已有对应图片，跳过单集刮削；不加入 10 天复查队列。",
            **self._episode_history_payload(show_root, episode_paths, prefix="checked"),
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    def _process_episode_scrape_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        episode_strm = Path(str(task.get("episode_strm") or ""))
        show_root = Path(str(task.get("show_root") or ""))
        refresh_batch = set()

        # 正常初始化会固定开启刮削；这里仍保留防御，确保异常运行态下绝不先删 NFO。
        if not self._scrape:
            result = {
                "time": self._now_iso(), "action": "episode_scrape_failed", "scope": str(episode_strm),
                "folder": str(task.get("scrape_target") or ""), "scrape": False,
                "scrape_msg": "插件刮削功能未启用，已停止该单集任务且未删除 CD2 NFO。",
                "deleted_nfo": int(task.get("deleted_nfo") or 0),
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        # 延迟任务执行时 STRM 可能已经被删除或改名。此时任务已失去来源，
        # 必须在查找 CD2 文件、删除 NFO 之前结束，避免旧任务修改网盘元数据。
        if not self._is_safe_strm_episode_file(episode_strm, show_root=show_root, require_exists=True):
            result = {
                "time": self._now_iso(),
                "action": "episode_strm_missing_skip",
                "scope": str(episode_strm),
                "folder": str(task.get("scrape_target") or ""),
                "scrape": None,
                "scrape_msg": "STRM 单集已不存在、已改名或不在配置根路径内；旧任务已移除，未删除 CD2 NFO，也未触发刮削。",
                "deleted_nfo": int(task.get("deleted_nfo") or 0),
                "media_type": "tv",
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        # 兼容旧队列：历史版本可能把 scrape_target 保存成 .strm 路径。
        # 单集刮削必须使用 CD2 中的真实视频文件，不能刮削 .strm，也不能退回 Season 目录。
        stored_target = str(task.get("scrape_target") or "").strip()
        target = ""
        if stored_target:
            stored_path = Path(stored_target)
            if not self._is_matching_episode_scrape_target(episode_strm, stored_path):
                logger.debug(f"监控strm刮削网盘：忽略旧队列中的非 CD2 单集刮削目标，重新查找同名真实媒体：{stored_target}")
            elif stored_path.exists() and stored_path.is_file():
                target = stored_target
            else:
                stored_ready, _ = self._ensure_scrape_path_refreshed(stored_path, reason="单集刮削文件检查", refresh_batch=refresh_batch)
                if stored_ready or (stored_path.exists() and stored_path.is_file()):
                    target = stored_target

        if not target:
            target = self._map_episode_strm_to_scrape_target(episode_strm, refresh_batch=refresh_batch)

        if not target:
            mapped = Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
            candidates = self._episode_media_candidate_paths(episode_strm)
            delays = self._tv_cd2_retry_delays()
            has_next, due_ts, retry_count = self._advance_retry_task(task, delays, reason="单集真实媒体文件暂未就绪")
            if has_next:
                task["status"] = "waiting_episode_scrape"
                task["scrape_target"] = ""
                task["last_msg"] = f"单集真实媒体文件暂未就绪，等待下次重试：{self._ts_to_str(due_ts)}"
                result = {
                    "time": self._now_iso(), "action": "episode_scrape_retry_wait", "scope": str(episode_strm), "folder": str(mapped.parent if mapped.suffix.lower() == ".strm" else mapped),
                    "scrape": None,
                    "scrape_msg": f"未在 CD2 目标目录找到同名单集真实媒体文件，已安排短期重试：{self._ts_to_str(due_ts)}。",
                    "deleted_nfo": int(task.get("deleted_nfo") or 0),
                    "note": "插件不会刮削 .strm 文件；已按同名视频文件规则查找 mkv/mp4/ts/iso 等格式。" + (f" 已尝试：{', '.join(candidates[:5])}" if candidates else " 未生成候选路径。"),
                }
                self._append_history(state, result)
                return {"remove": False, **result}
            result = {
                "time": self._now_iso(), "action": "episode_scrape_retry_failed", "scope": str(episode_strm), "folder": str(mapped.parent if mapped.suffix.lower() == ".strm" else mapped),
                "scrape": False,
                "scrape_msg": "单集刮削失败：多次等待后仍未在 CD2 目标目录找到同名真实媒体文件；不会退回刮削 Season 目录。",
                "deleted_nfo": int(task.get("deleted_nfo") or 0),
                "note": "插件不会刮削 .strm 文件；已按同名视频文件规则查找 mkv/mp4/ts/iso 等格式。" + (f" 已尝试：{', '.join(candidates[:8])}" if candidates else " 未生成候选路径。") + f" 已重试 {retry_count} 次。",
            }
            self._append_history(state, result)
            return {"remove": True, **result}
        target_path = Path(str(target))
        if not self._is_matching_episode_scrape_target(episode_strm, target_path):
            result = {
                "time": self._now_iso(), "action": "episode_scrape_target_rejected", "scope": str(episode_strm), "folder": str(target_path),
                "scrape": False,
                "scrape_msg": f"单集刮削目标被安全规则拒绝：{target_path}",
                "deleted_nfo": int(task.get("deleted_nfo") or 0),
                "note": "单集刮削目标必须位于 CD2 刮削根路径下，不能位于 STRM 媒体库路径下，且必须是真实视频后缀。已移除该异常任务，避免旧队列误刮非 CD2 路径。"
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        deleted_now, delete_failed, delete_msg = self._delete_episode_nfo(
            episode_strm,
            media_target=target_path,
            refresh_batch=refresh_batch,
        )
        if delete_failed:
            delays = self._tv_cd2_retry_delays()
            has_next, due_ts, retry_count = self._advance_retry_task(task, delays, reason=delete_msg)
            result = {
                "time": self._now_iso(),
                "action": "episode_nfo_delete_retry_wait" if has_next else "episode_nfo_delete_failed",
                "scope": str(episode_strm),
                "folder": str(target_path),
                "scrape": None if has_next else False,
                "scrape_msg": (
                    f"刮削前删除 CD2 同名 nfo 失败，已安排短期重试：{self._ts_to_str(due_ts)}。原因：{delete_msg}"
                    if has_next else
                    f"单集刮削失败：刮削前删除 CD2 同名 nfo 多次失败，未触发刮削。最后原因：{delete_msg}；已重试 {retry_count} 次。"
                ),
                "deleted_nfo": int(task.get("deleted_nfo") or 0),
                "note": str(task.get("reason") or "")
            }
            if has_next:
                task["status"] = "waiting_episode_scrape"
                task["scrape_target"] = str(target_path)
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            self._append_history(state, result)
            return {"remove": True, **result}

        if deleted_now:
            task["deleted_nfo"] = int(task.get("deleted_nfo") or 0) + int(deleted_now or 0)
        total_deleted_nfo = int(task.get("deleted_nfo") or 0)

        ok, raw_msg = self._trigger_scrape(target_path)
        if deleted_now:
            delete_note = f"刮削前已删除 CD2 同名 nfo {deleted_now} 个"
        elif total_deleted_nfo:
            delete_note = f"此前已删除 CD2 同名 nfo {total_deleted_nfo} 个"
        else:
            delete_note = ""
        msg = f"{raw_msg}；{delete_note}" if delete_note else raw_msg
        result = {
            "time": self._now_iso(), "action": "episode_delayed_scrape", "scope": str(episode_strm), "folder": str(target_path),
            "scrape": ok, "scrape_msg": msg, "deleted_nfo": total_deleted_nfo,
            "note": str(task.get("reason") or "")
        }
        if ok and show_root and str(show_root):
            self._ensure_tv_postcheck_task(
                state,
                show_root,
                mode="episodes",
                episodes=[episode_strm],
                reason="after_episode_scrape_success",
                batch_id=str(task.get("postcheck_batch_id") or ""),
                due_ts=time.time() + max(self._post_scrape_check_delay_minutes, 0) * 60,
            )
            result["postcheck_msg"] = "单集刮削事件已发送成功，已从当前时间开始计时 10 分钟后检查图片。"
        if not ok:
            delays = self._tv_cd2_retry_delays()
            has_next, due_ts, retry_count = self._advance_retry_task(task, delays, reason=msg)
            if has_next:
                task["status"] = "waiting_episode_scrape"
                task["scrape_target"] = str(target)
                task["last_msg"] = f"单集真实媒体 fileitem 暂不可用，等待下次重试：{self._ts_to_str(due_ts)}"
                result["action"] = "episode_scrape_retry_wait"
                result["scrape"] = None
                delete_prefix = f"{delete_note}；" if delete_note else ""
                result["scrape_msg"] = f"单集真实媒体 fileitem 暂不可用，已安排短期重试：{self._ts_to_str(due_ts)}。{delete_prefix}原始原因：{raw_msg}"
                self._append_history(state, result)
                return {"remove": False, **result}
            result["action"] = "episode_scrape_retry_failed"
            delete_prefix = f"{delete_note}；" if delete_note else ""
            result["scrape_msg"] = f"单集刮削失败：多次等待后仍无法触发 MP 刮削。{delete_prefix}最后原因：{raw_msg}；已重试 {retry_count} 次。"
        self._append_history(state, result)
        return {"remove": True, **result}

    def _defer_scope_access(self, task: Dict[str, Any], status: str) -> Tuple[bool, float, int]:
        """目录暂不可访问时使用独立的有限重试计数，不干扰刮削重试字段。"""
        delays = self.DEFAULT_SCOPE_ACCESS_RETRY_DELAYS_SECONDS
        retry_count = max(int(self._to_float(task.get("access_retry_count"), 0)), 0)
        if retry_count >= len(delays):
            return False, 0, retry_count
        due_ts = time.time() + delays[retry_count]
        task["access_retry_count"] = retry_count + 1
        task["due_ts"] = due_ts
        task["due_at"] = self._ts_to_str(due_ts)
        task["status"] = status
        return True, due_ts, retry_count + 1

    @staticmethod
    def _clear_scope_access_retry(task: Dict[str, Any]):
        task.pop("access_retry_count", None)

    def _process_tv_postcheck_task(self, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        show_root = Path(str(task.get("show_root") or ""))
        mode = str(task.get("mode") or "episodes")
        batch_id = str(task.get("batch_id") or "")
        if not show_root.exists() or not show_root.is_dir():
            if manual:
                result = {"time": self._now_iso(), "action": "tv_postcheck_show_missing", "scope": str(show_root), "folder": str(show_root), "scrape": None, "scrape_msg": "手动检查时剧名目录不存在，保留原任务等待到期检查"}
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            has_retry, due_ts, retry_count = self._defer_scope_access(task, "waiting_tv_postcheck")
            action = "tv_postcheck_show_retry" if has_retry else "tv_postcheck_show_failed"
            message = (
                f"刮削后检查时剧名目录暂不可访问，已安排第 {retry_count} 次重试：{self._ts_to_str(due_ts)}"
                if has_retry else
                f"刮削后检查多次重试后剧名目录仍不可访问，任务结束：{show_root}"
            )
            result = {"time": self._now_iso(), "action": action, "scope": str(show_root), "folder": str(show_root), "scrape": None if has_retry else False, "scrape_msg": message}
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": not has_retry, **result}
        if mode == "root":
            self._clear_scope_access_retry(task)
            status = self._tv_root_metadata_status(show_root)
            if not status.get("has_any_metadata"):
                result = {
                    "time": self._now_iso(),
                    "action": "tv_root_postcheck_manual_incomplete_keep_waiting" if manual else "tv_root_postcheck_incomplete",
                    "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)),
                    "scrape": None,
                    "scrape_msg": ("手动检查后剧名根目录仍没有图片/nfo；保留原任务到期后再检查。" if manual else "剧信息刮削后检查完成，剧名根目录仍没有图片/nfo；请检查 MP 目录刮削是否成功。"),
                    "metadata": status,
                }
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": not manual, **result}
            result = {
                "time": self._now_iso(),
                "action": "tv_root_postcheck_complete",
                "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)),
                "scrape": None,
                "scrape_msg": f"剧信息刮削后检查完成，剧名根目录已有图片/nfo：图片 {int(status.get('image_count') or 0)} 个，nfo {int(status.get('nfo_count') or 0)} 个。",
                "metadata": status,
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        if mode == "season":
            season_dirs = [Path(x) for x in self._to_list(task.get("season_dirs") or [])]
            if not season_dirs:
                season_dirs = self._unique_season_dirs(self._unique_episode_paths(task.get("episodes") or []), show_root)
            season_dirs = [
                season_dir for season_dir in season_dirs
                if self._is_safe_tv_source_scope(show_root, season_dir, mode="season", require_exists=True)
            ]
            if not season_dirs:
                if manual:
                    result = {
                        "time": self._now_iso(),
                        "action": "tv_season_postcheck_manual_incomplete_keep_waiting",
                        "scope": str(show_root),
                        "folder": self._map_strm_path_to_scrape_path(str(show_root)),
                        "scrape": None,
                        "scrape_msg": "手动检查时没有可用的 Season 范围；保留原任务等待到期检查。",
                    }
                    task["last_msg"] = result["scrape_msg"]
                    self._append_history(state, result)
                    return {"remove": False, **result}
                has_retry, due_ts, retry_count = self._defer_scope_access(task, "waiting_tv_postcheck")
                action = "tv_season_postcheck_no_scope_retry" if has_retry else "tv_season_postcheck_no_scope_failed"
                message = (
                    f"季信息检查没有可用的 Season 范围，已安排第 {retry_count} 次重试：{self._ts_to_str(due_ts)}"
                    if has_retry else
                    "季信息检查多次重试后仍没有可用的 Season 范围，任务结束。"
                )
                result = {
                    "time": self._now_iso(), "action": action, "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)),
                    "scrape": None if has_retry else False, "scrape_msg": message,
                }
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": not has_retry, **result}
            self._clear_scope_access_retry(task)
            incomplete = []
            checked_count = 0
            for season_dir in season_dirs:
                if not season_dir:
                    continue
                checked_count += 1
                status = self._tv_season_metadata_status(show_root, season_dir)
                if not status.get("complete"):
                    incomplete.append((season_dir, status))
            if incomplete:
                names = "、".join([f"{sd.name} 缺 {','.join(st.get('missing') or [])}" for sd, st in incomplete[:5]])
                season_rescrape_count = max(int(self._to_float(task.get("season_rescrape_count"), 0)), 0)
                can_rescrape = not manual and season_rescrape_count < self.MAX_SEASON_POSTCHECK_RESCRAPES
                if can_rescrape:
                    for sd, _st in incomplete:
                        self._ensure_tv_metadata_retry_task(
                            state,
                            show_root,
                            sd,
                            purpose=f"季信息 {sd.name}",
                            mode="season",
                            season_dirs=[sd],
                            reason="季信息刮削后检查仍不完整，自动补触发 Season 目录刮削",
                            batch_id=f"season_retry_{sd.name}",
                            season_rescrape_count=season_rescrape_count + 1,
                        )
                if manual:
                    action = "tv_season_postcheck_manual_incomplete_keep_waiting"
                    scrape = None
                    message = f"手动检查了 {checked_count} 个季目录，仍有 {len(incomplete)} 个季信息不完整；保留原任务到期后再检查。"
                elif can_rescrape:
                    action = "tv_season_postcheck_incomplete"
                    scrape = None
                    message = f"季信息刮削后检查了 {checked_count} 个季目录，仍有 {len(incomplete)} 个季信息不完整；已自动加入一次 Season 目录短期重试/补刮削队列，不会加入长期单集复查队列。"
                else:
                    action = "tv_season_postcheck_failed"
                    scrape = False
                    message = f"季信息补刮后再次检查了 {checked_count} 个季目录，仍有 {len(incomplete)} 个季信息不完整；已达到自动补刮上限，任务结束，请检查 MP 刮削结果。"
                result = {
                    "time": self._now_iso(), "action": action, "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": scrape,
                    "scrape_msg": message, "missing_count": len(incomplete), "note": names,
                    "season_rescrape_count": season_rescrape_count,
                }
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": not manual, **result}
            result = {
                "time": self._now_iso(), "action": "tv_season_postcheck_complete", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                "scrape_msg": f"季信息刮削后检查完成，检查 {checked_count} 个季目录，季海报、Season 海报和 season.nfo 均已存在。"
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        if mode == "show":
            checked_episodes = self._list_strm_files(show_root)
            missing = [ep for ep in checked_episodes if not self._episode_image_status(ep).get("has_image")]
            checked_count = len(checked_episodes)
        else:
            episode_paths = self._unique_episode_paths(task.get("episodes") or [])
            # 兼容旧队列：老的 episodes 模式如果没有记录单集，才全剧扫描。
            if episode_paths:
                missing = [ep for ep in episode_paths if not self._episode_image_status(ep).get("has_image")]
                checked_count = len(episode_paths)
                checked_episodes = episode_paths
            else:
                checked_episodes = self._list_strm_files(show_root)
                missing = [ep for ep in checked_episodes if not self._episode_image_status(ep).get("has_image")]
                checked_count = len(checked_episodes)

        if checked_count <= 0:
            if manual:
                result = {
                    "time": self._now_iso(), "action": "tv_postcheck_no_episode_keep_waiting", "scope": str(show_root),
                    "folder": str(show_root), "scrape": None,
                    "scrape_msg": "手动检查未读取到 STRM 单集，保留原任务等待到期检查。",
                }
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            has_retry, due_ts, retry_count = self._defer_scope_access(task, "waiting_tv_postcheck")
            action = "tv_postcheck_no_episode_retry" if has_retry else "tv_postcheck_no_episode_failed"
            message = (
                f"刮削后检查未读取到 STRM 单集，已安排第 {retry_count} 次重试：{self._ts_to_str(due_ts)}"
                if has_retry else
                "刮削后检查多次重试仍未读取到 STRM 单集，任务结束。"
            )
            result = {"time": self._now_iso(), "action": action, "scope": str(show_root), "folder": str(show_root), "scrape": None if has_retry else False, "scrape_msg": message}
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": not has_retry, **result}
        self._clear_scope_access_retry(task)

        # 先按当前 STRM 状态清理旧 10 天复查，避免页面继续显示已经有图的旧缓存。
        cleaned_count = self._sync_tv_recheck_tasks(state, show_root)

        if missing:
            names = "、".join([self._episode_label(ep, show_root) for ep in missing[:8]])
            if manual:
                task["missing_preview"] = self._preview_from_episodes(show_root, missing, limit=8)
                result = {
                    "time": self._now_iso(), "action": "tv_postcheck_manual_missing_keep_waiting", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": f"手动检查了 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片；保留原任务到期后再检查，不提前加入 10 天复查队列。" + (f" 旧复查预览中有 {cleaned_count} 集当前已恢复，原任务仍保留到期。" if cleaned_count else ""),
                    "missing_count": len(missing), "note": names,
                    **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                    **self._episode_history_payload(show_root, missing, prefix="missing"),
                }
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            if str(task.get("on_missing") or "") == "episode_scrape":
                next_batch_id = self._make_task_batch_id("postdir")
                for ep in missing:
                    self._ensure_episode_scrape_task(
                        state,
                        ep,
                        show_root,
                        reason="metadata_directory_scrape_still_missing_image",
                        deleted_nfo=0,
                        postcheck_batch_id=next_batch_id,
                    )
                result = {
                    "time": self._now_iso(), "action": "tv_postcheck_missing_schedule_episode_scrape", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": f"目录刮削后检查了 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片，已创建单集刮削任务，实际刮削前会删除 CD2 同名 nfo；不会直接加入 10 天复查。" + (f" 旧复查预览中有 {cleaned_count} 集当前已恢复，原任务仍保留到期。" if cleaned_count else ""),
                    "missing_count": len(missing), "deleted_nfo": 0, "note": names,
                    **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                    **self._episode_history_payload(show_root, missing, prefix="missing"),
                }
                self._append_history(state, result)
                return {"remove": True, **result}

            recheck = self._ensure_tv_recheck_task(
                state,
                show_root,
                episodes=missing,
                reason=str(task.get("reason") or "post_scrape_missing_image"),
                batch_id=batch_id,
            )
            if not recheck.get("scheduled"):
                result = {
                    "time": self._now_iso(), "action": "tv_postcheck_complete", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": f"刮削后检查创建复查任务前再次确认，{checked_count} 集均已有对应图片；不加入 10 天复查队列。",
                    **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                }
                self._append_history(state, result)
                return {"remove": True, **result}
            scheduled_missing = self._unique_episode_paths(recheck.get("episodes") or missing)
            result = {
                "time": self._now_iso(), "action": "tv_postcheck_missing_schedule_recheck", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                "scrape_msg": f"刮削后检查了 {checked_count} 集，仍有 {len(scheduled_missing)} 集缺少对应图片，已加入 {self._tv_recheck_days:g} 天复查队列。" + (f" 旧复查预览中另有 {cleaned_count} 集当前已恢复，将保留到期后给出复查结果。" if cleaned_count else ""),
                "missing_count": len(scheduled_missing), "note": "、".join([self._episode_label(ep, show_root) for ep in scheduled_missing[:8]]),
                "recheck_days": self._to_float(recheck.get("recheck_days"), self._tv_recheck_days),
                **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                **self._episode_history_payload(show_root, scheduled_missing, prefix="missing"),
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        # 只刷新历史复查的页面预览；原任务保留到期后给出正式复查结果。
        cleaned_count += self._sync_tv_recheck_tasks(state, show_root)
        result = {
            "time": self._now_iso(), "action": "tv_postcheck_complete", "scope": str(show_root),
            "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
            "scrape_msg": f"刮削后检查完成，检查 {checked_count} 集，均已有对应图片；不加入 10 天复查队列。" + (f" 旧复查预览中有 {cleaned_count} 集当前已恢复，原任务保留到期后给出结果。" if cleaned_count else ""),
            **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    def _tv_recheck_days_for_task(self, task: Dict[str, Any]) -> float:
        """读取任务创建时的复查天数；旧任务尽量从时间戳还原。"""
        if task.get("recheck_days") not in (None, ""):
            return min(max(self._to_float(task.get("recheck_days"), self._tv_recheck_days), 0), 3650)
        first_seen_ts = self._to_float(task.get("first_seen_ts"), 0)
        due_ts = self._to_float(task.get("due_ts"), 0)
        if first_seen_ts > 0 and due_ts >= first_seen_ts:
            inferred_days = (due_ts - first_seen_ts) / 86400
            if inferred_days <= 3650:
                return inferred_days
        return min(max(self._to_float(self._tv_recheck_days, self.DEFAULT_TV_RECHECK_DAYS), 0), 3650)

    def _process_tv_recheck_task(self, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        show_root = Path(str(task.get("show_root") or ""))
        recheck_days = self._tv_recheck_days_for_task(task)
        recheck_payload = {"recheck_days": recheck_days}
        if not show_root.exists() or not show_root.is_dir():
            if manual:
                result = {"time": self._now_iso(), "action": "tv_recheck_show_missing", "scope": str(show_root), "folder": str(show_root), "scrape": None, "scrape_msg": "手动复查时剧名目录不存在，保留原任务等待到期复查", **recheck_payload}
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            has_retry, due_ts, retry_count = self._defer_scope_access(task, "waiting_tv_recheck")
            action = "tv_recheck_show_retry" if has_retry else "tv_recheck_show_failed"
            message = (
                f"到期复查时剧名目录暂不可访问，已安排第 {retry_count} 次重试：{self._ts_to_str(due_ts)}"
                if has_retry else
                f"到期复查多次重试后剧名目录仍不可访问，任务结束：{show_root}"
            )
            result = {"time": self._now_iso(), "action": action, "scope": str(show_root), "folder": str(show_root), "scrape": None if has_retry else False, "scrape_msg": message, **recheck_payload}
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": not has_retry, **result}
        episode_values = task.get("episodes") or []
        if episode_values:
            stored_candidates = self._unique_episode_paths(episode_values)
            candidates = [
                ep for ep in stored_candidates
                if self._is_safe_strm_episode_file(ep, show_root=show_root, require_exists=True)
            ]
            stale_count = len(stored_candidates) - len(candidates)
            if not candidates:
                self._clear_scope_access_retry(task)
                result = {
                    "time": self._now_iso(),
                    "action": "tv_recheck_stale_complete",
                    "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)),
                    "scrape": None,
                    "scrape_msg": f"{recheck_days:g}天复查任务中的 {len(stored_candidates)} 个 STRM 单集均已不存在或改名；任务结束，未删除 CD2 NFO，也未触发刮削。",
                    "media_type": "tv",
                    **recheck_payload,
                    **self._episode_history_payload(show_root, stored_candidates, prefix="checked"),
                }
                self._append_history(state, result)
                return {"remove": True, **result}
            missing = [ep for ep in candidates if not self._episode_image_status(ep).get("has_image")]
            checked_count = len(candidates)
        else:
            stale_count = 0
            # 兼容旧队列：老任务没有 episodes 时才全剧扫描。
            missing = self._find_missing_episode_images(show_root)
            checked_count = self._count_strm_files(show_root)
        checked_payload = candidates if episode_values else self._list_strm_files(show_root)
        if not episode_values and checked_count <= 0:
            if manual:
                result = {"time": self._now_iso(), "action": "tv_recheck_no_episode_keep_waiting", "scope": str(show_root), "folder": str(show_root), "scrape": None, "scrape_msg": "手动复查未读取到 STRM 单集，保留原任务等待到期复查。", **recheck_payload}
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            has_retry, due_ts, retry_count = self._defer_scope_access(task, "waiting_tv_recheck")
            action = "tv_recheck_no_episode_retry" if has_retry else "tv_recheck_no_episode_failed"
            message = (
                f"到期复查未读取到 STRM 单集，已安排第 {retry_count} 次重试：{self._ts_to_str(due_ts)}"
                if has_retry else
                "到期复查多次重试仍未读取到 STRM 单集，任务结束。"
            )
            result = {"time": self._now_iso(), "action": action, "scope": str(show_root), "folder": str(show_root), "scrape": None if has_retry else False, "scrape_msg": message, **recheck_payload}
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": not has_retry, **result}
        self._clear_scope_access_retry(task)
        if not missing:
            stale_note = f"；另有 {stale_count} 个旧 STRM 路径已不存在并已忽略" if stale_count else ""
            result = {"time": self._now_iso(), "action": "tv_recheck_complete", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None, "scrape_msg": f"{recheck_days:g}天复查完成，检查 {checked_count} 集，均已有对应图片{stale_note}", **recheck_payload, **self._episode_history_payload(show_root, checked_payload, prefix="checked")}
            self._append_history(state, result)
            return {"remove": True, **result}
        postcheck_batch_id = self._make_task_batch_id("recheck")
        for episode in missing:
            self._ensure_episode_scrape_task(
                state,
                episode,
                show_root,
                reason="tv_10day_recheck_missing_image",
                deleted_nfo=0,
                postcheck_batch_id=postcheck_batch_id,
            )
        result = {
            "time": self._now_iso(), "action": "tv_recheck_missing_episodes_schedule_scrape", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)),
            "scrape": None, "scrape_msg": f"{recheck_days:g}天复查检查 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片，已创建单集刮削任务，实际刮削前会删除 CD2 同名 nfo，{self._episode_scrape_delay_seconds:g} 秒后逐集刮削。",
            "missing_count": len(missing), "deleted_nfo": 0,
            "note": "、".join([self._episode_label(ep, show_root) for ep in missing[:8]]) + (f"；已忽略 {stale_count} 个不存在的旧 STRM 路径" if stale_count else ""),
            **recheck_payload,
            **self._episode_history_payload(show_root, missing, prefix="missing"),
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    # --------------------------- 队列与任务创建 ---------------------------

    @staticmethod
    def _make_task_batch_id(prefix: str = "batch") -> str:
        safe_prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", str(prefix or "batch"))[:24] or "batch"
        return f"{safe_prefix}_{int(time.time() * 1000)}"

    def _tv_episode_season_scope(self, show_root: Path, episode_strm: Path) -> Path:
        """返回电视剧初始任务的隔离范围。

        多季剧按 Season 目录隔离；单集直接放在剧名目录时，退回剧名目录。
        用于确保“先入第一季、后入第二季”时，第一季任务只检查第一季快照，不扫描整部剧。
        """
        try:
            show_root = Path(str(show_root))
            episode_strm = Path(str(episode_strm))
            parent = episode_strm.parent
            if not str(show_root) or not str(parent):
                return show_root
            try:
                parent.relative_to(show_root)
            except Exception:
                return show_root
            return parent if parent != show_root else show_root
        except Exception:
            return Path(str(show_root or ""))

    @staticmethod
    def _tv_scan_scope_label(scope_type: str) -> str:
        mapping = {"show": "整剧范围", "season": "整季范围", "episode": "单集范围", "empty": "空范围"}
        return mapping.get(str(scope_type or ""), "本次入库范围")

    def _expand_tv_initial_episode_scope(self, task: Dict[str, Any], show_root: Path, episode_paths: List[Path], raw_path: str = "") -> Tuple[List[Path], str, Path]:
        """根据入库范围决定电视剧初始检查的 STRM 扫描范围。

        目标：
        - 整部剧目录入库：扫描整部剧；
        - 整季/批量多集入库：扫描当前 Season；
        - 追更单集：只检查该集。

        这样既能补齐整季/整剧里被 webhook 漏掉的集，又不会在追更单集时误扫旧季旧集。
        """
        show_root = Path(str(show_root or ""))
        raw_text = self._normalise_path_text(str(raw_path or task.get("raw_path") or task.get("strm_path") or ""))
        season_scope_text = str(task.get("season_scope") or "").strip()
        candidates: List[Path] = []

        def add_files_from_dir(folder: Path):
            for ep in self._list_strm_files(folder):
                candidates.append(ep)

        # 1. 如果入库事件本身是目录，优先按目录范围扫描。
        try:
            raw_path_obj = Path(raw_text) if raw_text else Path("")
            if raw_text and raw_path_obj.exists() and raw_path_obj.is_dir():
                if str(show_root) and self._path_same_or_under(str(raw_path_obj), str(show_root)):
                    add_files_from_dir(raw_path_obj)
                    scope_name = "show" if raw_path_obj == show_root else "season"
                    return self._unique_episode_paths(candidates), scope_name, raw_path_obj
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：解析电视剧入库目录范围失败：{raw_text} - {err}")

        # 2. 普通入库事件是单集文件：先使用 webhook 合并到的快照。
        episode_paths = self._unique_episode_paths(episode_paths)
        episode_paths = [ep for ep in episode_paths if ep and ep.suffix.lower() == ".strm"]
        if not episode_paths:
            return [], "empty", show_root

        # 3. 同一批次跨多个 Season，视为整剧/多季批量入库，扫描剧名目录。
        season_dirs = self._unique_season_dirs(episode_paths, show_root)
        if len(season_dirs) > 1:
            all_eps = self._list_strm_files(show_root)
            return self._unique_episode_paths(all_eps or episode_paths), "show", show_root

        # 4. 同一 Season 内多集，视为整季/批量入库，扫描当前 Season，补齐可能漏掉的事件。
        if len(episode_paths) >= 2:
            season_dir = None
            if season_dirs:
                season_dir = season_dirs[0]
            elif season_scope_text:
                season_dir = Path(season_scope_text)
            if season_dir and str(season_dir) and season_dir.exists() and season_dir.is_dir():
                season_eps = self._list_strm_files(season_dir)
                return self._unique_episode_paths(season_eps or episode_paths), ("show" if season_dir == show_root else "season"), season_dir

        # 5. 追更单集，只检查该集。
        return episode_paths, "episode", episode_paths[0].parent if episode_paths else show_root

    def _ensure_episode_scrape_task(
        self,
        state: Dict[str, Any],
        episode_strm: Path,
        show_root: Path,
        reason: str = "",
        deleted_nfo: int = 0,
        postcheck_batch_id: str = "",
        scrape_target: str = "",
        refresh_batch: set = None,
    ):
        queue = state.setdefault("queue", {})
        if not self._is_safe_strm_episode_file(episode_strm, show_root=show_root, require_exists=True):
            logger.info(f"监控strm刮削网盘：跳过已不存在或越界的 STRM 单集任务：{episode_strm}")
            return False
        key = f"episode::{episode_strm}"
        due_ts = time.time() + max(self._episode_scrape_delay_seconds, 0)
        target = ""
        if scrape_target:
            target_path = Path(str(scrape_target))
            if self._is_matching_episode_scrape_target(episode_strm, target_path):
                target = str(target_path)
        if not target:
            target = self._map_episode_strm_to_scrape_target(episode_strm, refresh_batch=refresh_batch)
        existing = queue.get(key)
        if existing:
            # 如果旧任务里残留 .strm 目标，或者当前已能找到真实媒体文件，刷新成真实媒体路径。
            old_target = str(existing.get("scrape_target") or "")
            if target and (
                not old_target
                or not self._is_matching_episode_scrape_target(episode_strm, Path(old_target))
                or not Path(old_target).exists()
            ):
                existing["scrape_target"] = str(target)
            # 单集刮削任务本身可以取更早时间执行；真正的 10 分钟检查会在刮削事件发送成功后再创建。
            existing["due_ts"] = min(self._to_float(existing.get("due_ts"), due_ts), due_ts)
            existing["due_at"] = self._ts_to_str(self._to_float(existing.get("due_ts"), due_ts))
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["deleted_nfo"] = int(existing.get("deleted_nfo") or 0) + int(deleted_nfo or 0)
            if postcheck_batch_id and not existing.get("postcheck_batch_id"):
                existing["postcheck_batch_id"] = str(postcheck_batch_id)
            existing["last_msg"] = f"重复单集刮削任务已合并，等待：{existing['due_at']}；刮削成功后再计时 10 分钟检查。"
            self._schedule_delayed_check_until(self._to_float(existing.get("due_ts"), due_ts))
            return True
        queue[key] = {
            "task_type": "episode_scrape",
            "key": key,
            "episode_strm": str(episode_strm),
            "show_root": str(show_root),
            "scrape_target": str(target),
            "reason": reason,
            "deleted_nfo": int(deleted_nfo or 0),
            "postcheck_batch_id": str(postcheck_batch_id or self._make_task_batch_id("ep")),
            "retry_index": -1,
            "retry_count": 0,
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_episode_scrape",
            "last_msg": f"等待 {self._episode_scrape_delay_seconds:g} 秒后刮削单集；刮削成功后再计时 10 分钟检查"
        }
        self._schedule_delayed_check(max(self._episode_scrape_delay_seconds, 0))
        return True

    def _ensure_tv_postcheck_task(
        self,
        state: Dict[str, Any],
        show_root: Path,
        mode: str = "episodes",
        episodes: List[Path] = None,
        reason: str = "",
        batch_id: str = "",
        due_ts: float = None,
        season_dirs: List[Path] = None,
        on_missing: str = "",
        season_rescrape_count: int = 0,
    ):
        """创建刮削后检查任务。

        关键原则：
        - 单集模式只应在 MP 单集刮削事件发送成功后创建，10 分钟从成功发送时开始计算；
        - 同一批次可合并，但不再把新批次合并进旧批次，避免新集被旧到期时间提前检查；
        - 合并同批次时取更晚时间，保证这一批里最后一集也至少等待完整检查时间。
        """
        queue = state.setdefault("queue", {})
        episodes = episodes or []
        season_dirs = season_dirs or []
        mode = str(mode or "episodes")
        season_rescrape_count = max(int(self._to_float(season_rescrape_count, 0)), 0)
        batch_id = str(batch_id or "").strip()
        if mode == "episodes" and not batch_id:
            batch_id = self._make_task_batch_id("postcheck")
        if mode == "season" and not batch_id:
            batch_id = self._make_task_batch_id("seasoncheck")

        if due_ts is None:
            due_ts = time.time() + max(self._post_scrape_check_delay_minutes, 0) * 60
        due_ts = self._to_float(due_ts, 0)
        if due_ts <= 0:
            due_ts = time.time() + max(self._post_scrape_check_delay_minutes, 0) * 60

        if mode == "episodes":
            key = f"tv_postcheck::{mode}::{show_root}::{batch_id}"
        elif mode == "season":
            key = f"tv_postcheck::{mode}::{show_root}::{batch_id}"
        else:
            # 整剧刮削检查按剧名根目录保留唯一任务，重复事件只会校准时间，不单独创建批次。
            key = f"tv_postcheck::{mode}::{show_root}"

        existing = queue.get(key)
        episode_strings = [str(x) for x in self._unique_episode_paths(episodes)]
        season_strings = []
        seen_season = set()
        for item in season_dirs:
            text = str(item or "").strip()
            if text and text not in seen_season:
                seen_season.add(text)
                season_strings.append(text)

        if existing:
            if mode == "episodes":
                old_eps = self._to_list(existing.get("episodes") or [])
                merged = old_eps[:]
                for ep in episode_strings:
                    if ep not in merged:
                        merged.append(ep)
                existing["episodes"] = merged
            elif mode == "season":
                old_dirs = self._to_list(existing.get("season_dirs") or [])
                merged_dirs = old_dirs[:]
                for sd in season_strings:
                    if sd not in merged_dirs:
                        merged_dirs.append(sd)
                existing["season_dirs"] = merged_dirs

            old_due = self._to_float(existing.get("due_ts"), 0)
            # 同一批次取更晚时间，避免最后加入的集数还没等够 10 分钟就被检查。
            existing["due_ts"] = max(old_due or due_ts, due_ts)
            existing["due_at"] = self._ts_to_str(self._to_float(existing.get("due_ts"), due_ts))
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["reason"] = reason or existing.get("reason") or ""
            if on_missing:
                existing["on_missing"] = str(on_missing)
            if batch_id:
                existing["batch_id"] = batch_id
            if mode == "season":
                existing["season_rescrape_count"] = max(
                    int(self._to_float(existing.get("season_rescrape_count"), 0)),
                    season_rescrape_count,
                )
            existing["last_msg"] = f"刮削后检查任务已合并，检查时间：{existing.get('due_at')}"
            self._schedule_delayed_check_until(self._to_float(existing.get("due_ts"), due_ts))
            return

        queue[key] = {
            "task_type": "tv_postcheck",
            "key": key,
            "show_root": str(show_root),
            "mode": mode,
            "episodes": episode_strings,
            "season_dirs": season_strings,
            "batch_id": batch_id,
            "scrape_target": self._map_strm_path_to_scrape_path(str(show_root)),
            "reason": reason,
            "on_missing": str(on_missing or ""),
            "season_rescrape_count": season_rescrape_count if mode == "season" else 0,
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_tv_postcheck",
            "last_msg": "等待刮削后检查 STRM 图片/季信息是否生成"
        }
        self._schedule_delayed_check_until(due_ts)

    def _ensure_tv_recheck_task(self, state: Dict[str, Any], show_root: Path, episodes: List[Path] = None, reason: str = "", batch_id: str = "") -> Dict[str, Any]:
        """只把确认仍缺图的单集加入复查，并返回实际入队结果。"""
        queue = state.setdefault("queue", {})
        batch_id = str(batch_id or "").strip()
        key = f"tv_recheck::{show_root}::{batch_id}" if batch_id else f"tv_recheck::{show_root}"
        due_ts = time.time() + self._tv_recheck_days * 86400
        existing = queue.get(key)

        # 再次过滤，避免已经生成图片的集数被加入 10 天复查。
        episode_strings = []
        for ep in self._unique_episode_paths(episodes or []):
            if not self._episode_image_status(ep).get("has_image"):
                text = str(ep)
                if text not in episode_strings:
                    episode_strings.append(text)
        if not episode_strings:
            return {"scheduled": False, "created": False, "key": key, "episodes": [], "due_ts": 0, "recheck_days": self._tv_recheck_days}

        if isinstance(existing, dict):
            old_due = self._to_float(existing.get("due_ts"), 0)
            # 同一批 10 天复查以首次确认缺图时间为准，不因重复检查反复后延。
            if old_due:
                due_ts = old_due
            existing["due_ts"] = due_ts
            existing["due_at"] = self._ts_to_str(due_ts)
            old_eps = self._to_list(existing.get("episodes") or [])
            merged = old_eps[:]
            for ep in episode_strings:
                if ep not in merged:
                    merged.append(ep)
            existing["episodes"] = merged
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["reason"] = reason or existing.get("reason") or ""
            existing["recheck_days"] = self._tv_recheck_days_for_task(existing)
            if batch_id:
                existing["batch_id"] = batch_id
            self._sync_tv_recheck_tasks(state, show_root, task_keys=[key])
            if key in queue:
                queue[key]["last_msg"] = f"10天复查任务已合并，复查时间：{queue[key].get('due_at')}"
            return {"scheduled": True, "created": False, "key": key, "episodes": episode_strings, "due_ts": due_ts, "recheck_days": existing.get("recheck_days")}

        queue[key] = {
            "task_type": "tv_recheck",
            "key": key,
            "show_root": str(show_root),
            "episodes": episode_strings,
            "batch_id": batch_id,
            "scrape_target": self._map_strm_path_to_scrape_path(str(show_root)),
            "reason": reason,
            "recheck_days": self._tv_recheck_days,
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_tv_recheck",
            "missing_preview": self._preview_from_episodes(show_root, episode_strings, limit=8),
            "last_msg": f"10 分钟检查后仍缺图，等待 {self._tv_recheck_days:g} 天后复查"
        }
        return {"scheduled": True, "created": True, "key": key, "episodes": episode_strings, "due_ts": due_ts, "recheck_days": self._tv_recheck_days}

    def _schedule_delayed_check(self, delay_seconds: float):
        """按相对秒数安排一次短期队列检查。"""
        self._schedule_delayed_check_until(time.time() + max(float(delay_seconds or 0), 0))

    def _schedule_delayed_check_until(self, due_ts: float):
        """安排最近到期的短期任务。

        只用于 10 秒入库检查、30 秒单集刮削、10 分钟刮削后检查；
        10 天复查按用户要求交给兜底检查周期处理，避免创建长期 Timer。
        """
        try:
            due_ts = self._to_float(due_ts, 0)
            if not due_ts:
                return
            now_ts = time.time()
            delay = max(due_ts - now_ts, 0.5)

            # 清理已经结束的 Timer，避免长期运行后列表增大。
            self._timers = [t for t in list(getattr(self, "_timers", []) or []) if t.is_alive()]

            current_due = self._to_float(getattr(self, "_next_timer_due_ts", 0), 0)
            # 已有更早或相近的 Timer 时复用；如果新任务更早，则额外创建一个更早 Timer。
            if current_due and current_due <= due_ts + 1:
                return

            self._next_timer_due_ts = due_ts
            timer = Timer(delay, self.run_once)
            timer.daemon = True
            timer.start()
            self._timers.append(timer)
            logger.debug(f"监控strm刮削网盘：已安排短期队列检查，{delay:.1f} 秒后执行")
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：创建延迟检查任务失败：{err}")

    def _restore_queue_timers(self, state: Dict[str, Any] = None):
        """恢复最近的短期到期任务 Timer。

        MP 重启、保存配置或 stop_service 后，内存 Timer 会丢失；队列仍在。
        这里只恢复短期任务，10 天复查继续由兜底检查周期处理。
        """
        if not self._enabled:
            return
        try:
            state = state or self._load_state()
            queue = state.get("queue") or {}
            if not isinstance(queue, dict) or not queue:
                return
            now_ts = time.time()
            nearest_due = None
            for task in queue.values():
                if not isinstance(task, dict):
                    continue
                task_type = str(task.get("task_type") or "")
                if task_type not in {"initial", "movie_retry", "tv_metadata_retry", "episode_scrape", "tv_postcheck"}:
                    continue
                due_ts = self._to_float(task.get("due_ts"), 0)
                if not due_ts:
                    continue
                if due_ts <= now_ts:
                    nearest_due = now_ts + 0.5
                    break
                if nearest_due is None or due_ts < nearest_due:
                    nearest_due = due_ts
            if nearest_due:
                self._schedule_delayed_check_until(nearest_due)
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：恢复短期队列定时器失败：{err}")

    # --------------------------- 路径与元数据判断 ---------------------------

    def _analyse_incoming_strm_path(self, raw_path: str) -> Dict[str, Any]:
        path_text = self._normalise_path_text(raw_path)
        root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT)
        if not self._path_same_or_under(path_text, root):
            return {"success": False, "message": f"路径不在 STRM 检查根路径下：{path_text}"}

        # 优先使用用户配置/媒体库缓存里的“媒体库路径映射”。
        # 这样可以支持 Emby 媒体库名和 STRM 实际路径不一致的结构，
        # 例如：动漫|tv|/media/电视剧/国漫,/media/电视剧/日番。
        mapped_library = self._match_library_mapping(path_text, include_cache=False)
        if mapped_library:
            return self._analyse_by_library_mapping(path_text, mapped_library)

        rel_parts = self._relative_parts_under_root(path_text, root)
        if len(rel_parts) < self._target_depth:
            return {"success": False, "message": f"路径层级不足，至少需要 {self._target_depth} 级：{path_text}"}
        first = rel_parts[0].strip().lower()
        media_type = "tv" if first in {"电视剧", "剧集", "番剧", "动漫", "tv", "series", "shows"} else "movie"
        root_dir_text = self._join_posix(root, *rel_parts[:self._target_depth])
        scrape_dir = self._map_strm_path_to_scrape_path(root_dir_text)
        info = {
            "success": True,
            "media_type": media_type,
            "strm_path": path_text,
            "check_dir": root_dir_text,
            "scrape_dir": scrape_dir,
            "relative_parts": rel_parts
        }
        if media_type == "tv":
            info.update({"show_root": root_dir_text, "episode_strm": path_text})
        else:
            info.update({"movie_dir": root_dir_text})
        return info

    def _analyse_by_library_mapping(self, path_text: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        library_root = self._normalise_path_text(str(mapping.get("path") or "")).rstrip("/")
        media_type = str(mapping.get("type") or "").strip().lower()
        if media_type not in {"movie", "tv"}:
            media_type = "movie"
        rel_parts = self._relative_parts_under_root(path_text, library_root)
        if not rel_parts:
            return {"success": False, "message": f"路径没有落在媒体库子目录下：{path_text}"}

        # 映射路径一般指向“媒体库实际路径/分类目录”，其下一层是片名或剧名。
        # 如果入库事件直接给到媒体文件，仍尽量以父目录作为检查目录，避免把文件名当目录。
        if len(rel_parts) == 1 and Path(path_text).suffix.lower() == ".strm":
            root_dir_text = self._normalise_path_text(str(Path(path_text).parent))
        else:
            root_dir_text = self._join_posix(library_root, rel_parts[0])

        scrape_dir = self._map_strm_path_to_scrape_path(root_dir_text)
        info = {
            "success": True,
            "media_type": media_type,
            "strm_path": path_text,
            "check_dir": root_dir_text,
            "scrape_dir": scrape_dir,
            "relative_parts": self._relative_parts_under_root(path_text, self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT),
            "library_mapping": {
                "name": mapping.get("name"),
                "type": media_type,
                "path": library_root,
                "source": mapping.get("source") or "config",
            }
        }
        if media_type == "tv":
            info.update({"show_root": root_dir_text, "episode_strm": path_text})
        else:
            info.update({"movie_dir": root_dir_text})
        return info

    def _movie_metadata_status(self, movie_dir: Path) -> Dict[str, Any]:
        files = set()
        nfo_count = 0
        image_map = {"backdrop": False, "fanart": False, "poster": False}
        try:
            if movie_dir.exists() and movie_dir.is_dir():
                for item in movie_dir.iterdir():
                    if not item.is_file():
                        continue
                    name = item.name.lower()
                    stem = item.stem.lower()
                    suffix = item.suffix.lower()
                    files.add(name)
                    if suffix == ".nfo":
                        nfo_count += 1
                    if suffix in self._image_suffixes():
                        if stem == "backdrop" or stem.startswith("backdrop"):
                            image_map["backdrop"] = True
                        if stem == "fanart" or stem.startswith("fanart"):
                            image_map["fanart"] = True
                        if stem in {"poster", "folder", "cover"} or stem.startswith("poster"):
                            image_map["poster"] = True
        except Exception as err:
            return {"complete": False, "error": str(err), "missing": ["backdrop.*", "fanart.*", "poster/folder/cover.*", "*.nfo"]}
        missing = []
        if not image_map["backdrop"]:
            missing.append("backdrop.*")
        if not image_map["fanart"]:
            missing.append("fanart.*")
        if not image_map["poster"]:
            missing.append("poster/folder/cover.*")
        if nfo_count <= 0:
            missing.append("*.nfo")
        return {
            "complete": not missing,
            "missing": missing,
            "nfo_count": nfo_count,
            "images": image_map,
            "files": sorted(list(files))[:50]
        }

    def _tv_root_metadata_status(self, show_root: Path) -> Dict[str, Any]:
        """检查剧名根目录是否已有剧级元数据。

        只把 tvshow.nfo、根目录 nfo、poster/fanart/backdrop 等剧级图片算作剧信息；
        排除 seasonXX-poster、season-poster 等季级图片，避免误判“剧信息已存在”。
        """
        image_count = 0
        nfo_count = 0
        names: List[str] = []
        root_image_count = 0
        root_nfo_count = 0
        try:
            if show_root.exists() and show_root.is_dir():
                for item in show_root.iterdir():
                    if not item.is_file():
                        continue
                    suffix = item.suffix.lower()
                    stem = item.stem.lower()
                    if suffix in self._image_suffixes():
                        image_count += 1
                        names.append(item.name)
                        if not stem.startswith("season") and (
                            stem in {"poster", "folder", "cover", "fanart", "backdrop"}
                            or stem.startswith("poster") or stem.startswith("fanart") or stem.startswith("backdrop")
                        ):
                            root_image_count += 1
                    elif suffix == ".nfo":
                        nfo_count += 1
                        names.append(item.name)
                        if stem not in {"season"} and not stem.startswith("season"):
                            root_nfo_count += 1
        except Exception as err:
            return {"has_any_metadata": False, "error": str(err), "image_count": image_count, "nfo_count": nfo_count, "root_image_count": root_image_count, "root_nfo_count": root_nfo_count}
        return {
            "has_any_metadata": (root_image_count + root_nfo_count) > 0,
            "image_count": image_count,
            "nfo_count": nfo_count,
            "root_image_count": root_image_count,
            "root_nfo_count": root_nfo_count,
            "names": names[:50],
        }

    def _unique_season_dirs(self, episodes: List[Path], show_root: Path) -> List[Path]:
        """从本次入库单集中提取唯一季目录。

        只处理位于剧名根目录之下的季目录，例如：
        /media/电视剧/日韩剧/剧名/Season 2/E01.strm -> Season 2。
        如果单集直接位于剧名根目录，返回剧名根目录本身。
        """
        result: List[Path] = []
        seen = set()
        for ep in episodes or []:
            try:
                ep = Path(str(ep))
                parent = ep.parent
                if not str(parent):
                    continue
                # 正常多季结构：剧名根目录 / Season N / E01.strm
                try:
                    parent.relative_to(show_root)
                except Exception:
                    continue
                season_dir = parent if parent != show_root else show_root
                key = str(season_dir)
                if key and key not in seen:
                    seen.add(key)
                    result.append(season_dir)
            except Exception:
                continue
        return result

    def _tv_season_metadata_status(self, show_root: Path, season_dir: Path) -> Dict[str, Any]:
        """判断当前季信息是否完整。

        按 MP 当前生成结构判断：
        - 剧名根目录存在 season02-poster.*
        - 当前季目录存在 poster.*
        - 当前季目录存在 season.nfo
        三项都存在才认为季信息完整。
        """
        show_root = Path(show_root)
        season_dir = Path(season_dir)
        season_num = self._season_number_from_dir(season_dir)
        root_stems: List[str] = []
        if season_num is not None:
            # 能识别出具体季号时，只认可对应季海报，避免 Season 2 被通用 season-poster 误判为完整。
            root_stems.extend([f"season{season_num:02d}-poster", f"season{season_num}-poster"])
        else:
            # 只有无法识别季号时才使用通用 season-poster 兜底。
            root_stems.append("season-poster")
        root_poster = self._has_image_with_stems(show_root, root_stems)
        season_poster = self._has_image_with_stems(season_dir, ["poster"])
        season_nfo = self._has_file_case_insensitive(season_dir, "season.nfo")
        missing = []
        if not root_poster:
            missing.append((f"season{season_num:02d}-poster.*" if season_num is not None else "seasonXX-poster.*"))
        if not season_poster:
            missing.append("Season目录/poster.*")
        if not season_nfo:
            missing.append("Season目录/season.nfo")
        return {
            "complete": not missing,
            "missing": missing,
            "season_number": season_num,
            "root_poster": root_poster,
            "season_poster": season_poster,
            "season_nfo": season_nfo,
            "show_root": str(show_root),
            "season_dir": str(season_dir),
        }

    @staticmethod
    def _season_number_from_dir(season_dir: Path) -> Optional[int]:
        name = Path(season_dir).name.strip()
        patterns = [
            r"(?i)^season\s*(\d+)$",
            r"(?i)^s(\d+)$",
            r"第\s*(\d+)\s*季",
        ]
        for pattern in patterns:
            match = re.search(pattern, name)
            if match:
                try:
                    return int(match.group(1))
                except Exception:
                    return None
        return None

    def _has_image_with_stems(self, directory: Path, stems: List[str]) -> bool:
        stems_lower = {str(stem).lower() for stem in stems if stem}
        if not stems_lower:
            return False
        try:
            if not directory.exists() or not directory.is_dir():
                return False
            for item in directory.iterdir():
                if not item.is_file():
                    continue
                if item.suffix.lower() in self._image_suffixes() and item.stem.lower() in stems_lower:
                    return True
        except Exception:
            return False
        return False

    @staticmethod
    def _has_file_case_insensitive(directory: Path, filename: str) -> bool:
        wanted = str(filename or "").lower()
        if not wanted:
            return False
        try:
            if not directory.exists() or not directory.is_dir():
                return False
            for item in directory.iterdir():
                if item.is_file() and item.name.lower() == wanted:
                    return True
        except Exception:
            return False
        return False

    def _episode_image_status(self, episode_strm: Path) -> Dict[str, Any]:
        candidates = self._episode_image_candidates(episode_strm)
        exists = [str(p) for p in candidates if p.exists() and p.is_file()]
        # 兼容部分刮削器生成的短文件名图片，例如“猎犬 - S02E01-thumb.jpg”，
        # 只在精确同名候选不存在时，按 SxxEyy 编号在同一季目录内做轻量匹配。
        if not exists:
            exists = [str(p) for p in self._episode_image_loose_candidates(episode_strm) if p.exists() and p.is_file()]
        return {"has_image": bool(exists), "exists": exists, "candidates": [str(p) for p in candidates]}

    def _episode_image_loose_candidates(self, episode_strm: Path) -> List[Path]:
        result: List[Path] = []
        try:
            parent = episode_strm.parent
            if not parent.exists() or not parent.is_dir():
                return []
            match = re.search(r"(?i)s(\d{1,2})e(\d{1,3})", episode_strm.stem)
            if not match:
                return []
            season_no = int(match.group(1))
            episode_no = int(match.group(2))
            for item in parent.iterdir():
                if not item.is_file() or item.suffix.lower() not in self._image_suffixes():
                    continue
                stem = item.stem.lower()
                if stem.startswith("season"):
                    continue
                item_match = re.search(r"(?i)s(\d{1,2})e(\d{1,3})", item.stem)
                if not item_match:
                    continue
                if int(item_match.group(1)) == season_no and int(item_match.group(2)) == episode_no:
                    result.append(item)
        except Exception:
            return []
        return result

    def _episode_image_candidates(self, episode_strm: Path) -> List[Path]:
        parent = episode_strm.parent
        stem = episode_strm.stem
        suffixes = [".jpg", ".jpeg", ".png", ".webp"]
        names: List[str] = []
        for suffix in suffixes:
            names.append(f"{stem}{suffix}")
        for tag in ["-thumb", "-poster", "-fanart", "-backdrop"]:
            for suffix in suffixes:
                names.append(f"{stem}{tag}{suffix}")
        # 去重保持顺序
        seen = set()
        result = []
        for name in names:
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(parent / name)
        return result

    def _delete_episode_nfo(self, episode_strm: Path, media_target: Path = None, refresh_batch: set = None) -> Tuple[int, bool, str]:
        deleted = 0
        if media_target and not self._is_matching_episode_scrape_target(episode_strm, Path(media_target)):
            msg = f"跳过删除单集 nfo，真实媒体目标与 STRM 映射不同名或不在同一 CD2 目录：{media_target}"
            logger.warning(f"监控strm刮削网盘：{msg}")
            return deleted, True, msg
        media_path = Path(media_target) if media_target else None
        mapped = media_path or Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
        nfo = mapped.with_suffix(".nfo")
        if not self._is_safe_episode_nfo_delete_file(nfo):
            msg = f"跳过删除单集 nfo，目标不在 CD2 刮削根路径或仍位于 STRM 路径：{nfo}"
            logger.warning(f"监控strm刮削网盘：{msg}")
            return deleted, True, msg
        try:
            refresh_ok, refresh_msg = self._ensure_scrape_path_refreshed(nfo.parent, reason="删除 CD2 单集 nfo 前刷新父目录", refresh_batch=refresh_batch, force=True)
            if not nfo.exists():
                try:
                    matches = [
                        item for item in nfo.parent.iterdir()
                        if item.name.casefold() == nfo.name.casefold()
                    ]
                except Exception as err:
                    msg = f"无法确认 CD2 单集 nfo 是否存在，暂不触发刮削：{nfo.parent} - {err}"
                    logger.warning(f"监控strm刮削网盘：{msg}")
                    return deleted, True, msg
                if len(matches) > 1:
                    msg = f"发现多个仅大小写不同的 CD2 同名单集 nfo，无法安全确定删除目标：{nfo}"
                    logger.warning(f"监控strm刮削网盘：{msg}")
                    return deleted, True, msg
                if matches:
                    nfo = matches[0]
                    if not self._is_safe_episode_nfo_delete_file(nfo):
                        msg = f"大小写兼容匹配到的单集 nfo 未通过 CD2 安全校验：{nfo}"
                        logger.warning(f"监控strm刮削网盘：{msg}")
                        return deleted, True, msg
            nfo_exists = nfo.exists()
            if nfo_exists and nfo.is_file():
                nfo.unlink()
                deleted += 1
                msg = f"已删除 CD2 单集 nfo：{nfo}"
                logger.info(f"监控strm刮削网盘：{msg}")
                return deleted, False, msg
            if nfo_exists:
                msg = f"CD2 单集 nfo 路径存在但暂未确认为文件，暂不触发刮削：{nfo}"
                logger.warning(f"监控strm刮削网盘：{msg}")
                return deleted, True, msg
            if not refresh_ok:
                msg = f"删除 CD2 单集 nfo 前无法确认父目录已刷新，暂不触发刮削：{nfo.parent}；{refresh_msg}"
                logger.warning(f"监控strm刮削网盘：{msg}")
                return deleted, True, msg
            return deleted, False, f"未找到 CD2 单集 nfo：{nfo}"
        except Exception as err:
            msg = f"删除 CD2 单集 nfo 失败：{nfo} - {err}"
            logger.warning(f"监控strm刮削网盘：{msg}")
            return deleted, True, msg

    def _is_safe_episode_nfo_delete_file(self, target: Path) -> bool:
        """单集 nfo 删除安全保护：只能删除 CD2 刮削根路径下的 .nfo。"""
        try:
            text = self._normalise_path_text(str(target))
            if not text or Path(text).suffix.lower() != ".nfo":
                return False
            scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT).rstrip("/")
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT).rstrip("/")
            if not self._resolved_path_same_or_under(text, scrape_root):
                return False
            if strm_root and self._resolved_path_same_or_under(text, strm_root):
                return False
            return True
        except Exception:
            return False

    def _find_missing_episode_images(self, show_root: Path) -> List[Path]:
        missing: List[Path] = []
        try:
            for root, dirs, files in os.walk(show_root, followlinks=False):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if not self._path_is_link_or_junction(root_path / d)]
                for name in files:
                    if Path(name).suffix.lower() != ".strm":
                        continue
                    episode = root_path / name
                    if not self._episode_image_status(episode).get("has_image"):
                        missing.append(episode)
        except Exception as err:
            logger.warning(f"监控strm刮削网盘：检查剧集单集图片失败：{show_root} - {err}")
        return sorted(missing, key=lambda p: str(p))

    def _sync_tv_recheck_tasks(self, state: Dict[str, Any], show_root: Path, task_keys: List[str] = None) -> int:
        """只刷新指定剧复查任务的展示预览。

        原始任务和集数会保留到真正到期，由到期处理统一记录并通知复查结果。
        返回当前已恢复图片的单集数量。
        """
        queue = state.setdefault("queue", {})
        if not isinstance(queue, dict) or not queue:
            return 0
        show_text = str(show_root)
        selected = set(task_keys or [])
        removed_done = 0
        for key, item in list(queue.items()):
            if selected and key not in selected:
                continue
            if not isinstance(item, dict) or str(item.get("task_type") or "") != "tv_recheck":
                continue
            if str(item.get("show_root") or "") != show_text:
                continue

            episode_values = item.get("episodes") or []
            if not show_root.exists() or not show_root.is_dir():
                item["last_msg"] = "STRM 剧名目录当前不可访问；复查任务保留到期后重试。"
                continue
            if episode_values:
                stored_candidates = self._unique_episode_paths(episode_values)
                candidates = [
                    ep for ep in stored_candidates
                    if self._is_safe_strm_episode_file(ep, show_root=show_root, require_exists=True)
                ]
                stale_count = len(stored_candidates) - len(candidates)
                still_missing: List[Path] = []
                for ep in candidates:
                    if self._episode_image_status(ep).get("has_image"):
                        removed_done += 1
                    else:
                        still_missing.append(ep)
            else:
                stale_count = 0
                # 兼容旧队列：没有记录具体集数时才扫描整剧。
                still_missing = self._find_missing_episode_images(show_root)
                if not self._count_strm_files(show_root):
                    item["last_msg"] = "当前未读取到 STRM 单集；复查任务保留到期后重试。"
                    continue

            item["missing_preview"] = self._preview_from_episodes(show_root, still_missing, limit=8)
            item["current_missing_count"] = len(still_missing)
            recheck_days = self._tv_recheck_days_for_task(item)
            item["last_msg"] = (
                f"等待 {recheck_days:g} 天后复查；当前仍缺图 {len(still_missing)} 集。"
                if still_missing else
                f"当前缺图已恢复；任务保留到 {item.get('due_at') or '到期时间'} 后给出正式复查结果。"
            )
            if stale_count:
                item["last_msg"] += f" 已忽略 {stale_count} 个不存在的旧 STRM 路径。"
        return removed_done

    def _refresh_queue_preview_cache_for_display(self, state: Dict[str, Any]) -> bool:
        """在详情页状态副本上刷新 10 天复查预览。

        不保存、不删除真实队列任务；仅让页面显示当前仍缺图的集数。
        """
        queue = state.get("queue") or {}
        if not isinstance(queue, dict) or not queue:
            return False
        changed = False
        refreshed = 0
        for key, item in list(queue.items()):
            if not isinstance(item, dict):
                continue
            if str(item.get("task_type") or "") != "tv_recheck":
                continue
            show_root = Path(str(item.get("show_root") or ""))
            if not str(show_root):
                continue
            before = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
            self._sync_tv_recheck_tasks(state, show_root, task_keys=[key])
            after_item = queue.get(key)
            after = json.dumps(after_item, ensure_ascii=False, sort_keys=True, default=str) if isinstance(after_item, dict) else "__removed__"
            if before != after:
                changed = True
            refreshed += 1
            # 页面展示最多 10 个任务，防止一次打开页面扫描过多剧集。
            if refreshed >= 10:
                break
        return changed

    def _unique_episode_paths(self, values: Any) -> List[Path]:
        result: List[Path] = []
        seen = set()
        for value in self._to_list(values):
            if not value:
                continue
            ep = Path(str(value))
            key = str(ep)
            if not key or key in seen:
                continue
            seen.add(key)
            result.append(ep)
        return result

    def _list_strm_files(self, show_root: Path) -> List[Path]:
        episodes: List[Path] = []
        try:
            for root, dirs, files in os.walk(show_root, followlinks=False):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if not self._path_is_link_or_junction(root_path / d)]
                for name in files:
                    if Path(name).suffix.lower() == ".strm":
                        episodes.append(root_path / name)
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：枚举 STRM 集数失败：{show_root} - {err}")
        return sorted(episodes, key=lambda p: str(p))

    def _count_strm_files(self, show_root: Path) -> int:
        return len(self._list_strm_files(show_root))

    def _preview_from_episodes(self, show_root: Path, episodes: Any, limit: int = 8) -> Dict[str, Any]:
        eps = self._unique_episode_paths(episodes)
        take = max(int(limit or 0), 0)
        names = [self._episode_label(p, show_root) for p in eps[:take]]
        paths = [str(p) for p in eps[:take]]
        return {"exists": bool(show_root), "total": len(eps), "names": names, "paths": paths, "truncated": len(eps) > len(names), "refresh_time": self._now_iso()}

    @staticmethod
    def _episode_label(episode: Path, show_root: Path = None) -> str:
        try:
            if show_root:
                rel = episode.relative_to(show_root)
                return str(rel.with_suffix(""))
        except Exception:
            pass
        return episode.stem

    def _map_strm_path_to_scrape_path(self, path_text: str) -> str:
        path_text = self._normalise_path_text(path_text)
        return self._map_root(path_text, self._strm_check_root, self._scrape_target_root)

    def _scrape_refresh_scope_path(self, target: Path) -> Path:
        path = Path(target)
        suffix = path.suffix.lower()
        # .strm 不是实际刮削目标，只预热它映射后的所在目录；
        # 真实媒体文件必须按文件路径自身做就绪判断，不能降级成父目录，
        # 否则父目录有其它文件时会误缓存“已就绪”。
        if suffix == ".strm":
            return path.parent
        return path

    def _scrape_refresh_probe_paths(self, scope: Path) -> List[Path]:
        result: List[Path] = []
        seen = set()
        scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT).rstrip("/")
        current = Path(scope)
        for _ in range(3):
            text = self._normalise_path_text(str(current)).rstrip("/")
            if text and text.lower() not in seen and (not scrape_root or self._path_same_or_under(text, scrape_root)):
                seen.add(text.lower())
                result.append(current)
            parent = current.parent
            if parent == current:
                break
            current = parent
        return result

    def _get_storagechain(self):
        if StorageChain is None:
            return None
        if self._storagechain is None:
            self._storagechain = StorageChain()
        return self._storagechain

    def _call_storage_path_method(self, chain: Any, method_name: str, path: Path) -> Tuple[bool, str]:
        func = getattr(chain, method_name, None)
        if not callable(func):
            return False, ""
        lower_name = method_name.lower()
        is_list_method = any(token in lower_name for token in ("list", "files", "items"))

        if method_name == "list_files":
            try:
                fileitem = chain.get_file_item(storage=self._storage, path=path)
            except Exception as err:
                return False, str(err)
            if not fileitem:
                return False, f"get_file_item({path}) 返回空结果"
            attempts = (
                lambda: func(fileitem=fileitem),
                lambda: func(fileitem, recursion=False),
                lambda: func(fileitem),
            )
        else:
            attempts = (
                lambda: func(storage=self._storage, path=path),
                lambda: func(path=path, storage=self._storage),
                lambda: func(self._storage, path),
                lambda: func(path),
                lambda: func(str(path)),
            )
        last_err = ""
        for attempt in attempts:
            try:
                value = attempt()
                if is_list_method:
                    if value:
                        return True, f"{method_name}({path})"
                    last_err = f"{method_name}({path}) 返回空结果"
                    continue
                if value is False:
                    last_err = f"{method_name}({path}) 返回 False"
                    continue
                return True, f"{method_name}({path})"
            except Exception as err:
                last_err = str(err)
        return False, last_err

    def _touch_filesystem_path_for_refresh(self, probes: List[Path]) -> Tuple[bool, str]:
        last_err = ""
        for path in probes:
            try:
                if path.exists():
                    if path.is_dir():
                        first_item = next(path.iterdir(), None)
                        if first_item is not None:
                            return True, f"iterdir({path})"
                        last_err = f"目录暂为空或内容未刷新：{path}"
                        continue
                    return True, f"exists({path})"
            except Exception as err:
                last_err = str(err)
        return False, last_err

    def _ensure_scrape_path_refreshed(self, target: Path, reason: str = "", refresh_batch: set = None, force: bool = False) -> Tuple[bool, str]:
        """按需预热/探测 CD2 路径。

        只按具体路径去重，不再用“本批次已刷新过”覆盖所有层级。
        刷新成功才长时间缓存；刷新失败只短暂缓存，避免 CD2 刚好未就绪时 600 秒内不再尝试。
        """
        try:
            interval = max(float(self._scrape_path_refresh_interval_seconds or 0), 0)
        except Exception:
            interval = self.DEFAULT_SCRAPE_PATH_REFRESH_INTERVAL_SECONDS
        scope = self._scrape_refresh_scope_path(Path(target))
        scope_text = self._normalise_path_text(str(scope)).rstrip("/")
        scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT).rstrip("/")
        if not scope_text:
            return False, "刷新路径为空"
        if scrape_root and not self._path_same_or_under(scope_text, scrape_root):
            return False, f"跳过非刮削目标根路径：{scope_text}"

        key = scope_text.lower()
        if not force and refresh_batch is not None and key in refresh_batch:
            return True, "本批次已刷新过该路径"

        cache = getattr(self, "_scrape_path_refresh_cache", None)
        if not isinstance(cache, dict):
            cache = {}
            self._scrape_path_refresh_cache = cache
        now_ts = time.time()
        cache_item = cache.get(key)
        cache_ok = False
        last_ts = 0.0
        if isinstance(cache_item, dict):
            try:
                last_ts = self._to_float(cache_item.get("ts"), 0)
            except Exception:
                last_ts = 0.0
            cache_ok = bool(cache_item.get("ok"))
        else:
            try:
                last_ts = float(cache_item or 0)
                cache_ok = True
            except Exception:
                last_ts = 0.0
                cache_ok = False
        failure_interval = min(max(interval, 0), 30) if interval else 10
        effective_interval = interval if cache_ok else failure_interval
        cache_age = now_ts - last_ts if last_ts else -1
        if not force and effective_interval and last_ts and 0 <= cache_age < effective_interval:
            if cache_ok:
                if refresh_batch is not None:
                    refresh_batch.add(key)
                return True, f"{effective_interval:g} 秒内已刷新过该路径"
            return False, f"{effective_interval:g} 秒内已探测过该路径但未确认就绪，等待重试"

        probes = self._scrape_refresh_probe_paths(scope)
        ok = False
        detail = ""
        preheat_detail = ""
        try:
            chain = self._get_storagechain()
        except Exception as err:
            chain = None
            detail = f"初始化 StorageChain 失败：{err}"

        scope_ready_by_storage = False
        scope_is_media_file = Path(scope).suffix.lower() in self._media_suffixes()
        if chain is not None:
            # 先触碰目标层级和父级，触发 CD2 懒加载；
            # 但最终是否缓存为成功，必须以目标 scope 自己可见且非空为准。
            # 对真实媒体文件，只有目标文件本身 get_file_item 成功或文件存在，才算就绪；
            # 父目录非空只能作为预热信息，不能缓存为成功。
            for path in probes:
                try:
                    fileitem = chain.get_file_item(storage=self._storage, path=path)
                    if fileitem and not preheat_detail:
                        preheat_detail = f"get_file_item({path})"
                    if path == scope and fileitem and scope_is_media_file:
                        scope_ready_by_storage = True
                except Exception as err:
                    detail = str(err)
            for method_name in ("refresh", "refresh_path", "refresh_item", "list_files", "list_items", "list_file", "list", "get_files", "get_items", "get_file_list"):
                lower_name = method_name.lower()
                is_list_method = any(token in lower_name for token in ("list", "files", "items"))
                for path in probes:
                    method_ok, method_detail = self._call_storage_path_method(chain, method_name, path)
                    if method_ok and not preheat_detail:
                        preheat_detail = method_detail
                    if path == scope and method_ok and is_list_method and not scope_is_media_file:
                        scope_ready_by_storage = True

        fs_ok, fs_detail = self._touch_filesystem_path_for_refresh([scope])
        ok = bool(fs_ok or scope_ready_by_storage)
        if ok:
            detail = fs_detail or preheat_detail or detail
        else:
            _, parent_detail = self._touch_filesystem_path_for_refresh(probes[1:]) if len(probes) > 1 else (False, "")
            detail = fs_detail or preheat_detail or parent_detail or detail

        cache[key] = {"ts": now_ts, "ok": bool(ok)}
        expire_after = max(interval * 6, 3600) if interval else 3600
        for item_key, item_value in list(cache.items()):
            try:
                item_ts = self._to_float(item_value.get("ts") if isinstance(item_value, dict) else item_value, 0)
                if now_ts - item_ts > expire_after:
                    cache.pop(item_key, None)
            except Exception:
                cache.pop(item_key, None)
        if ok and refresh_batch is not None:
            refresh_batch.add(key)

        reason_text = f"（{reason}）" if reason else ""
        if ok:
            logger.info(f"监控strm刮削网盘：刮削前已刷新/探测 CD2 路径{reason_text}：{scope_text}，方式：{detail}")
            return True, detail or "已刷新/探测路径"
        logger.debug(f"监控strm刮削网盘：刮削前刷新/探测 CD2 路径未确认{reason_text}：{scope_text}，{detail or '无可用刷新方法'}")
        return False, detail or "刷新/探测路径未确认"

    def _map_strm_file_to_media_target(self, strm_path: Path, refresh_batch: set = None) -> str:
        """把 STRM 文件映射到 CD2 真实媒体文件。

        只按同名真实视频匹配，不读取 STRM 内容里的 HTTP 短链，避免短链名与网盘实际文件名不一致。
        单集真实文件采用文件级探测：即使父目录暂时不可见，也会逐个候选后缀尝试 StorageChain fileitem。
        """
        mapped = Path(self._map_strm_path_to_scrape_path(str(strm_path)))
        if mapped.suffix.lower() != ".strm":
            if not self._is_safe_episode_scrape_file(mapped):
                return ""
            mapped_ready, _ = self._ensure_scrape_path_refreshed(mapped, reason="单集真实媒体查找", refresh_batch=refresh_batch)
            return str(mapped) if mapped_ready or (mapped.exists() and mapped.is_file()) else ""

        parent = mapped.parent
        stem = mapped.stem
        try:
            # 父目录只做预热，不作为候选文件探测的前置条件；CD2/FUSE 下父目录可能还没本地可见，
            # 但 StorageChain 已经能按完整文件路径拿到目标 fileitem。
            self._ensure_scrape_path_refreshed(parent, reason="单集真实媒体父目录预热", refresh_batch=refresh_batch)

            for suffix in self._media_suffixes():
                candidate = parent / f"{stem}{suffix}"
                if not self._is_safe_episode_scrape_file(candidate):
                    continue
                candidate_ready, _ = self._ensure_scrape_path_refreshed(candidate, reason="单集真实媒体候选查找", refresh_batch=refresh_batch)
                if candidate_ready or (candidate.exists() and candidate.is_file()):
                    return str(candidate)

            if parent.exists() and parent.is_dir():
                for item in parent.iterdir():
                    if item.is_file() and item.stem == stem and self._is_safe_episode_scrape_file(item):
                        return str(item)
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：查找网盘真实媒体失败：{mapped} - {err}")
        return ""

    def _map_episode_strm_to_scrape_target(self, episode_strm: Path, refresh_batch: set = None) -> str:
        # STRM 库是 .strm，MP 实际刮削必须使用 CD2 中的真实媒体文件。
        # 找不到时返回空字符串，由任务处理阶段记录失败并通知，不再退回刮削 Season 目录。
        return self._map_strm_file_to_media_target(episode_strm, refresh_batch=refresh_batch)

    def _movie_same_name_candidate_paths(self, movie_strm: Path, movie_dir: Path) -> List[str]:
        """按 STRM 同名规则生成 CD2 真实电影文件候选路径。

        文件名必须与 STRM stem 一致，只枚举真实视频后缀；不会扫描视频目录选最大文件。
        """
        candidates: List[str] = []

        def add_from_strm(strm_file: Path):
            if not strm_file or not str(strm_file):
                return
            mapped = Path(self._map_strm_path_to_scrape_path(str(strm_file)))
            if mapped.suffix.lower() == ".strm":
                for suffix in self._media_suffixes():
                    candidate = mapped.with_suffix(suffix)
                    if self._is_safe_movie_scrape_file(candidate):
                        candidates.append(str(candidate))
            elif self._is_safe_movie_scrape_file(mapped):
                candidates.append(str(mapped))

        if movie_strm and str(movie_strm) and Path(str(movie_strm)).suffix.lower() == ".strm":
            add_from_strm(Path(str(movie_strm)))
        else:
            # 少数入库事件只给电影目录时，仅读取 STRM 目录中的 .strm 文件名来生成同名 CD2 候选。
            # 这里仍然不会把 STRM 文件或 STRM 目录作为刮削目标。
            try:
                if movie_dir and str(movie_dir) and Path(str(movie_dir)).exists() and Path(str(movie_dir)).is_dir():
                    strm_files = [p for p in Path(str(movie_dir)).iterdir() if p.is_file() and p.suffix.lower() == ".strm"]
                    for item in sorted(strm_files, key=lambda x: x.name.lower()):
                        add_from_strm(item)
            except Exception as err:
                logger.debug(f"监控strm刮削网盘：读取电影 STRM 文件名失败：{movie_dir} - {err}")
            if not candidates and movie_strm and str(movie_strm):
                add_from_strm(Path(str(movie_strm)))

        seen = set()
        result: List[str] = []
        for item in candidates:
            key = self._normalise_path_text(item).lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
        return result

    def _is_safe_movie_scrape_file(self, target: Path) -> bool:
        """电影刮削目标安全保护：只能是 CD2 真实视频文件候选。"""
        try:
            text = self._normalise_path_text(str(target))
            if not text:
                return False
            scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT).rstrip("/")
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT).rstrip("/")
            if not self._resolved_path_same_or_under(text, scrape_root):
                return False
            if strm_root and self._resolved_path_same_or_under(text, strm_root):
                return False
            suffix = Path(text).suffix.lower()
            if suffix == ".strm":
                return False
            if suffix not in self._media_suffixes():
                return False
            return True
        except Exception:
            return False

    def _trigger_movie_same_name_scrape(self, movie_strm: Path, movie_dir: Path) -> Tuple[bool, str, str, List[str]]:
        candidates = self._movie_same_name_candidate_paths(movie_strm, movie_dir)
        if not candidates:
            return False, "未生成 CD2 同名真实视频候选路径", "", []

        last_msg = ""
        for candidate_text in candidates:
            candidate = Path(candidate_text)
            ok, msg = self._trigger_movie_file_scrape(candidate)
            if ok:
                return True, msg, str(candidate), candidates
            last_msg = msg
        return False, last_msg or "所有 CD2 同名真实视频候选均无法获取 fileitem", "", candidates

    def _trigger_movie_file_scrape(self, target: Path) -> Tuple[bool, str]:
        """直接按电影真实视频候选触发刮削。

        这里不依赖 Path.exists()/is_file()，而是以 MP StorageChain 能否取得 fileitem 为准；
        发送事件时 file_list 固定为该真实视频路径，避免 FUSE 状态短暂异常导致 file_list 为空。
        """
        if not self._scrape:
            return False, "未启用刮削"
        if StorageChain is None:
            return False, "当前 MP 版本无法导入 StorageChain"
        target = Path(target)
        if not self._is_safe_movie_scrape_file(target):
            return False, f"电影刮削目标被安全规则拒绝：{target}"
        try:
            self._ensure_scrape_path_refreshed(target, reason="电影刮削")
            if self._storagechain is None:
                self._storagechain = StorageChain()
            fileitem = self._storagechain.get_file_item(storage=self._storage, path=target)
            if not fileitem:
                return False, f"无法获取电影真实文件项，等待 CD2 刷新：{target}"
            meta, mediainfo, recognize_msg = self._recognize_for_scrape_event(target)
            eventmanager.send_event(EventType.MetadataScrape, {
                "fileitem": fileitem,
                "file_list": [str(target)],
                "meta": meta,
                "mediainfo": mediainfo
            })
            msg = f"已发送 MP 电影刮削事件，文件：{target}" + (f"；{recognize_msg}" if recognize_msg else "")
            logger.info(f"监控strm刮削网盘：{msg}")
            return True, msg
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：电影真实文件刮削候选暂不可用：{target} - {err}")
            return False, f"电影真实文件候选暂不可用：{target} - {err}"


    def _trigger_tv_metadata_scrape(self, scope_dir: Path, purpose: str = "剧/季信息", refresh_batch: set = None) -> Tuple[bool, str, str]:
        """触发电视剧剧信息/季信息刮削。

        返回：(是否成功, 消息, 实际目标)。
        规则：按 MP 原生模式处理，剧信息/季信息只传 CD2 目录，不使用真实单集文件兜底。
        """
        mapped_dir = Path(self._map_strm_path_to_scrape_path(str(scope_dir)))
        if not self._is_safe_tv_scrape_dir(mapped_dir):
            return False, f"{purpose}目录刮削目标被安全规则拒绝：{mapped_dir}", str(mapped_dir)
        refresh_ok, refresh_msg = self._ensure_scrape_path_refreshed(mapped_dir, reason=purpose, refresh_batch=refresh_batch)
        if mapped_dir.exists() and mapped_dir.is_dir():
            ok, msg = self._trigger_scrape(mapped_dir, refresh_batch=refresh_batch)
            if ok:
                return True, msg, str(mapped_dir)
            return False, f"{purpose}目录刮削失败：{msg}", str(mapped_dir)
        refresh_note = f"；刮削前路径刷新：{refresh_msg}" if refresh_msg else ""
        if refresh_ok:
            refresh_note = f"；刮削前路径刷新：{refresh_msg}，但目录仍不可访问"
        return False, f"{purpose}目录不存在或不可访问：{mapped_dir}{refresh_note}", str(mapped_dir)

    def _episode_media_candidate_paths(self, episode_strm: Path) -> List[str]:
        """返回单集 STRM 映射到 CD2 后会尝试查找的同名视频路径。"""
        mapped = Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
        if mapped.suffix.lower() != ".strm":
            return [str(mapped)] if self._is_safe_episode_scrape_file(mapped) else []
        return [str(mapped.with_suffix(suffix)) for suffix in self._media_suffixes() if self._is_safe_episode_scrape_file(mapped.with_suffix(suffix))]

    def _is_safe_episode_scrape_file(self, target: Path) -> bool:
        """电视剧单集刮削目标安全保护：只能是 CD2 真实视频文件候选。"""
        try:
            text = self._normalise_path_text(str(target))
            if not text:
                return False
            scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT).rstrip("/")
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT).rstrip("/")
            if not self._resolved_path_same_or_under(text, scrape_root):
                return False
            if strm_root and self._resolved_path_same_or_under(text, strm_root):
                return False
            suffix = Path(text).suffix.lower()
            if suffix == ".strm":
                return False
            if suffix not in self._media_suffixes():
                return False
            return True
        except Exception:
            return False

    def _is_matching_episode_scrape_target(self, episode_strm: Path, target: Path) -> bool:
        """单集真实媒体必须与 STRM 映射到同一 CD2 目录且严格同 stem。"""
        try:
            target = Path(target)
            if not self._is_safe_episode_scrape_file(target):
                return False
            mapped = Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
            if mapped.suffix.lower() != ".strm":
                return False
            if target.stem != mapped.stem:
                return False
            return target.parent.resolve(strict=False) == mapped.parent.resolve(strict=False)
        except Exception:
            return False

    def _is_safe_tv_scrape_dir(self, target: Path) -> bool:
        """电视剧剧/季刮削目标安全保护：只能是 CD2 刮削根路径下的目录目标。"""
        try:
            text = self._normalise_path_text(str(target))
            if not text:
                return False
            scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT).rstrip("/")
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT).rstrip("/")
            if not self._resolved_path_same_or_under(text, scrape_root):
                return False
            if strm_root and self._resolved_path_same_or_under(text, strm_root):
                return False
            suffix = Path(text).suffix.lower()
            if suffix == ".strm" or suffix in self._media_suffixes():
                return False
            return True
        except Exception:
            return False

    def _trigger_scrape(self, target: Path, refresh_batch: set = None) -> Tuple[bool, str]:
        if not self._scrape:
            return False, "未启用刮削"
        if StorageChain is None:
            return False, "当前 MP 版本无法导入 StorageChain"
        target = Path(target)
        target_is_media_file = target.suffix.lower() in self._media_suffixes()
        if target_is_media_file:
            if not self._is_safe_episode_scrape_file(target):
                return False, f"文件刮削目标被 CD2 安全规则拒绝：{target}"
        elif not self._is_safe_tv_scrape_dir(target):
            return False, f"目录刮削目标被 CD2 安全规则拒绝：{target}"
        try:
            self._ensure_scrape_path_refreshed(target, reason="MP 刮削", refresh_batch=refresh_batch)
            if self._storagechain is None:
                self._storagechain = StorageChain()
            # 当前 MP 本地存储 get_item 需要 pathlib.Path，传 str 会触发：'str' object has no attribute 'exists'。
            # 因此这里统一使用 Path 对象，不再用字符串兜底。
            fileitem = self._storagechain.get_file_item(storage=self._storage, path=target)
            if not fileitem:
                return False, f"无法获取文件项，请确认路径在 MP 容器内可见：{target}"
            if target_is_media_file:
                # CD2/FUSE 有时 StorageChain 已能获取 fileitem，但 Path.is_file() 仍短暂返回 False。
                # 真实媒体文件目标直接固定 file_list，避免发送空列表导致 MP 单集刮削不生效。
                file_list = [str(target)]
            else:
                file_list = self._file_list_for_scrape(target)
            if target.is_dir() and not file_list:
                return False, f"目录下未找到真实媒体文件，等待 CD2 刷新：{target}"
            meta, mediainfo, recognize_msg = self._recognize_for_scrape_event(target)
            eventmanager.send_event(EventType.MetadataScrape, {
                "fileitem": fileitem,
                "file_list": file_list,
                "meta": meta,
                "mediainfo": mediainfo
            })
            scope = "文件" if target_is_media_file or target.is_file() else "目录"
            msg = f"已发送 MP 刮削事件，{scope}：{target}，媒体文件 {len(file_list)} 个" + (f"；{recognize_msg}" if recognize_msg else "")
            logger.info(f"监控strm刮削网盘：{msg}")
            return True, msg
        except Exception as err:
            logger.error(f"监控strm刮削网盘：触发刮削失败：{target} - {err}\n{traceback.format_exc()}")
            return False, str(err)

    def _recognize_for_scrape_event(self, target: Path) -> Tuple[Any, Any, str]:
        """按 MoviePilot 手动刮削方式预识别，并预取 Fanart/logo 等图片字段。"""
        if MediaChain is None:
            return None, None, "未预识别图片：当前 MP 版本无法导入 MediaChain"
        try:
            context = MediaChain().recognize_by_path(str(target), obtain_images=True)
            meta = getattr(context, "meta_info", None) if context else None
            mediainfo = getattr(context, "media_info", None) if context else None
            if not mediainfo:
                return meta, None, "未预识别到媒体信息，已回退 MP 事件内识别"
            logo_path = str(getattr(mediainfo, "logo_path", "") or "").strip()
            tvdb_id = str(getattr(mediainfo, "tvdb_id", "") or "").strip()
            tmdb_id = str(getattr(mediainfo, "tmdb_id", "") or "").strip()
            title = str(getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", "") or "").strip()
            parts = ["已按 MP 手动刮削路径预识别并预取图片"]
            if title:
                parts.append(title)
            if tmdb_id:
                parts.append(f"tmdb={tmdb_id}")
            if tvdb_id:
                parts.append(f"tvdb={tvdb_id}")
            parts.append("logo=已获取" if logo_path else "logo=未获取")
            return meta, mediainfo, "，".join(parts)
        except Exception as err:
            logger.warning(f"监控strm刮削网盘：刮削前预识别媒体信息失败，回退 MP 事件内识别：{target} - {err}")
            return None, None, "预识别失败，已回退 MP 事件内识别"

    def _file_list_for_scrape(self, target: Path) -> List[str]:
        scrape_root = self._normalise_path_text(self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT)
        if target.is_file():
            if self._path_is_link_or_junction(target) or not self._resolved_path_same_or_under(str(target), scrape_root):
                return []
            return [str(target)]
        files: List[str] = []
        try:
            for root, dirs, names in os.walk(target, followlinks=False):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if not self._path_is_link_or_junction(root_path / d)]
                for name in names:
                    candidate = root_path / name
                    if self._path_is_link_or_junction(candidate):
                        continue
                    if Path(name).suffix.lower() not in self._media_suffixes():
                        continue
                    if not self._resolved_path_same_or_under(str(candidate), scrape_root):
                        continue
                    files.append(str(candidate))
        except Exception:
            pass
        return files

    # --------------------------- 媒体库 / 服务 ---------------------------

    def _library_mapping_check_report(self) -> Dict[str, Any]:
        lines: List[str] = []
        errors: List[str] = []
        warnings: List[str] = []
        valid_count = 0
        mapping_text = str(self._library_path_mapping or "")
        root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT)
        raw_lines = [line.strip() for line in mapping_text.splitlines() if line.strip() and not line.strip().startswith("#")]
        if not raw_lines:
            return {"ok": True, "title": "未填写媒体库路径映射，将使用路径自动识别。", "lines": ["未填写映射；特殊库名与路径不一致时建议填写。"], "ts": self._now_iso()}

        for idx, line in enumerate(raw_lines, start=1):
            parts = [x.strip() for x in line.split("|")]
            if len(parts) < 3:
                errors.append(f"第 {idx} 行格式错误，应为：媒体库名称|类型|路径1,路径2")
                continue
            name, media_type = parts[0], parts[1].lower()
            paths_text = "|".join(parts[2:])
            paths = [self._normalise_path_text(x.strip()).rstrip("/") for x in re.split(r"[,，;；]+", paths_text) if x.strip()]
            if not name:
                errors.append(f"第 {idx} 行媒体库名称为空")
                continue
            if media_type not in {"movie", "tv"}:
                errors.append(f"第 {idx} 行类型必须是 movie 或 tv：{media_type or '-'}")
                continue
            if not paths:
                errors.append(f"第 {idx} 行没有填写路径")
                continue
            bad_paths = []
            warn_paths = []
            for path in paths:
                if not path.startswith("/"):
                    bad_paths.append(path)
                elif not self._path_same_or_under(path, root):
                    warn_paths.append(path)
            if bad_paths:
                errors.append(f"第 {idx} 行路径不是绝对路径：{', '.join(bad_paths[:2])}")
                continue
            if warn_paths:
                warnings.append(f"第 {idx} 行路径不在 STRM 根路径 {root} 下：{', '.join(warn_paths[:2])}")
            valid_count += len(paths)
            sample_path = self._mapping_sample_path(paths[0], media_type)
            try:
                analyse = self._analyse_by_library_mapping(sample_path, {"name": name, "type": media_type, "path": paths[0], "source": "config"})
                if analyse.get("success"):
                    lines.append(f"第 {idx} 行 OK：{name} / {media_type} / {paths[0]} → {self._short_path(str(analyse.get('check_dir') or ''), 48)}")
                else:
                    warnings.append(f"第 {idx} 行测试路径未能识别：{analyse.get('message')}")
            except Exception as err:
                warnings.append(f"第 {idx} 行测试异常：{err}")

        for msg in errors[:6]:
            lines.append(f"错误：{msg}")
        for msg in warnings[:4]:
            lines.append(f"提醒：{msg}")
        if not lines:
            lines.append("映射格式正常。")
        ok = not errors
        title = f"映射检测{'通过' if ok else '发现问题'}：有效路径 {valid_count} 个，错误 {len(errors)} 个，提醒 {len(warnings)} 个"
        return {"ok": ok, "title": title, "lines": lines[:10], "errors": errors, "warnings": warnings, "valid_count": valid_count, "ts": self._now_iso()}

    @staticmethod
    def _mapping_sample_path(root: str, media_type: str) -> str:
        root = str(root or "").rstrip("/")
        if str(media_type or "").lower() == "tv":
            return f"{root}/示例剧 (2026)/Season 1/示例剧 - S01E01 - 第 1 集.strm"
        return f"{root}/示例电影 (2026)/示例电影 (2026) - 2160p.strm"

    def _library_allowed(self, payload: Any, raw_path: str) -> Tuple[bool, str, Dict[str, Any]]:
        selected = self._parse_include_libraries()
        if not selected:
            return True, "未限制媒体库", {"mode": "all"}

        candidates = self._extract_library_candidates(payload)
        path_parts = self._relative_parts_under_root(raw_path, self._strm_check_root)
        if path_parts:
            # Emby 媒体库可能是第一层（电影/电视剧），也可能是第二层分类（华语电影/国产剧/日韩剧）。
            # 同时加入前两层和组合路径，避免“已选择国产剧但只识别到电视剧”的误判。
            candidates.append(path_parts[0])
            if len(path_parts) >= 2:
                candidates.append(path_parts[1])
                candidates.append(f"{path_parts[0]}/{path_parts[1]}")

        mapped_library = self._match_library_mapping(raw_path)
        if mapped_library:
            name = str(mapped_library.get("name") or "").strip()
            if name:
                candidates.append(name)
            path_value = str(mapped_library.get("path") or "").strip()
            if path_value:
                candidates.append(path_value)

        norm_selected = {self._bare_library_name(x) for x in selected if self._bare_library_name(x)}
        norm_candidates = {self._bare_library_name(x) for x in candidates if self._bare_library_name(x)}
        matched = sorted(norm_selected & norm_candidates)
        if matched:
            return True, "命中已选媒体库", {"mode": "selected", "selected": selected, "candidates": candidates, "matched": matched, "path_parts": path_parts, "mapping": mapped_library}
        return False, "未命中已选媒体库，已忽略", {"mode": "not_selected", "selected": selected, "candidates": candidates, "path_parts": path_parts, "raw_path": raw_path, "mapping": mapped_library}

    def _match_library_mapping(self, raw_path: str, include_cache: bool = True) -> Optional[Dict[str, Any]]:
        path_text = self._normalise_path_text(raw_path)
        matches: List[Dict[str, Any]] = []
        for mapping in self._configured_library_mappings():
            if not include_cache and str(mapping.get("source") or "") != "config":
                continue
            map_path = self._normalise_path_text(str(mapping.get("path") or "")).rstrip("/")
            if map_path and self._path_same_or_under(path_text, map_path):
                item = dict(mapping)
                item["path"] = map_path
                item["path_len"] = len(map_path)
                matches.append(item)
        if not matches:
            return None
        # 多个路径命中时取最长路径，避免 /media/电视剧 覆盖 /media/电视剧/国漫。
        matches.sort(key=lambda x: int(x.get("path_len") or 0), reverse=True)
        return matches[0]

    def _configured_library_mappings(self) -> List[Dict[str, Any]]:
        mappings: List[Dict[str, Any]] = []
        seen = set()

        def add_mapping(name: str, media_type: str, paths: List[str], source: str):
            clean_name = str(name or "").strip()
            clean_type = str(media_type or "").strip().lower()
            if clean_type in {"series", "show", "shows", "tvshow", "episode", "anime", "番剧", "电视剧", "剧集", "综艺", "纪录片"}:
                clean_type = "tv"
            elif clean_type in {"film", "movies", "电影", "影片"}:
                clean_type = "movie"
            if clean_type not in {"movie", "tv"}:
                clean_type = self._guess_library_type(clean_name, paths)
            for p in paths:
                path = self._normalise_path_text(str(p or "")).rstrip("/")
                if not clean_name or not path:
                    continue
                key = (clean_name.lower(), clean_type, path.lower())
                if key in seen:
                    continue
                seen.add(key)
                mappings.append({"name": clean_name, "type": clean_type, "path": path, "source": source})

        for line in str(self._library_path_mapping or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [x.strip() for x in line.split("|")]
            if len(parts) >= 3:
                name, media_type = parts[0], parts[1]
                paths_text = "|".join(parts[2:])
            elif len(parts) == 2:
                name, media_type = parts[0], ""
                paths_text = parts[1]
            else:
                continue
            paths = [x.strip() for x in re.split(r"[,，;；]+", paths_text) if x.strip()]
            add_mapping(name, media_type, paths, "config")

        # 如果媒体服务器缓存里能拿到 Library paths，也作为兜底映射。
        for lib in self._get_cached_libraries():
            if not isinstance(lib, dict):
                continue
            name = str(lib.get("name") or lib.get("title") or "").strip()
            paths = self._to_list(lib.get("paths") or [])
            media_type = str(lib.get("type") or lib.get("media_type") or "").strip().lower()
            add_mapping(name, media_type, paths, "cache")
        return mappings

    def _guess_library_type(self, name: str, paths: List[str]) -> str:
        text = " ".join([str(name or "")] + [str(x or "") for x in paths]).lower()
        # “动画电影”同时包含“动画”和“电影”，应按电影处理，所以先判断电影关键词。
        movie_words = ["电影", "movie", "movies", "film"]
        tv_words = ["电视剧", "剧集", "番剧", "动漫", "国漫", "日番", "综艺", "纪录片", "tv", "series", "show", "anime"]
        if any(word.lower() in text for word in movie_words):
            return "movie"
        if any(word.lower() in text for word in tv_words):
            return "tv"
        return "movie"

    @staticmethod
    def _bare_library_name(value: Any) -> str:
        text = str(value or "").strip()
        if ":" in text:
            text = text.split(":", 1)[1]
        return text.strip().lower()

    def _refresh_library_cache(self):
        libs: List[Dict[str, Any]] = []
        service = self._get_selected_media_service()
        if service:
            for item in self._get_service_libraries(service):
                norm = self._normalise_library_item(item)
                if norm.get("name"):
                    libs.append(norm)
        self._all_libraries = libs
        logger.info(f"监控strm刮削网盘：已刷新媒体库列表：{len(libs)} 个")

    def _library_cache_needs_refresh(self) -> bool:
        if not self._media_server:
            return False
        if not self._all_libraries:
            return True
        # 媒体库列表非常小，24小时自动刷新一次。
        data = self.get_data("library_cache_meta") or {}
        if isinstance(data, dict) and data.get("server") and data.get("server") != self._media_server:
            return True
        ts = self._to_float(data.get("ts"), 0) if isinstance(data, dict) else 0
        cache_age = time.time() - ts if ts else -1
        # 时间回拨、旧数据写入未来时间或缺少时间戳时都重新刷新。
        if not ts or cache_age < 0 or cache_age > 86400:
            return True
        return False

    def _get_media_server_select_items(self) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        for key, service in self._get_mp_media_services().items():
            stype = self._service_type(service)
            name = self._service_name(service, key)
            label = f"{name}" if not stype else f"{name} ({stype})"
            items.append({"title": label, "value": key})
        if self._media_server and not any(x.get("value") == self._media_server for x in items):
            items.insert(0, {"title": self._media_server, "value": self._media_server})
        return items

    def _get_library_select_items(self) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        for lib in self._get_cached_libraries():
            name = str(lib.get("name") or lib.get("title") or "").strip()
            if not name:
                continue
            sid = str(lib.get("server") or self._media_server or "emby").strip() or "emby"
            value = f"{sid}:{name}"
            items.append({"title": f"{sid}: {name}", "value": value})
        # 保留旧配置项，避免保存后丢失
        for selected in self._include_libraries:
            if selected and not any(x.get("value") == selected for x in items):
                items.append({"title": selected, "value": selected})
        return items

    def _get_cached_libraries(self) -> List[Dict[str, Any]]:
        if isinstance(self._all_libraries, list):
            return self._all_libraries
        return []

    def _get_selected_media_service(self) -> Any:
        if not self._media_server:
            return None
        services = self._get_mp_media_services()
        return services.get(self._media_server)

    def _get_mp_media_services(self) -> Dict[str, Any]:
        services: Dict[str, Any] = {}
        try:
            if self.mediaserver_helper:
                raw = None
                for name in ("get_services", "get_media_servers", "get_servers"):
                    func = getattr(self.mediaserver_helper, name, None)
                    if callable(func):
                        try:
                            raw = func()
                            if raw:
                                break
                        except Exception:
                            continue
                services.update(self._normalise_service_map(raw))
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：读取媒体服务器失败：{err}")
        try:
            if self.mschain:
                for name in ("get_mediaservers", "get_services", "get_servers"):
                    func = getattr(self.mschain, name, None)
                    if callable(func):
                        try:
                            raw = func()
                            if raw:
                                services.update(self._normalise_service_map(raw))
                                break
                        except Exception:
                            continue
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：读取 MediaServerChain 失败：{err}")
        return services

    def _get_service_libraries(self, service: Any) -> List[Any]:
        candidates = [self._get_service_instance(service), service]
        for obj in candidates:
            if not obj:
                continue
            for name in ("get_libraries", "get_librarys", "libraries", "librarys"):
                func = getattr(obj, name, None)
                if callable(func):
                    try:
                        raw = func()
                        if raw:
                            return self._normalise_library_raw(raw)
                    except Exception:
                        continue
                elif func:
                    return self._normalise_library_raw(func)
            # 尝试 Emby/Jellyfin API: /Library/VirtualFolders
            raw = self._mp_media_server_api_get_json(service, "/Library/VirtualFolders")
            if raw:
                return self._normalise_library_raw(raw)
        return []

    def _mp_media_server_api_get_json(self, service: Any, endpoint: str) -> Any:
        obj = self._get_service_instance(service) or service
        for name in ("get", "get_data", "get_json", "request"):
            func = getattr(obj, name, None)
            if callable(func):
                try:
                    return func(endpoint)
                except Exception:
                    try:
                        return func(path=endpoint)
                    except Exception:
                        continue
        return None

    @staticmethod
    def _normalise_service_map(raw: Any) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if not raw:
            return result
        if isinstance(raw, dict):
            for key, value in raw.items():
                result[str(key)] = value
            return result
        if isinstance(raw, (list, tuple, set)):
            for idx, value in enumerate(raw):
                name = LocalMetadataCleaner._service_name(value, f"server{idx+1}")
                result[name] = value
        return result

    @staticmethod
    def _service_name(service: Any, default: str = "") -> str:
        if isinstance(service, dict):
            return str(service.get("name") or service.get("server") or service.get("type") or default or "").strip()
        return str(getattr(service, "name", None) or getattr(service, "server", None) or getattr(service, "type", None) or default or "").strip()

    @staticmethod
    def _service_type(service: Any) -> str:
        if isinstance(service, dict):
            return str(service.get("type") or service.get("server_type") or "").strip()
        return str(getattr(service, "type", None) or getattr(service, "server_type", None) or "").strip()

    @staticmethod
    def _get_service_instance(service: Any) -> Any:
        if isinstance(service, dict):
            return service.get("instance") or service.get("client") or service.get("service")
        return getattr(service, "instance", None) or getattr(service, "client", None) or service

    def _normalise_library_item(self, item: Any) -> Dict[str, Any]:
        if isinstance(item, dict):
            data = dict(item)
        else:
            data = self._object_to_dict(item)
        name = str(data.get("Name") or data.get("name") or data.get("title") or data.get("library_name") or "").strip()
        paths = []
        raw_paths = data.get("Locations") or data.get("locations") or data.get("Paths") or data.get("paths") or data.get("Path") or data.get("path") or []
        if isinstance(raw_paths, str):
            raw_paths = [raw_paths]
        if isinstance(raw_paths, list):
            paths = [str(x) for x in raw_paths if str(x or "").strip()]
        return {"name": name, "server": self._media_server or "emby", "paths": paths, "raw": data}

    @staticmethod
    def _normalise_library_raw(raw: Any) -> List[Any]:
        if raw is None:
            return []
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            for key in ("Items", "items", "data", "Data", "libraries", "librarys", "result"):
                value = raw.get(key)
                if isinstance(value, list):
                    return value
            return list(raw.values())
        if isinstance(raw, (tuple, set)):
            return list(raw)
        return [raw]

    def _query_media_item_info(self, item_id: str, server_name: str = "") -> Dict[str, Any]:
        if not item_id:
            return {}
        service = None
        if server_name:
            service = self._get_mp_media_services().get(server_name)
        if not service:
            service = self._get_selected_media_service()
        if not service:
            return {}
        endpoints = [f"/Items/{item_id}", f"/Users/Me/Items/{item_id}"]
        for endpoint in endpoints:
            raw = self._mp_media_server_api_get_json(service, endpoint)
            if raw:
                return self._normalise_item_info(raw)
        return {}

    def _normalise_item_info(self, item: Any) -> Dict[str, Any]:
        data = item if isinstance(item, dict) else self._object_to_dict(item)
        return {
            "path": data.get("Path") or data.get("path") or data.get("FileName") or data.get("file_name") or "",
            "name": data.get("Name") or data.get("name") or "",
            "type": data.get("Type") or data.get("type") or "",
            "library_name": data.get("LibraryName") or data.get("library_name") or "",
            "raw": data
        }

    # --------------------------- 队列操作与状态 ---------------------------


    def _check_api_key(self, apikey: str) -> bool:
        token = str(getattr(settings, "API_TOKEN", "") or "")
        if not token:
            return True
        return str(apikey or "") == token

    def api_delete_queue(self, key: str = "", apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        key = str(key or "").strip()
        if not key:
            return schemas.Response(success=False, message="缺少任务标识")
        deleted, missing = self._delete_queue_items([key])
        if deleted:
            logger.info(f"监控strm刮削网盘：详情页删除队列任务 {key}")
            return schemas.Response(success=True, message="任务已删除")
        return schemas.Response(success=False, message="未找到任务" if missing else "删除失败")

    def api_delete_episode_group(self, show_root: str = "", status: str = "", apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        show_root = str(show_root or "").strip()
        status = str(status or "").strip()
        if not show_root:
            return schemas.Response(success=False, message="缺少剧名目录")
        with self._lock:
            state = self._load_state()
            queue = state.get("queue") or {}
            keys = []
            if isinstance(queue, dict):
                for key, meta in queue.items():
                    if not isinstance(meta, dict):
                        continue
                    if str(meta.get("task_type") or "") != "episode_scrape":
                        continue
                    if str(meta.get("show_root") or "") != show_root:
                        continue
                    if status and str(meta.get("status") or "") != status:
                        continue
                    keys.append(str(key))
        deleted, missing = self._delete_queue_items(keys)
        if deleted:
            logger.info(f"监控strm刮削网盘：详情页删除同剧单集任务组 {show_root}，共 {deleted} 个")
            return schemas.Response(success=True, message=f"已删除 {deleted} 个单集任务")
        return schemas.Response(success=False, message="未找到同剧单集任务" if not missing else "删除失败")

    def api_run_queue(self, key: str = "", apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        key = str(key or "").strip()
        if not key:
            return schemas.Response(success=False, message="缺少任务标识")
        done, kept, missing, messages = self._run_queue_items([key], manual=True)
        if done or kept:
            msg = messages[0] if messages else ("任务已执行" if done else "已检查，任务保留")
            logger.info(f"监控strm刮削网盘：详情页立即执行队列任务 {key}，完成 {done}，保留 {kept}")
            return schemas.Response(success=True, message=msg)
        return schemas.Response(success=False, message="未找到任务" if missing else "任务执行失败")

    def api_run_episode_group(self, show_root: str = "", status: str = "", apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        show_root = str(show_root or "").strip()
        status = str(status or "").strip()
        if not show_root:
            return schemas.Response(success=False, message="缺少剧名目录")
        with self._lock:
            state = self._load_state()
            queue = state.get("queue") or {}
            keys = []
            if isinstance(queue, dict):
                for key, meta in queue.items():
                    if not isinstance(meta, dict):
                        continue
                    if str(meta.get("task_type") or "") != "episode_scrape":
                        continue
                    if str(meta.get("show_root") or "") != show_root:
                        continue
                    if status and str(meta.get("status") or "") != status:
                        continue
                    keys.append(str(key))
        if not keys:
            return schemas.Response(success=False, message="未找到同剧单集任务")
        done, kept, missing, _messages = self._run_queue_items(keys, manual=True)
        if done or kept:
            logger.info(f"监控strm刮削网盘：详情页立即执行同剧单集任务组 {show_root}，完成 {done}，保留 {kept}")
            return schemas.Response(success=True, message=f"已立即执行 {done} 个单集任务" + (f"，保留 {kept} 个检查任务" if kept else ""))
        return schemas.Response(success=False, message="未找到同剧单集任务" if not missing else "任务执行失败")

    def _run_queue_items(self, keys: List[str], manual: bool = True) -> Tuple[int, int, int, List[str]]:
        keys = [str(x).strip() for x in keys if str(x or "").strip()]
        if not keys:
            return 0, 0, 0, []
        notify_results: List[Dict[str, Any]] = []
        try:
            with self._lock:
                self._active_notify_results = notify_results
                state = self._load_state()
                queue = state.setdefault("queue", {})
                done = 0
                kept = 0
                missing = 0
                messages: List[str] = []
                remove_keys: List[str] = []
                for key in keys:
                    task = queue.get(key)
                    if task is None:
                        missing += 1
                        continue
                    if not isinstance(task, dict):
                        remove_keys.append(key)
                        done += 1
                        continue
                    try:
                        result = self._process_queue_task(key, task, state, manual=manual)
                    except Exception as err:
                        logger.error(f"监控strm刮削网盘：手动执行队列任务失败 {key}：{err}\n{traceback.format_exc()}")
                        messages.append(f"任务执行失败：{err}")
                        continue
                    task["last_result"] = result
                    msg = str(result.get("scrape_msg") or result.get("message") or "").strip()
                    if msg:
                        messages.append(msg)
                    if result.get("remove", True):
                        remove_keys.append(key)
                        done += 1
                    else:
                        kept += 1
                for key in remove_keys:
                    queue.pop(key, None)
                self.save_data("state", state)
        finally:
            self._active_notify_results = None
        self._restore_queue_timers()
        if self._notify and notify_results:
            self._send_notify_results(notify_results)
        return done, kept, missing, messages

    def api_library_mapping_check(self, apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        report = self._library_mapping_check_report()
        try:
            self.save_data("library_mapping_check", report)
        except Exception:
            pass
        logger.info(f"监控strm刮削网盘：媒体库路径映射检测：{report.get('title')}")
        lines = self._to_list(report.get("lines") or [])
        message = str(report.get("title") or "检测完成")
        if lines:
            message += "；" + "；".join(lines[:3])
        return schemas.Response(success=bool(report.get("ok")), message=message)

    def api_clear_queue(self, apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        count = self._clear_queue_items()
        logger.info(f"监控strm刮削网盘：详情页清空待处理队列，共删除 {count} 个任务")
        return schemas.Response(success=True, message=f"已清空 {count} 个任务")

    def api_clear_history(self, apikey: str = ""):
        if not self._check_api_key(apikey):
            return schemas.Response(success=False, message="API密钥错误")
        with self._lock:
            state = self._load_state()
            history = state.get("history") or []
            count = len(history) if isinstance(history, list) else 0
            state["history"] = []
            self.save_data("state", state)
        logger.info(f"监控strm刮削网盘：详情页清空历史记录，共删除 {count} 条")
        return schemas.Response(success=True, message=f"已清空 {count} 条历史记录")

    def _delete_queue_items(self, keys: List[str]) -> Tuple[int, int]:
        keys = [str(x).strip() for x in keys if str(x or "").strip()]
        if not keys:
            return 0, 0
        with self._lock:
            state = self._load_state()
            queue = state.setdefault("queue", {})
            deleted = 0
            missing = 0
            for key in keys:
                meta = queue.pop(key, None)
                if meta is None:
                    missing += 1
                    continue
                deleted += 1
                if isinstance(meta, dict):
                    self._clear_task_markers(state, meta)
                self._append_history(state, {"time": self._now_iso(), "action": "queue_deleted_by_user", "scope": key, "folder": key, "scrape": None, "scrape_msg": "用户手动删除队列任务，已同步清理该任务相关去重标记，未执行刮削"})
            self.save_data("state", state)
        self._restore_queue_timers()
        return deleted, missing

    def _clear_queue_items(self) -> int:
        with self._lock:
            state = self._load_state()
            queue = state.setdefault("queue", {})
            count = len(queue) if isinstance(queue, dict) else 0
            if count:
                state["queue"] = {}
                # 队列全部清空时，同步清理本插件的去重标记，避免失败任务重新入库仍被合并跳过。
                state["markers"] = {}
                self._append_history(state, {"time": self._now_iso(), "action": "queue_cleared_by_user", "scope": "全部待处理队列", "folder": "全部待处理队列", "scrape": None, "scrape_msg": f"用户手动清空队列，共 {count} 个任务，并已清理去重标记"})
                self.save_data("state", state)
        self._restore_queue_timers()
        return count


    def _clear_task_markers(self, state: Dict[str, Any], task: Dict[str, Any]):
        """清理与队列任务相关的去重标记。

        手动删除队列任务后，如果不清理 marker，重新入库同一部剧时可能被误判为
        “近期已触发整剧刮削”，导致不再立即尝试刮削。
        """
        markers = state.get("markers")
        if not isinstance(markers, dict):
            return
        candidates = []
        for key in ("show_root", "check_dir", "raw_path", "strm_path", "episode_strm", "scope_dir"):
            value = str(task.get(key) or "").strip()
            if value:
                candidates.append(value)
                # 单集任务删除时同步尝试清理其季目录 marker。
                try:
                    path_value = Path(value)
                    if path_value.suffix.lower() == ".strm":
                        candidates.append(str(path_value.parent))
                except Exception:
                    pass
        for value in self._to_list(task.get("season_dirs") or []):
            if value:
                candidates.append(value)
        seen = set()
        for value in candidates:
            if value in seen:
                continue
            seen.add(value)
            markers.pop(f"tv_root_whole_scrape::{value}", None)
            markers.pop(f"tv_season_scrape::{value}", None)

    def _prune_stale_markers(self, state: Dict[str, Any], now_ts: float = None) -> int:
        """清理超过一小时的本插件目录刮削去重标记，限制持久状态增长。"""
        markers = state.get("markers")
        if not isinstance(markers, dict) or not markers:
            return 0
        now_ts = self._to_float(now_ts, time.time())
        prefixes = ("tv_root_whole_scrape::", "tv_season_scrape::")
        removed = 0
        for key, value in list(markers.items()):
            if not str(key).startswith(prefixes):
                continue
            marker_ts = self._to_float(value.get("ts"), 0) if isinstance(value, dict) else 0
            if not marker_ts or marker_ts > now_ts + 300 or now_ts - marker_ts > 3600:
                markers.pop(key, None)
                removed += 1
        return removed

    def _load_state(self) -> Dict[str, Any]:
        # 始终在独立副本上补默认字段；部分 MP 存储实现可能返回可变对象引用。
        data = copy.deepcopy(self.get_data("state") or {})
        if not isinstance(data, dict):
            data = {}
        data.setdefault("version", 4)
        data.setdefault("queue", {})
        data.setdefault("history", [])
        data.setdefault("markers", {})
        if not isinstance(data.get("queue"), dict):
            data["queue"] = {}
        if not isinstance(data.get("history"), list):
            data["history"] = []
        if not isinstance(data.get("markers"), dict):
            data["markers"] = {}
        return data

    def _append_history(self, state: Dict[str, Any], result: Dict[str, Any]):
        if result.get("skip_history"):
            return
        # 运行批次内单独收集结果，通知层只读取已有字段；旧队列/旧历史无需迁移。
        if isinstance(self._active_notify_results, list):
            self._active_notify_results.append(dict(result))
        history = state.setdefault("history", [])
        if not isinstance(history, list):
            history = []
            state["history"] = history
        # 去重：同一动作、同一范围、同一刮削目标在短时间内重复出现时，只更新计数，不刷屏。
        key = self._history_dedupe_key(result)
        now_ts = time.time()
        start_index = max(len(history) - 20, 0)
        for old_index in range(len(history) - 1, start_index - 1, -1):
            old = history[old_index]
            if not isinstance(old, dict):
                continue
            if old.get("_dedupe_key") != key:
                continue
            old_ts = self._to_float(old.get("_dedupe_ts"), 0)
            history_age = now_ts - old_ts if old_ts else -1
            if old_ts and 0 <= history_age <= 600:
                old["time"] = result.get("time") or old.get("time")
                for field in [
                    # 展示文字也要同步，否则同类记录 10 分钟内去重后，页面可能显示旧的“检查 X 集”。
                    "scrape_msg", "postcheck_msg", "metadata",
                    "checked_episodes", "checked_episode_labels", "checked_episode_total", "checked_episode_truncated",
                    "missing_episodes", "missing_episode_labels", "missing_episode_total", "missing_episode_truncated",
                    "scraped_episodes", "scraped_episode_labels", "scraped_episode_total", "scraped_episode_truncated",
                    "missing_count", "existing_count", "deleted_nfo", "note",
                ]:
                    if result.get(field) not in (None, "", [], {}):
                        old[field] = result.get(field)
                old["duplicate_count"] = int(old.get("duplicate_count") or 1) + 1
                old["_dedupe_ts"] = now_ts
                # 更新时间后的记录也应成为最新一条，保证概览和倒序历史一致。
                history.append(history.pop(old_index))
                state["history"] = history[-self.HISTORY_LIMIT:]
                return
        result["_dedupe_key"] = key
        result["_dedupe_ts"] = now_ts
        history.append(result)
        state["history"] = history[-self.HISTORY_LIMIT:]

    @staticmethod
    def _history_dedupe_key(result: Dict[str, Any]) -> str:
        return "|".join([
            str(result.get("action") or ""),
            str(result.get("scope") or ""),
            str(result.get("folder") or ""),
        ])

    # --------------------------- 请求、事件、路径工具 ---------------------------

    def _extract_payload_path(self, payload: Any) -> str:
        value = self._recursive_get_first(payload, ["path", "Path", "ItemPath", "item_path", "file_path", "FileName", "MediaPath"])
        return self._normalise_path_text(str(value or ""))

    def _extract_library_candidates(self, payload: Any) -> List[str]:
        candidates: List[str] = []
        for key in ("library_name", "LibraryName", "library", "Library", "library_id", "LibraryId", "media_library"):
            value = self._recursive_get_first(payload, [key])
            if value:
                candidates.append(str(value))
        return candidates

    def _event_info_path(self, event_info: Any, payload: Dict[str, Any]) -> str:
        for key in ("path", "item_path", "file_path", "itempath", "media_path"):
            value = self._get_obj_value(event_info, key) or payload.get(key)
            if value:
                return self._normalise_path_text(str(value))
        return self._extract_payload_path(payload)

    def _recursive_get_first(self, payload: Any, keys: List[str]) -> Any:
        if payload is None:
            return None
        if isinstance(payload, dict):
            for key in keys:
                if key in payload and payload[key]:
                    return payload[key]
            for value in payload.values():
                found = self._recursive_get_first(value, keys)
                if found:
                    return found
        elif isinstance(payload, (list, tuple)):
            for item in payload:
                found = self._recursive_get_first(item, keys)
                if found:
                    return found
        return None

    def _get_obj_value(self, obj: Any, key: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def _object_to_dict(self, obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    result[k] = v
                elif isinstance(v, (list, tuple)):
                    result[k] = [self._object_to_dict(x) if not isinstance(x, (str, int, float, bool)) else x for x in v]
                else:
                    result[k] = self._object_to_dict(v)
            return result
        data = {}
        for key in dir(obj):
            if key.startswith("_"):
                continue
            try:
                value = getattr(obj, key)
            except Exception:
                continue
            if callable(value):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                data[key] = value
        return data

    @staticmethod
    def _event_ignore_reason(result: Dict[str, Any]) -> str:
        message = str((result or {}).get("message") or "未登记任务").strip()
        if "未命中已选媒体库" in message:
            return "媒体库不匹配"
        if "路径不在 STRM" in message or "路径不在STRM" in message:
            return "路径不在STRM根目录"
        if "路径层级不足" in message:
            return "路径层级不足"
        if "未取得媒体路径" in message:
            return "未取得媒体路径"
        return message or "未登记任务"

    @staticmethod
    def _is_item_added_event(event_name: str) -> bool:
        if not event_name:
            return True
        name = str(event_name).strip().lower().replace("_", ".").replace("-", ".")
        allowed = {"itemadded", "item.added", "library.new", "media.added", "itemcreated", "item.created", "newitem", "new.item", "item.add"}
        return name in allowed or "itemadded" in name or ("added" in name and "item" in name)

    @staticmethod
    def _normalise_path_text(path_text: str) -> str:
        text = str(path_text or "").strip().strip('"').strip("'")
        text = text.replace("\\", "/")
        while "//" in text and not re.match(r"^[a-zA-Z]+://", text):
            text = text.replace("//", "/")
        return text.rstrip("/") if text != "/" else "/"

    @staticmethod
    def _normalise_config_root(value: Any, default: str, allow_filesystem_root: bool = False) -> str:
        text = LocalMetadataCleaner._normalise_path_text(str(value or default))
        if text == "/":
            return "/" if allow_filesystem_root else LocalMetadataCleaner._normalise_path_text(default)
        return text or LocalMetadataCleaner._normalise_path_text(default)

    @staticmethod
    def _path_has_unsafe_segments(path_text: str) -> bool:
        text = LocalMetadataCleaner._normalise_path_text(path_text)
        return any(part in {".", ".."} for part in text.split("/") if part)

    @staticmethod
    def _path_same_or_under(child: str, parent: str) -> bool:
        child = LocalMetadataCleaner._normalise_path_text(child)
        parent = LocalMetadataCleaner._normalise_path_text(parent)
        if not child or not parent:
            return False
        if LocalMetadataCleaner._path_has_unsafe_segments(child) or LocalMetadataCleaner._path_has_unsafe_segments(parent):
            return False
        if child == parent:
            return True
        return child.startswith(parent.rstrip("/") + "/")

    @staticmethod
    def _resolved_path_same_or_under(child: str, parent: str) -> bool:
        """校验解析符号链接后的真实边界；任一解析失败时按不安全处理。"""
        if not LocalMetadataCleaner._path_same_or_under(child, parent):
            return False
        try:
            child_path = Path(LocalMetadataCleaner._normalise_path_text(child)).resolve(strict=False)
            parent_path = Path(LocalMetadataCleaner._normalise_path_text(parent)).resolve(strict=False)
            if child_path == parent_path:
                return True
            child_path.relative_to(parent_path)
            return True
        except Exception:
            return False

    @staticmethod
    def _path_is_link_or_junction(path: Path) -> bool:
        try:
            if path.is_symlink():
                return True
            is_junction = getattr(path, "is_junction", None)
            return bool(callable(is_junction) and is_junction())
        except Exception:
            return True

    def _is_safe_strm_episode_file(self, target: Path, show_root: Path = None, require_exists: bool = True) -> bool:
        """验证单集任务仍绑定在有效 STRM 文件上。"""
        try:
            text = self._normalise_path_text(str(target or ""))
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT)
            if not text or Path(text).suffix.lower() != ".strm":
                return False
            if not self._resolved_path_same_or_under(text, strm_root):
                return False
            show_raw = str(show_root or "").strip()
            show_text = "" if show_raw in {"", "."} else self._normalise_path_text(show_raw)
            if show_text and not self._resolved_path_same_or_under(text, show_text):
                return False
            target_path = Path(text)
            if require_exists and (not target_path.exists() or not target_path.is_file()):
                return False
            return True
        except Exception:
            return False

    def _existing_movie_strm_source(self, movie_strm: Path, movie_dir: Path) -> Optional[Path]:
        """返回仍存在的电影 STRM；目录型旧任务仅在影片目录内选择 STRM。"""
        if self._is_safe_strm_episode_file(movie_strm, require_exists=True):
            return Path(movie_strm)
        try:
            directory = Path(movie_dir)
            directory_text = self._normalise_path_text(str(directory))
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT)
            if not directory.exists() or not directory.is_dir():
                return None
            if not self._resolved_path_same_or_under(directory_text, strm_root):
                return None
            for item in sorted(directory.iterdir(), key=lambda value: str(value)):
                if self._is_safe_strm_episode_file(item, require_exists=True):
                    return item
        except Exception:
            return None
        return None

    def _is_safe_tv_source_scope(self, show_root: Path, scope_dir: Path, mode: str = "root", require_exists: bool = True) -> bool:
        """验证剧/季任务的 STRM 范围没有串剧或跳出配置根路径。"""
        try:
            show_text = self._normalise_path_text(str(show_root or ""))
            scope_text = self._normalise_path_text(str(scope_dir or ""))
            strm_root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT)
            if not show_text or not scope_text:
                return False
            if not self._resolved_path_same_or_under(show_text, strm_root):
                return False
            if not self._resolved_path_same_or_under(scope_text, show_text):
                return False
            if str(mode or "root") == "root":
                if Path(show_text).resolve(strict=False) != Path(scope_text).resolve(strict=False):
                    return False
            if require_exists:
                if not Path(show_text).exists() or not Path(show_text).is_dir():
                    return False
                if not Path(scope_text).exists() or not Path(scope_text).is_dir():
                    return False
            return True
        except Exception:
            return False

    @staticmethod
    def _map_root(path: str, left: str, right: str) -> str:
        path = LocalMetadataCleaner._normalise_path_text(path)
        left = LocalMetadataCleaner._normalise_path_text(left)
        right = LocalMetadataCleaner._normalise_path_text(right)
        if not path or not left or not right:
            return path
        if (
            LocalMetadataCleaner._path_has_unsafe_segments(path)
            or LocalMetadataCleaner._path_has_unsafe_segments(left)
            or LocalMetadataCleaner._path_has_unsafe_segments(right)
        ):
            return path
        if path == left:
            return right
        if left == "/" and path.startswith("/"):
            return right.rstrip("/") + path
        if path.startswith(left.rstrip("/") + "/"):
            return right.rstrip("/") + path[len(left):]
        return path

    def _relative_parts_under_root(self, path_text: str, root: str) -> List[str]:
        path_text = self._normalise_path_text(path_text)
        root = self._normalise_path_text(root)
        if not self._path_same_or_under(path_text, root):
            return []
        rel = path_text[len(root):].lstrip("/")
        return [part for part in rel.split("/") if part]

    @staticmethod
    def _join_posix(*parts: str) -> str:
        cleaned = []
        for idx, part in enumerate(parts):
            text = str(part or "")
            if idx == 0:
                cleaned.append("/" if text == "/" else text.rstrip("/"))
            else:
                cleaned.append(text.strip("/"))
        values = [x for x in cleaned if x]
        if values and values[0] == "/":
            return "/" + "/".join(values[1:])
        return "/".join(values) or "/"

    @staticmethod
    def _image_suffixes() -> set:
        return {".jpg", ".jpeg", ".png", ".webp"}

    @staticmethod
    def _media_suffixes() -> tuple:
        # 按常见程度排序，单集刮削时会依次尝试把 .strm 替换成这些视频后缀。
        return (".mkv", ".mp4", ".ts", ".m2ts", ".iso", ".mov", ".avi", ".rmvb", ".wmv", ".flv", ".mpeg", ".mpg")

    def _send_notify_results(self, results: List[Dict[str, Any]]):
        """只通知用户关心的最终节点，并按同一媒体合并同批次结果。"""
        grouped: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        for index, result in enumerate(results or []):
            entry = self._notification_entry(result)
            if not entry:
                continue
            kind = str(entry.get("kind") or "")
            if kind == "success":
                key = (
                    kind,
                    str(entry.get("media_type") or ""),
                    str(entry.get("media_name") or ""),
                    str(entry.get("identity") or ""),
                )
            elif kind.startswith("recheck_"):
                key = (
                    kind,
                    str(entry.get("media_type") or ""),
                    str(entry.get("media_name") or ""),
                    str(entry.get("identity") or ""),
                    str(entry.get("recheck_days") or ""),
                )
            elif kind == "failure":
                key = (
                    kind,
                    str(entry.get("media_type") or ""),
                    str(entry.get("media_name") or ""),
                    str(entry.get("identity") or ""),
                    str(entry.get("reason") or ""),
                )
            else:
                key = (kind, str(index))
            old = grouped.get(key)
            if not old:
                entry["merged_count"] = 1
                entry["categories"] = [str(entry.get("category") or "")] if entry.get("category") else []
                entry["scope_labels"] = [str(entry.get("scope_label") or "")] if entry.get("scope_label") else []
                grouped[key] = entry
                continue
            self._merge_notification_entry(old, entry)

        for entry in grouped.values():
            try:
                title, body = self._notification_message(entry)
                if not title or not body:
                    continue
                self.post_message(mtype=NotificationType.SiteMessage, title=title, text=body)
            except Exception as err:
                logger.error(f"监控strm刮削网盘：发送通知失败：{err}")

    def _notification_entry(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(result, dict):
            return None
        action = str(result.get("action") or "")
        scrape = result.get("scrape")
        confirmed_success_actions = {
            "tv_root_postcheck_complete",
            "tv_season_postcheck_complete",
            "tv_postcheck_complete",
        }
        explicit_failure_actions = {
            "tv_root_postcheck_incomplete",
            "tv_season_postcheck_failed",
            "tv_postcheck_show_failed",
            "tv_postcheck_no_episode_failed",
            "tv_recheck_show_failed",
            "tv_recheck_no_episode_failed",
            "tv_season_postcheck_no_scope_failed",
            "queue_task_exception_failed",
        }
        if scrape is False or action in explicit_failure_actions:
            kind = "failure"
        elif action in confirmed_success_actions:
            kind = "success"
        elif action == "tv_postcheck_missing_schedule_recheck":
            kind = "recheck_scheduled"
        elif action == "tv_recheck_complete":
            kind = "recheck_complete"
        elif action == "tv_recheck_stale_complete":
            kind = "recheck_skipped"
        elif action == "tv_recheck_missing_episodes_schedule_scrape":
            kind = "recheck_missing"
        else:
            return None

        media_type, media_name, category, scope_label, identity = self._notification_media(result, action)
        episode_labels = (
            self._notification_episode_labels(result, kind)
            if category == "episode" or kind.startswith("recheck_") or action == "tv_postcheck_complete"
            else []
        )
        if episode_labels:
            category = "episode"
        return {
            "kind": kind,
            "media_type": media_type,
            "media_name": media_name,
            "category": category,
            "scope_label": scope_label,
            "identity": identity,
            "episode_labels": episode_labels,
            "episode_total": self._notification_episode_total(result, episode_labels),
            "recheck_days": self._to_float(result.get("recheck_days"), self._tv_recheck_days),
            "reason": self._notification_failure_reason(result, action) if kind == "failure" else "",
            "action": action,
        }

    def _notification_media(self, result: Dict[str, Any], action: str) -> Tuple[str, str, str, str, str]:
        """从新旧结果的公共字段回退识别媒体；不要求迁移历史或队列。"""
        scope_text = str(result.get("scope") or result.get("show_root") or result.get("folder") or "").strip()
        scope = Path(scope_text) if scope_text else Path("")
        explicit_type = str(result.get("media_type") or "").lower()
        is_movie = explicit_type == "movie" or action.startswith("movie_")
        if is_movie:
            name = str(result.get("media_name") or "").strip()
            if not name:
                name = scope.parent.name if scope.suffix.lower() == ".strm" else scope.name
            movie_root = scope.parent if scope.suffix.lower() == ".strm" else scope
            identity = self._normalise_path_text(str(movie_root)).rstrip("/").lower()
            return "movie", name or "未知电影", "movie", "", identity

        show_root_text = str(result.get("show_root") or "").strip()
        if show_root_text:
            show_root = Path(show_root_text)
        elif scope.suffix.lower() == ".strm":
            show_root = scope.parent.parent if self._looks_like_season_dir(scope.parent.name) else scope.parent
        elif self._looks_like_season_dir(scope.name):
            show_root = scope.parent
        else:
            show_root = scope
        name = str(result.get("media_name") or "").strip() or show_root.name or scope.name or "未知电视剧"
        identity = self._normalise_path_text(str(show_root)).rstrip("/").lower()

        if scope.suffix.lower() == ".strm" or action.startswith("episode_"):
            return "tv", name, "episode", "", identity
        if self._looks_like_season_dir(scope.name) or "season" in action:
            scope_label = scope.name if self._looks_like_season_dir(scope.name) else "季信息"
            return "tv", name, "season", scope_label, identity
        if "root" in action or action.startswith("tv_metadata_retry"):
            return "tv", name, "show", "整剧", identity
        return "tv", name, "show", "", identity

    def _notification_episode_labels(self, result: Dict[str, Any], kind: str) -> List[str]:
        field_order = ["scraped_episode_labels", "missing_episode_labels", "checked_episode_labels"]
        path_order = ["scraped_episodes", "missing_episodes", "checked_episodes"]
        if kind in {"recheck_scheduled", "recheck_missing"}:
            field_order = ["missing_episode_labels", "checked_episode_labels", "scraped_episode_labels"]
            path_order = ["missing_episodes", "checked_episodes", "scraped_episodes"]
        elif kind == "recheck_complete":
            field_order = ["checked_episode_labels", "missing_episode_labels", "scraped_episode_labels"]
            path_order = ["checked_episodes", "missing_episodes", "scraped_episodes"]

        values: List[str] = []
        for field in field_order:
            values = [str(x) for x in self._to_list(result.get(field) or []) if str(x or "").strip()]
            if values:
                break
        if not values:
            for field in path_order:
                values = [str(x) for x in self._to_list(result.get(field) or []) if str(x or "").strip()]
                if values:
                    break
        scope = str(result.get("scope") or "").strip()
        if not values and Path(scope).suffix.lower() == ".strm":
            values = [scope]
        if not values and kind in {"recheck_scheduled", "recheck_missing"}:
            note = str(result.get("note") or "")
            values = [x.strip() for x in re.split(r"[、,，]", note) if x.strip()]

        labels: List[str] = []
        for value in values:
            label = self._compact_episode_label(value)
            if label and label not in labels:
                labels.append(label)
        return labels

    @staticmethod
    def _compact_episode_label(value: str) -> str:
        text = str(value or "").strip()
        match = re.search(r"(?i)(S\d{1,3}E\d{1,4})", text)
        if match:
            return match.group(1).upper()
        stem = Path(text).stem if text else ""
        return stem[:40]

    @staticmethod
    def _looks_like_season_dir(name: str) -> bool:
        return bool(re.match(r"(?i)^season(?:\s+|\s*[-_.]?\s*)\d+$", str(name or "").strip()))

    def _notification_failure_reason(self, result: Dict[str, Any], action: str) -> str:
        reasons = {
            "movie_retry_scrape_failed": "CD2 未找到同名真实视频文件",
            "movie_scrape_target_missing": "CD2 未找到同名真实视频文件",
            "episode_scrape_retry_failed": "CD2 未找到同名真实视频文件",
            "episode_scrape_target_missing": "CD2 未找到同名真实视频文件",
            "episode_scrape_target_rejected": "刮削目标未通过 CD2 安全校验",
            "episode_nfo_delete_failed": "CD2 同名 NFO 删除失败",
            "tv_metadata_retry_failed": "CD2 剧名或季目录仍不可用",
            "episode_scrape_failed": "MoviePilot 未能触发单集刮削",
            "tv_root_postcheck_incomplete": "刮削后剧名根目录仍没有图片或 NFO",
            "tv_season_postcheck_failed": "季信息补刮后仍不完整，已达到自动补刮上限",
            "tv_postcheck_show_failed": "刮削后多次无法访问 STRM 剧名目录",
            "tv_postcheck_no_episode_failed": "刮削后多次未读取到 STRM 单集",
            "tv_recheck_show_failed": "到期复查时多次无法访问 STRM 剧名目录",
            "tv_recheck_no_episode_failed": "到期复查时多次未读取到 STRM 单集",
            "tv_season_postcheck_no_scope_failed": "季信息检查没有可用的 Season 范围",
            "queue_task_exception_failed": "待处理任务连续执行异常",
        }
        if action in reasons:
            return reasons[action]
        msg = str(result.get("scrape_msg") or self._action_label(action) or "处理失败").replace("\n", " ").strip()
        return msg[:160]

    def _merge_notification_entry(self, old: Dict[str, Any], entry: Dict[str, Any]):
        old["merged_count"] = int(old.get("merged_count") or 1) + 1
        labels = self._to_list(old.get("episode_labels") or [])
        for label in self._to_list(entry.get("episode_labels") or []):
            if label and label not in labels:
                labels.append(label)
        old["episode_labels"] = labels

        categories = self._to_list(old.get("categories") or [])
        category = str(entry.get("category") or "")
        if category and category not in categories:
            categories.append(category)
        old["categories"] = categories

        scope_labels = self._to_list(old.get("scope_labels") or [])
        scope_label = str(entry.get("scope_label") or "")
        if scope_label and scope_label not in scope_labels:
            scope_labels.append(scope_label)
        old["scope_labels"] = scope_labels

        old_total = int(self._to_float(old.get("episode_total"), len(labels)))
        new_total = int(self._to_float(entry.get("episode_total"), len(self._to_list(entry.get("episode_labels") or []))))
        old["episode_total"] = max(old_total, new_total, len(labels))
        if not old.get("reason") and entry.get("reason"):
            old["reason"] = entry.get("reason")

    @staticmethod
    def _notification_category_label(category: str) -> str:
        labels = {
            "movie": "电影",
            "show": "整剧",
            "season": "季信息",
            "episode": "单集图片",
        }
        return labels.get(str(category or ""), str(category or ""))

    @staticmethod
    def _notification_episode_total(result: Dict[str, Any], labels: List[str]) -> int:
        totals: List[int] = [len(labels or [])]
        for field in ("scraped_episode_total", "missing_episode_total", "checked_episode_total"):
            try:
                value = int(result.get(field) or 0)
                if value > 0:
                    totals.append(value)
            except Exception:
                pass
        return max(totals) if totals else 0

    def _notification_message(self, entry: Dict[str, Any]) -> Tuple[str, str]:
        kind = str(entry.get("kind") or "")
        media_type = str(entry.get("media_type") or "tv")
        media_name = str(entry.get("media_name") or ("未知电影" if media_type == "movie" else "未知电视剧"))
        labels = [str(x) for x in self._to_list(entry.get("episode_labels") or []) if str(x or "").strip()]
        episode_total = int(self._to_float(entry.get("episode_total"), len(labels)))
        episode_text = self._notification_episode_text(labels, total=episode_total)
        media_label = "电影" if media_type == "movie" else "电视剧"
        recheck_days = f"{self._to_float(entry.get('recheck_days'), self._tv_recheck_days):g}天"

        if kind == "success":
            categories = self._to_list(entry.get("categories") or [])
            if not categories and entry.get("category"):
                categories = [str(entry.get("category") or "")]
            range_labels: List[str] = []
            for category in categories:
                label = self._notification_category_label(category)
                if label and label not in range_labels:
                    range_labels.append(label)
            title = "〖刮削完成〗" if len(range_labels) > 1 or int(entry.get("merged_count") or 1) > 1 else "〖刮削成功〗"
            lines = [f"{media_label}：{media_name}"]
            if range_labels:
                lines.append(f"范围：{' / '.join(range_labels)}")
            if episode_text:
                lines.append(f"集数：{episode_text}")
            elif entry.get("scope_label"):
                lines.append(f"范围：{entry.get('scope_label')}")
            if int(entry.get("merged_count") or 1) > 1:
                lines.append(f"通知：已合并 {int(entry.get('merged_count') or 1)} 条处理记录")
            return title, "\n".join(lines)
        if kind == "recheck_scheduled":
            lines = [f"电视剧：{media_name}"]
            if episode_text:
                lines.append(f"缺图：{episode_text}")
            lines.append(f"复查：{recheck_days}后")
            return f"〖已列入{recheck_days}复查〗", "\n".join(lines)
        if kind == "recheck_complete":
            lines = [f"电视剧：{media_name}"]
            lines.append(f"已恢复：{episode_text}" if episode_text else "结果：缺图已恢复")
            return f"〖{recheck_days}复查完成〗", "\n".join(lines)
        if kind == "recheck_missing":
            lines = [f"电视剧：{media_name}"]
            if episode_text:
                lines.append(f"仍缺图：{episode_text}")
            lines.append("处理：已创建补刮削任务")
            return f"〖{recheck_days}复查仍缺图〗", "\n".join(lines)
        if kind == "recheck_skipped":
            lines = [f"电视剧：{media_name}"]
            if episode_text:
                lines.append(f"集数：{episode_text}")
            lines.append("结果：对应 STRM 已不存在，未执行刮削")
            return f"〖{recheck_days}复查结束〗", "\n".join(lines)
        if kind == "failure":
            action = str(entry.get("action") or "")
            lines = [f"{media_label}：{media_name}"]
            if episode_text:
                lines.append(f"集数：{episode_text}")
            elif entry.get("scope_label"):
                lines.append(f"范围：{entry.get('scope_label')}")
            lines.append(f"原因：{entry.get('reason') or '处理失败'}")
            if action.startswith("tv_recheck_"):
                title = "〖复查失败〗"
            elif action.startswith("tv_postcheck_") or action == "tv_root_postcheck_incomplete":
                title = "〖刮削检查失败〗"
            elif action == "queue_task_exception_failed":
                title = "〖任务异常〗"
            else:
                title = "〖刮削失败〗"
            return title, "\n".join(lines)
        return "", ""

    @staticmethod
    def _notification_episode_text(labels: List[str], limit: int = 8, total: int = 0) -> str:
        values = [str(x) for x in labels or [] if str(x or "").strip()]
        if not values:
            return ""
        preview = "、".join(values[:limit])
        count = max(int(total or 0), len(values))
        if len(values) <= limit and count <= len(values):
            return preview
        return f"{preview} 等，共{count}集"

    def _queue_title(self, key: str, item: Dict[str, Any]) -> str:
        task_type = str(item.get("task_type") or "")
        if task_type == "tv_recheck":
            show_root = Path(str(item.get("show_root") or ""))
            name = show_root.name or str(show_root)
            days = self._tv_recheck_days_for_task(item)
            preview = item.get("missing_preview") or {}
            if preview.get("total", 0) > 0:
                return f"{days:g}天复查：{name}（当前缺图 {preview.get('total')} 集）"
            return f"{days:g}天复查：{name}"
        if task_type == "tv_postcheck":
            show_root = Path(str(item.get("show_root") or ""))
            return f"刮削后检查：{show_root.name or str(show_root)}"
        if task_type == "tv_metadata_retry":
            scope_dir = Path(str(item.get("scope_dir") or ""))
            purpose = str(item.get("purpose") or "剧/季信息")
            return f"CD2就绪重试：{purpose} {scope_dir.name or str(scope_dir)}"
        if task_type == "episode_scrape":
            ep = Path(str(item.get("episode_strm") or ""))
            return f"单集刮削：{ep.stem or key}"
        if task_type == "initial":
            ep = Path(str(item.get("episode_strm") or item.get("strm_path") or item.get("raw_path") or ""))
            return f"入库检查：{ep.stem or key}"
        return key

    def _help_text(self) -> str:
        return (
            "逻辑说明：\n"
            "1. 本插件适用于想要整理刮削网盘的用户，依赖‘媒体库服务器通知’插件和 CloudDrive2（CD2）。\n"
            "2. 原理：收到 Emby 入库通知后，先检查 STRM 路径里的刮削信息是否完整；判断需要刮削时，再去刮削 CD2 挂载的网盘文件。MP 刮削是固定核心功能，不再提供关闭开关。\n"
            "3. MP 必须同时映射 STRM 文件夹和 CD2 文件夹，建议 STRM 映射路径与 Emby 一致。例如 Emby 是 /media，MP 也建议映射为 /media。\n"
            "4. STRM 检查根路径填写 Emby/MP 看到的 STRM 根目录，例如 /media；MP 刮削目标根路径填写 MP 看到的 CD2 网盘媒体根目录，例如 /CD2/115/CMS影库/影视。兜底检查周期只用于补跑丢失或到期未执行的队列任务。\n"
            "5. 路径示例：/media/电影/华语电影/片名/xxx.strm 用于检查；需要刮削时映射为 /CD2/115/CMS影库/影视/电影/华语电影/片名。\n"
            "6. 电影：检查片名目录是否同时存在 backdrop、fanart、poster/folder/cover 类图片和任意 nfo；完整则跳过，不完整则只用 CD2 同名真实视频文件刮削，文件暂不可见时进入短期重试。\n"
            "7. 电视剧/番剧：定位 STRM 所在剧名根目录。剧名根目录缺少剧级图片/nfo 时刮削整部剧；CD2 剧目录/Season 目录/单集真实视频暂不可见时会先预热路径并短期重试，不会立即判最终失败。\n"
            "8. 剧名根目录已有基础信息时，会先检查当前季信息；具体季号只认可 season02-poster 这类对应季海报，避免通用 season-poster 误判第二季完整。缺季信息会刮削当前季并在 10 分钟后复查；仍缺失时只额外补刮一次，再失败则结束并通知。\n"
            "9. 本次入库单集缺图时，先创建单集刮削任务；真正触发单集刮削前会删除该集 CD2 同名 nfo，然后只刮削该集。单集刮削事件发送成功后，才从成功时间开始计时 10 分钟检查。仍缺图时加入配置天数的长期复查队列；图片提前恢复时只更新预览，任务保留到期后给出正式复查结果。\n"
            "10. 刮削前会按配置间隔刷新/探测一次 CD2 目标路径；同一批电视剧任务和同一刷新间隔内不会重复刷新同一路径。\n"
            "11. 媒体库过滤会同时识别路径第一层和第二层，例如 /media/电视剧/国产剧/... 可命中‘电视剧’或‘国产剧’；如果媒体库名称和实际路径不一致，可在‘媒体库路径映射’里填写：媒体库名称|类型|路径1,路径2，例如 动漫|tv|/media/电视剧/国漫,/media/电视剧/日番。\n"
            "12. 待处理任务可在插件详情页单独立即执行、删除或清空，同一部剧的单集刮削任务会合并展示。检查类任务手动提前检查仍缺图时，会保留原到期时间，不会提前进入10天复查。"
        )

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": False,
            "media_server": self._media_server,
            "include_libraries": self._include_libraries,
            "all_libraries": self._all_libraries,
            "library_path_mapping": self._library_path_mapping,
            "library_mapping_check_once": False,
            "strm_check_root": self._strm_check_root,
            "scrape_target_root": self._scrape_target_root,
            "cron": self._cron,
            "initial_check_delay_seconds": self._initial_check_delay_seconds,
            "episode_scrape_delay_seconds": self._episode_scrape_delay_seconds,
            "scrape_path_refresh_interval_seconds": self._scrape_path_refresh_interval_seconds,
            "tv_recheck_days": self._tv_recheck_days,
            "scrape": True,
            "queue_delete_items": [],
            "queue_delete_confirm": False,
            "queue_clear_all": False
        })
        try:
            self.save_data("library_cache_meta", {"ts": time.time(), "server": self._media_server})
        except Exception:
            pass

    @staticmethod
    def _to_list(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(x).strip() for x in value if str(x or "").strip()]
        if isinstance(value, (tuple, set)):
            return [str(x).strip() for x in value if str(x or "").strip()]
        text = str(value).strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                data = json.loads(text)
                return LocalMetadataCleaner._to_list(data)
            except Exception:
                pass
        return [x.strip() for x in re.split(r"[,\n;]+", text) if x.strip()]

    def _parse_include_libraries(self) -> List[str]:
        return self._to_list(self._include_libraries)

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            if value is None or value == "":
                return float(default)
            result = float(value)
            return result if math.isfinite(result) else float(default)
        except Exception:
            return float(default)

    @staticmethod
    def _ts_to_str(ts: float) -> str:
        try:
            return datetime.fromtimestamp(float(ts), tz=pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(tz=pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

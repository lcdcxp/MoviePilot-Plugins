import json
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
    plugin_version = "2.1"
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
    DEFAULT_TARGET_DEPTH = 3
    DEFAULT_STORAGE = "local"
    HISTORY_LIMIT = 100

    STATUS_LABELS = {
        "waiting_initial_check": "等待入库检查",
        "waiting_movie_retry": "等待电影文件重试",
        "waiting_episode_scrape": "等待单集刮削",
        "waiting_tv_postcheck": "等待刮削后检查",
        "waiting_tv_recheck": "等待10天复查",
    }
    TASK_TYPE_LABELS = {
        "initial": "入库检查",
        "movie_retry": "电影重试",
        "episode_scrape": "单集刮削",
        "tv_postcheck": "刮削后检查",
        "tv_recheck": "10天复查",
    }
    TASK_COLORS = {
        "initial": "primary",
        "movie_retry": "orange",
        "episode_scrape": "purple",
        "tv_postcheck": "info",
        "tv_recheck": "warning",
    }
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
        "episode_scrape_target_missing": "单集刮削文件缺失",
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
    _episode_scrape_delay_minutes: float = 0.5  # 兼容旧配置，内部按秒换算
    _post_scrape_check_delay_minutes: float = DEFAULT_POST_SCRAPE_CHECK_MINUTES
    _tv_recheck_days: float = DEFAULT_TV_RECHECK_DAYS

    # 刮削配置
    _scrape: bool = True
    _storage: str = DEFAULT_STORAGE

    # 队列操作
    _queue_delete_items: List[str] = []
    _queue_delete_confirm: bool = False
    _queue_clear_all: bool = False
    _clear_history: bool = False

    _lock = RLock()
    _timers: List[Timer] = []
    _next_timer_due_ts: float = 0
    _storagechain = None
    mschain = None
    mediaserver_helper = None

    def init_plugin(self, config: dict = None):
        self.stop_service()
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

            self._strm_check_root = str(config.get("strm_check_root") or self.DEFAULT_STRM_CHECK_ROOT).strip().rstrip("/") or self.DEFAULT_STRM_CHECK_ROOT
            self._scrape_target_root = str(config.get("scrape_target_root") or self.DEFAULT_SCRAPE_TARGET_ROOT).strip().rstrip("/") or self.DEFAULT_SCRAPE_TARGET_ROOT
            self._target_depth = self.DEFAULT_TARGET_DEPTH

            self._cron = str(config.get("cron") or self.DEFAULT_CRON).strip() or self.DEFAULT_CRON
            self._initial_check_delay_seconds = int(self._to_float(config.get("initial_check_delay_seconds"), self.DEFAULT_INITIAL_CHECK_DELAY_SECONDS))
            delay_seconds_value = config.get("episode_scrape_delay_seconds")
            if delay_seconds_value is None:
                # 兼容旧版本：旧配置单位是分钟。
                delay_seconds_value = self._to_float(config.get("episode_scrape_delay_minutes"), self.DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS / 60) * 60
            self._episode_scrape_delay_seconds = max(self._to_float(delay_seconds_value, self.DEFAULT_EPISODE_SCRAPE_DELAY_SECONDS), 0)
            self._episode_scrape_delay_minutes = self._episode_scrape_delay_seconds / 60
            self._post_scrape_check_delay_minutes = self.DEFAULT_POST_SCRAPE_CHECK_MINUTES
            self._tv_recheck_days = self._to_float(config.get("tv_recheck_days"), self.DEFAULT_TV_RECHECK_DAYS)
            self._scrape = bool(config.get("scrape", True))
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
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "tv_recheck_days", "label": "复查天数", "placeholder": "10"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "scrape", "label": "触发 MP 刮削"}}]}
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
            "tv_recheck_days": self.DEFAULT_TV_RECHECK_DAYS,
            "scrape": True,
            "queue_delete_items": [],
            "queue_delete_confirm": False,
            "queue_clear_all": False
        }

    def get_page(self) -> List[dict]:
        """插件详情页：参考签到历史页，使用概览卡片 + 紧凑记录列表。"""
        state = self._load_state()
        # 兼容旧队列：如果 10 天复查任务缺少具体缺图集名称，打开详情页时轻量刷新一次缓存。
        if self._refresh_queue_preview_cache_for_display(state):
            self.save_data("state", state)
        queue = state.get("queue") or {}
        history = state.get("history") or []
        queue_count = len(queue)
        history_display = self._history_for_display(history, limit=10)
        failure_display = self._history_failure_for_display(history, limit=5)

        task_stats = self._queue_stats(queue)
        last_record = history[-1] if history else {}

        cards: List[Dict[str, Any]] = []
        cards.append(self._overview_path_row(queue_count, task_stats, last_record))
        cards.append(self._queue_section(queue, queue_count))
        cards.append(self._history_section(history_display, failure_display))

        if not queue and not history_display:
            cards.append({
                "component": "VAlert",
                "props": {"type": "success", "variant": "tonal", "class": "mt-3", "text": "暂无待处理任务和历史记录。"}
            })

        return [{"component": "div", "props": {"class": "pa-2"}, "content": cards}]

    def _overview_path_row(self, queue_count: int, task_stats: Dict[str, int], last_record: Dict[str, Any]) -> Dict[str, Any]:
        """顶部区域：拆成 3 个等宽小卡片，映射检测明细独立成整行，避免左右高度差造成大面积留白。"""
        content: List[Dict[str, Any]] = [
            {"component": "VRow", "props": {"class": "mb-3", "dense": True}, "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [self._overview_card(queue_count, task_stats, last_record)]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [self._recent_card(last_record)]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [self._path_rule_card()]},
            ]}
        ]
        report = self.get_data("library_mapping_check") or {}
        report_lines = self._to_list(report.get("lines") or []) if isinstance(report, dict) else []
        if report_lines:
            content.append(self._mapping_report_card(report, report_lines))
        return {"component": "div", "content": content}

    def _overview_card(self, queue_count: int, task_stats: Dict[str, int], last_record: Dict[str, Any]) -> Dict[str, Any]:
        """运行状态卡片：只放数字，避免和路径卡片高度相差太大。"""
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 rounded-xl h-100"}, "content": [
            {"component": "div", "props": {"class": "d-flex align-center justify-space-between ga-2 mb-2"}, "content": [
                self._card_header("mdi-view-dashboard-outline", "运行概览"),
                self._chip("运行中" if self._enabled else "未启用", "success" if self._enabled else "grey")
            ]},
            {"component": "VDivider", "props": {"class": "my-3"}},
            {"component": "VRow", "props": {"dense": True}, "content": [
                self._summary_metric("待处理", f"{queue_count} 个", "info" if queue_count else "success"),
                self._summary_metric("10分钟检查", f"{task_stats.get('tv_postcheck', 0)} 个", "info" if task_stats.get('tv_postcheck') else "grey"),
                self._summary_metric("10天复查", f"{task_stats.get('tv_recheck', 0)} 个", "warning" if task_stats.get('tv_recheck') else "grey"),
                self._summary_metric("单集刮削", f"{task_stats.get('episode_scrape', 0)} 个", "purple" if task_stats.get('episode_scrape') else "grey"),
            ]}
        ]}

    def _recent_card(self, last_record: Dict[str, Any]) -> Dict[str, Any]:
        """最近处理卡片：把原来顶部蓝色提示独立出来，填平顶部视觉。"""
        if last_record:
            action = self._action_label(str(last_record.get("action") or ""))
            t = str(last_record.get("time") or "")
            scope = self._short_path(str(last_record.get("scope") or last_record.get("folder") or ""))
            desc = scope or "暂无路径"
            color = "error" if last_record.get("scrape") is False else ("warning" if "missing" in str(last_record.get("action") or "") else "info")
        else:
            action = "等待入库事件"
            t = ""
            desc = "插件会监听 MP 全局 Webhook 入库事件。"
            color = "info"
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 rounded-xl h-100"}, "content": [
            self._card_header("mdi-history", "最近处理"),
            {"component": "VDivider", "props": {"class": "my-3"}},
            {"component": "VAlert", "props": {"type": color, "variant": "tonal", "density": "compact", "text": f"{t} · {action}" if t else action}},
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": desc},
        ]}

    def _path_rule_card(self) -> Dict[str, Any]:
        """路径规则卡片：只显示关键结果；详细映射检测放到下方整行卡片。"""
        report = self.get_data("library_mapping_check") or {}
        ok = bool(report.get("ok")) if isinstance(report, dict) and report else None
        title = str(report.get("title") or "未检测媒体库映射") if isinstance(report, dict) else "未检测媒体库映射"
        alert_type = "success" if ok is True else ("warning" if ok is False else "info")
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 rounded-xl h-100"}, "content": [
            self._card_header("mdi-folder-sync-outline", "路径与规则"),
            {"component": "VDivider", "props": {"class": "my-3"}},
            self._mini_line("STRM 根路径", self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT),
            self._mini_line("刮削目标", self._scrape_target_root or self.DEFAULT_SCRAPE_TARGET_ROOT),
            {"component": "VAlert", "props": {"type": alert_type, "variant": "tonal", "density": "compact", "class": "mt-3", "text": title if report else "配置页打开“保存后检测媒体库映射”并保存，可检查映射规则。"}},
        ]}

    def _mapping_report_card(self, report: Dict[str, Any], report_lines: List[str]) -> Dict[str, Any]:
        """媒体库映射检测明细：单独整行展示，避免顶栏左右高度不一致。"""
        ok = bool(report.get("ok"))
        preview_lines = report_lines[:6]
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 mb-3 rounded-xl"}, "content": [
            {"component": "div", "props": {"class": "d-flex align-center justify-space-between ga-2"}, "content": [
                self._card_header("mdi-clipboard-check-outline", "媒体库映射检测"),
                self._chip("通过" if ok else "需检查", "success" if ok else "warning")
            ]},
            {"component": "VDivider", "props": {"class": "my-3"}},
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-2"}, "text": "仅展示前 6 条，完整明细见插件日志。"},
            {"component": "VRow", "props": {"dense": True}, "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [self._mini_line("结果", line)]}
                for line in preview_lines
            ]},
            *([{"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": f"还有 {len(report_lines) - len(preview_lines)} 条检测结果已写入日志。"}] if len(report_lines) > len(preview_lines) else [])
        ]}

    def _queue_section(self, queue: Dict[str, Any], queue_count: int) -> Dict[str, Any]:
        display_items = self._queue_display_items(queue)
        content: List[Dict[str, Any]] = [
            self._section_title("mdi-timer-sand", f"待处理任务（{queue_count} 个）", "按任务类型分组展示；可单独立即执行。检查类任务若提前执行仍缺图，会保留原到期时间，不会提前加入10天复查。")
        ]
        if display_items:
            content.append({"component": "div", "props": {"class": "d-flex justify-end mb-2"}, "content": [
                {"component": "VBtn", "props": {"variant": "tonal", "color": "error", "size": "small", "prepend-icon": "mdi-delete-sweep"}, "text": "清空队列", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_clear", "method": "get", "params": {"apikey": getattr(settings, "API_TOKEN", "")}}}}
            ]})
            grouped = self._queue_grouped_display_items(display_items)
            shown_groups = 0
            shown_items = 0
            for group in grouped:
                items = group.get("items") or []
                if not items:
                    continue
                shown_groups += 1
                group_title = f"{group.get('label')}（{len(items)} 组 / {group.get('task_count', len(items))} 个任务）"
                content.append(self._queue_group_title(str(group.get("icon") or "mdi-chevron-right"), group_title, str(group.get("desc") or "")))
                content.append({"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [self._queue_display_card(display)]}
                    for display in items[:5]
                ]})
                shown_items += min(len(items), 5)
                if len(items) > 5:
                    content.append({"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "density": "compact", "class": "mt-1 mb-2", "text": f"{group.get('label')} 仅展示前 5 组，其余 {len(items) - 5} 组仍会按队列处理。"}})
            if shown_groups == 0:
                content.append({"component": "VAlert", "props": {"type": "success", "variant": "tonal", "density": "compact", "text": "当前没有待处理任务。"}})
            elif len(display_items) > shown_items:
                content.append({"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mt-2", "text": f"当前页面分组展示 {shown_items} 组，实际共有 {len(display_items)} 组、{queue_count} 个任务。"}})
        else:
            content.append({"component": "VAlert", "props": {"type": "success", "variant": "tonal", "density": "compact", "text": "当前没有待处理任务。"}})
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 mb-3 rounded-xl"}, "content": content}

    def _history_section(self, history_display: List[Dict[str, Any]], failure_display: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        failure_display = failure_display or []
        content: List[Dict[str, Any]] = [
            self._section_title("mdi-history", "最近处理记录", "已做展示去重；失败/异常记录会优先单独列出，便于排查。"),
            {"component": "div", "props": {"class": "d-flex justify-end mb-2"}, "content": [
                {"component": "VBtn", "props": {"variant": "tonal", "color": "warning", "size": "small", "prepend-icon": "mdi-broom"}, "text": "清空历史记录", "events": {"click": {"api": "plugin/LocalMetadataCleaner/history_clear", "method": "get", "params": {"apikey": getattr(settings, "API_TOKEN", "")}}}}
            ]}
        ]
        if failure_display:
            content.append(self._queue_group_title("mdi-alert-circle-outline", f"最近失败/异常（{len(failure_display)} 条）", "只展示刮削失败、目标缺失、缺图复查等需要关注的记录。"))
            content.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [self._history_row_card(item)]}
                for item in failure_display
            ]})
        if history_display:
            content.append(self._queue_group_title("mdi-format-list-bulleted", "全部最近记录", "包含成功、跳过、失败和手动操作。"))
            content.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [self._history_row_card(item)]}
                for item in history_display
            ]})
        else:
            content.append({"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "text": "暂无最近处理记录。"}})
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 mb-3 rounded-xl"}, "content": content}

    @staticmethod
    def _card_header(icon: str, title: str) -> Dict[str, Any]:
        return {"component": "div", "props": {"class": "d-flex align-center ga-2 text-h6 font-weight-medium"}, "content": [
            {"component": "VIcon", "props": {"icon": icon, "color": "primary"}},
            {"component": "span", "text": title}
        ]}

    @staticmethod
    def _section_title(icon: str, title: str, subtitle: str = "") -> Dict[str, Any]:
        content: List[Dict[str, Any]] = [
            {"component": "div", "props": {"class": "d-flex align-center ga-2"}, "content": [
                {"component": "VIcon", "props": {"icon": icon, "color": "primary", "size": "small"}},
                {"component": "span", "props": {"class": "text-h6 font-weight-medium"}, "text": title}
            ]}
        ]
        if subtitle:
            content.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": subtitle})
        content.append({"component": "VDivider", "props": {"class": "my-3"}})
        return {"component": "div", "content": content}

    @staticmethod
    def _queue_group_title(icon: str, title: str, subtitle: str = "") -> Dict[str, Any]:
        content: List[Dict[str, Any]] = [
            {"component": "div", "props": {"class": "d-flex align-center ga-2 mt-3 mb-1"}, "content": [
                {"component": "VIcon", "props": {"icon": icon, "color": "primary", "size": "small"}},
                {"component": "span", "props": {"class": "text-subtitle-2 font-weight-medium"}, "text": title}
            ]}
        ]
        if subtitle:
            content.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-2"}, "text": subtitle})
        return {"component": "div", "content": content}

    @staticmethod
    def _summary_metric(label: str, value: str, color: str = "grey") -> Dict[str, Any]:
        return {"component": "VCol", "props": {"cols": 6, "sm": 3}, "content": [
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": label},
            {"component": "VChip", "props": {"color": color, "variant": "tonal", "size": "small", "class": "font-weight-medium"}, "text": value}
        ]}

    @staticmethod
    def _path_metric(label: str, value: str) -> Dict[str, Any]:
        return {"component": "VCol", "props": {"cols": 12, "sm": 6}, "content": [
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": label},
            {"component": "VSheet", "props": {"class": "pa-2 rounded-lg text-caption", "color": "grey-lighten-4"}, "text": value}
        ]}

    def _overview_text(self, last_record: Dict[str, Any]) -> str:
        if not last_record:
            return "等待 MP 全局 Webhook 入库事件。"
        t = str(last_record.get("time") or "")
        action = self._action_label(str(last_record.get("action") or ""))
        scope = self._short_path(str(last_record.get("scope") or last_record.get("folder") or ""))
        return f"最近处理：{t} · {action} · {scope}" if scope else f"最近处理：{t} · {action}"

    @staticmethod
    def _queue_stats(queue: Dict[str, Any]) -> Dict[str, int]:
        stats: Dict[str, int] = {}
        for item in (queue or {}).values():
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
        sorted_items = sorted(queue.items(), key=lambda kv: float((kv[1] or {}).get("due_ts") or 0))
        groups: Dict[str, Dict[str, Any]] = {}
        display: List[Dict[str, Any]] = []
        for key, item in sorted_items:
            if not isinstance(item, dict):
                item = {}
            task_type = str(item.get("task_type") or "")
            if task_type == "episode_scrape":
                show_root = str(item.get("show_root") or "").strip()
                status = str(item.get("status") or "")
                group_key = f"episode_group::{show_root}::{status}"
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
                        "min_due_ts": float(item.get("due_ts") or 0),
                    }
                    groups[group_key] = group
                    display.append(group)
                group["items"].append(item)
                group["keys"].append(str(key))
                if item.get("due_at"):
                    group["due_values"].append(str(item.get("due_at")))
                due_ts = float(item.get("due_ts") or 0)
                if due_ts and (not group.get("min_due_ts") or due_ts < float(group.get("min_due_ts") or 0)):
                    group["min_due_ts"] = due_ts
                continue
            display.append({"display_type": "single", "key": str(key), "item": item, "min_due_ts": float(item.get("due_ts") or 0)})
        return sorted(display, key=lambda x: float(x.get("min_due_ts") or 0))

    def _queue_grouped_display_items(self, display_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        configs = {
            "initial": {"label": "等待入库检查", "icon": "mdi-magnify-scan", "desc": "下一步会检查 STRM 目录刮削信息，缺失时再触发电影/电视剧刮削。"},
            "movie_retry": {"label": "等待电影文件重试", "icon": "mdi-movie-search-outline", "desc": "下一步会继续尝试 CD2 同名真实视频文件；仍不可见则按短期重试间隔继续等待。"},
            "episode_scrape": {"label": "等待刮削任务", "icon": "mdi-play-circle-outline", "desc": "下一步会触发真实媒体文件刮削；成功后再创建10分钟检查。"},
            "tv_postcheck": {"label": "等待10分钟检查", "icon": "mdi-timer-check-outline", "desc": "下一步会确认图片/季信息是否已生成；提前手动检查仍缺图时会保留原到期时间。"},
            "tv_recheck": {"label": "等待10天复查", "icon": "mdi-calendar-clock", "desc": "下一步会再次确认缺图集，仍缺图才删除同名 nfo 并重新排单集刮削。"},
            "other": {"label": "其他任务", "icon": "mdi-dots-horizontal-circle-outline", "desc": "未知或兼容旧版本的任务。"},
        }
        order = ["initial", "movie_retry", "episode_scrape", "tv_postcheck", "tv_recheck", "other"]
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
                return "检查本次入库季/集的刮削信息；缺图时删除同名 nfo，并排单集刮削任务。"
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
            return "重新确认缺图集；到期仍缺图时删除同名 nfo，并重新排单集刮削任务。"
        return "按兼容逻辑处理该任务。"

    def _queue_display_card(self, display: Dict[str, Any]) -> Dict[str, Any]:
        if display.get("display_type") == "episode_group":
            return self._episode_group_card(display)
        return self._task_row_card(str(display.get("key") or ""), display.get("item") or {})

    def _episode_group_card(self, group: Dict[str, Any]) -> Dict[str, Any]:
        items = [x for x in (group.get("items") or []) if isinstance(x, dict)]
        show_root = Path(str(group.get("show_root") or ""))
        show_name = show_root.name or str(show_root) or "同剧单集"
        status = str(group.get("status") or "")
        due_values = sorted(set([str(x) for x in (group.get("due_values") or []) if str(x or "").strip()]))
        due_text = due_values[0] if len(due_values) == 1 else (f"{due_values[0]} 等" if due_values else "")
        title = f"单集刮削：{show_name}（{len(items)} 集）"
        chips = [self._chip("单集刮削", "purple")]
        if status:
            chips.append(self._chip(self._status_label(status), "grey"))
        if due_text:
            chips.append(self._chip(f"到期 {due_text}", "info"))
        if len(due_values) > 1:
            chips.append(self._chip("到期时间不同", "warning"))

        episode_labels = []
        for item in items:
            ep = Path(str(item.get("episode_strm") or ""))
            if ep:
                episode_labels.append(self._episode_label(ep, show_root))
        episode_labels = episode_labels[:12]

        target = str(items[0].get("scrape_target") or "") if items else ""
        detail_lines: List[Dict[str, Any]] = []
        if show_root:
            detail_lines.append(self._mini_line("剧名目录", self._short_path(str(show_root))))
        if target:
            detail_lines.append(self._mini_line("刮削目标示例", self._short_path(target)))
        if episode_labels:
            detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": "待刮削集："})
            detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "purple") for name in episode_labels]})
            if len(items) > len(episode_labels):
                detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": f"仅展示前 {len(episode_labels)} 集，其余 {len(items) - len(episode_labels)} 集仍会按队列处理。"})
        detail_lines.append(self._mini_line("下一步", "触发这些单集的真实媒体文件刮削；每集刮削成功后再各自创建10分钟检查任务。"))
        detail_lines.append(self._mini_line("说明", "页面合并展示；底层仍按单集逐个刮削。"))

        return {"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 rounded-xl"}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2"}, "content": [
                {"component": "div", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-1"}, "content": chips + [
                    {"component": "VBtn", "props": {"variant": "tonal", "color": "success", "size": "x-small", "prepend-icon": "mdi-play-circle-outline", "class": "ml-1"}, "text": "立即执行整组", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_run_episode_group", "method": "get", "params": {"show_root": str(show_root), "status": status, "apikey": getattr(settings, "API_TOKEN", "")}}}},
                    {"component": "VBtn", "props": {"variant": "tonal", "color": "error", "size": "x-small", "prepend-icon": "mdi-delete-outline", "class": "ml-1"}, "text": "删除整组", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_delete_episode_group", "method": "get", "params": {"show_root": str(show_root), "status": status, "apikey": getattr(settings, "API_TOKEN", "")}}}}
                ]}
            ]},
            {"component": "div", "props": {"class": "mt-2"}, "content": detail_lines}
        ]}

    def _task_row_card(self, key: str, item: Dict[str, Any]) -> Dict[str, Any]:
        task_type = str(item.get("task_type") or "")
        title = self._queue_title(key, item)
        status = str(item.get("status") or "")
        due_at = str(item.get("due_at") or "")
        strm = str(item.get("strm_path") or item.get("episode_strm") or item.get("show_root") or "")
        target = str(item.get("scrape_target") or item.get("scrape_dir") or "")
        chips = [self._chip(self._task_type_label(task_type), self._task_color(task_type))]
        if status:
            chips.append(self._chip(self._status_label(status), "grey"))
        if due_at:
            chips.append(self._chip(f"到期 {due_at}", "info"))

        detail_lines: List[Dict[str, Any]] = []
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
                names = [Path(sd).name or str(sd) for sd in season_dirs[:8]]
                detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": "待检查季："})
                detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "info") for name in names]})
            episodes = self._to_list(item.get("episodes") or [])
            if episodes:
                show_root = Path(str(item.get("show_root") or ""))
                names = [self._episode_label(Path(ep), show_root) for ep in episodes[:8]]
                detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-2"}, "text": "待检查集："})
                detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "info") for name in names]})
                if len(episodes) > 8:
                    detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": "仅展示前 8 集。"})

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
                    detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-1"}, "content": [self._chip(str(name), "warning") for name in names[:8]]})
                    if preview.get("truncated") or len(names) > 8:
                        detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": "仅展示前 8 集，到期仍会检查全部 STRM。"})
                elif total:
                    detail_lines.append({"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "density": "compact", "class": "mt-2", "text": "当前任务来自旧缓存，只记录了缺图数量；保存一次配置或等待下次事件后会刷新具体集数。"}})
            else:
                detail_lines.append(self._mini_line("当前缺图", "剧名目录暂不可访问，到期会再次检查"))

        next_step = self._task_next_step(task_type, item)
        if next_step:
            detail_lines.append(self._mini_line("下一步", next_step))
        msg = str(item.get("last_msg") or "")
        if msg:
            detail_lines.append(self._mini_line("说明", msg))

        return {"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 rounded-xl"}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2"}, "content": [
                {"component": "div", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-1"}, "content": chips + [
                    {"component": "VBtn", "props": {"variant": "tonal", "color": "success" if task_type not in ("tv_postcheck", "tv_recheck") else "info", "size": "x-small", "prepend-icon": "mdi-play-circle-outline", "class": "ml-1"}, "text": "立即检查" if task_type in ("tv_postcheck", "tv_recheck") else "立即执行", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_run", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}},
                    {"component": "VBtn", "props": {"variant": "tonal", "color": "error", "size": "x-small", "prepend-icon": "mdi-delete-outline", "class": "ml-1"}, "text": "删除任务", "events": {"click": {"api": "plugin/LocalMetadataCleaner/queue_delete", "method": "get", "params": {"key": key, "apikey": getattr(settings, "API_TOKEN", "")}}}}
                ]}
            ]},
            {"component": "div", "props": {"class": "mt-2"}, "content": detail_lines}
        ]}

    def _history_row_card(self, item: Dict[str, Any]) -> Dict[str, Any]:
        title = self._short_path(str(item.get("scope") or item.get("folder") or "处理记录"))
        action = self._action_label(str(item.get("action") or ""))
        t = str(item.get("time") or "")
        scrape = item.get("scrape")
        if scrape is True:
            result_chip = self._chip("刮削已触发", "success")
        elif scrape is False:
            result_chip = self._chip("刮削失败", "error")
        else:
            result_chip = self._chip("记录", "grey")

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

        return {"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 rounded-xl"}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2"}, "content": [
                {"component": "div", "content": [
                    {"component": "div", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                    {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": f"{t} · {action}"}
                ]},
                {"component": "div", "content": [result_chip]}
            ]},
            {"component": "div", "props": {"class": "mt-2"}, "content": detail_lines}
        ]}

    @staticmethod
    def _chip(text: str, color: str = "grey") -> Dict[str, Any]:
        return {"component": "VChip", "props": {"color": color, "variant": "tonal", "size": "small", "class": "mr-1 mb-1"}, "text": text}

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
            ("scraped", "刮削集", "purple"),
        ]
        for prefix, label, color in groups:
            labels = self._history_episode_labels(item, prefix, limit=12)
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
        inferred = self._infer_history_checked_episode_labels(item, limit=12)
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

    def _episode_history_payload(self, show_root: Path, episodes: Any, prefix: str = "checked", limit: int = 20) -> Dict[str, Any]:
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

    def _task_type_label(self, task_type: str) -> str:
        return self.TASK_TYPE_LABELS.get(task_type, task_type or "任务")

    def _task_color(self, task_type: str) -> str:
        return self.TASK_COLORS.get(task_type, "grey")

    def _action_label(self, action: str) -> str:
        return self.ACTION_LABELS.get(action, action or "记录")

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

    def _process_queue_task(self, key: str, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        task_type = str(task.get("task_type") or "initial")
        if task_type == "initial":
            return self._process_initial_task(task, state)
        if task_type == "movie_retry":
            return self._process_movie_retry_task(task, state, manual=manual)
        if task_type == "episode_scrape":
            return self._process_episode_scrape_task(task, state)
        if task_type == "tv_postcheck":
            return self._process_tv_postcheck_task(task, state, manual=manual)
        if task_type == "tv_recheck":
            return self._process_tv_recheck_task(task, state, manual=manual)
        return {"success": True, "action": "drop_unknown_task", "scope": key, "folder": key, "message": f"未知任务类型：{task_type}"}

    def run_once(self):
        if not self._enabled:
            return
        with self._lock:
            try:
                state = self._load_state()
                queue = state.setdefault("queue", {})
                now_ts = time.time()
                if getattr(self, "_next_timer_due_ts", 0) and float(self._next_timer_due_ts) <= now_ts:
                    self._next_timer_due_ts = 0
                done = 0
                changed = False
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
                    due_ts = float(task.get("due_ts") or 0)
                    if due_ts and now_ts < due_ts:
                        continue

                    result = self._process_queue_task(key, task, state, manual=False)
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

    def _process_initial_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        raw_path = str(task.get("raw_path") or "")
        media_type = str(task.get("media_type") or "")
        if media_type == "movie":
            movie_dir = Path(str(task.get("movie_dir") or task.get("check_dir") or ""))
            movie_strm = Path(str(task.get("strm_path") or raw_path or ""))
            scrape_dir = Path(str(task.get("scrape_dir") or self._map_strm_path_to_scrape_path(str(movie_dir))))
            status = self._movie_metadata_status(movie_dir)
            if status.get("complete"):
                result = {
                    "time": self._now_iso(), "action": "movie_metadata_complete_skip", "scope": str(movie_dir), "folder": str(scrape_dir),
                    "scrape": None, "scrape_msg": "电影刮削信息完整，跳过刮削", "metadata": status
                }
                self._append_history(state, result)
                return {"remove": True, **result}

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
            old_due = float(existing.get("due_ts") or 0)
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
                "first_seen_ts": float(source_task.get("first_seen_ts") or now_ts),
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

    def _process_movie_retry_task(self, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        movie_dir = Path(str(task.get("movie_dir") or task.get("check_dir") or ""))
        movie_strm = Path(str(task.get("movie_strm") or task.get("strm_path") or task.get("raw_path") or ""))
        scrape_dir = Path(str(task.get("scrape_dir") or self._map_strm_path_to_scrape_path(str(movie_dir))))
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
        if self._notify:
            self._send_notify(result)
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
        task["resolved_scan_scope"] = scan_scope_type
        task["resolved_scan_path"] = str(scan_scope_path or "")

        now_ts = time.time()
        postcheck_batch_id = self._make_task_batch_id("postcheck")
        delay_seconds = max(self._post_scrape_check_delay_minutes, 0) * 60
        postcheck_due_ts = now_ts + delay_seconds
        sent_targets = set()
        episode_target_map: Dict[str, str] = {}
        # 剧/季目录刮削在 MP 原生流程里可能会同时处理目录下文件。
        # 为避免同一集在“目录刮削”和“单集刮削任务”里重复触发，
        # 被成功目录刮削覆盖的缺图集先等待 10 分钟检查；仍缺图时再创建单集刮削任务。
        metadata_scrape_episodes: List[Path] = []
        root_directory_scrape_active = False

        def target_key(value: str) -> str:
            return str(Path(str(value or ""))) if value else ""

        for episode in episode_paths:
            try:
                episode_target_map[str(episode)] = self._map_episode_strm_to_scrape_target(episode) or ""
            except Exception as err:
                logger.debug(f"监控strm刮削网盘：查找单集真实媒体失败：{episode} - {err}")
                episode_target_map[str(episode)] = ""

        # 先检查本次快照内的每一集是否已有图片。
        # 注意：这里不立即删除单集 nfo。若剧/季目录刮削会覆盖该集，先等待目录刮削后的10分钟检查；
        # 只有真正进入单集补刮削时，才删除该集对应 nfo，避免目录刮削和单集刮削造成重复删除/重复刮削。
        missing_eps: List[Path] = []
        existing_count = 0
        deleted_total = 0
        deleted_map: Dict[str, int] = {}
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
            recent_marker = isinstance(marker, dict) and now_ts - float(marker.get("ts") or 0) < 600
            ok = False
            msg = ""
            scrape_target = ""
            if recent_marker:
                scrape_target = str(marker.get("target") or "")
                if scrape_target:
                    sent_targets.add(target_key(scrape_target))
                marker_ts = float(marker.get("ts") or now_ts)
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
                ok, msg, scrape_target, used_file_fallback = self._trigger_tv_metadata_scrape(show_root, episode_paths, purpose="剧信息")
                if ok and isinstance(markers, dict):
                    markers[marker_key] = {"ts": now_ts, "time": self._now_iso(), "show_root": str(show_root), "target": str(scrape_target), "used_file_fallback": False}
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
                self._append_history(state, result)
                if self._notify and not ok:
                    self._send_notify(result)

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
            recent_marker = isinstance(marker, dict) and now_ts - float(marker.get("ts") or 0) < 600
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
                marker_ts = float(marker.get("ts") or now_ts)
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

            ok, msg, scrape_target, used_file_fallback = self._trigger_tv_metadata_scrape(season_dir, season_episodes, purpose=f"季信息 {season_dir.name}")

            if ok and isinstance(markers, dict):
                markers[marker_key] = {"ts": now_ts, "time": self._now_iso(), "season_dir": str(season_dir), "target": str(scrape_target), "used_file_fallback": False}
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
            self._append_history(state, season_result)
            if self._notify and not ok:
                self._send_notify(season_result)
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
            target = episode_target_map.get(str(episode_strm), "")
            key = target_key(target)
            if str(episode_strm) in metadata_wait_set:
                metadata_wait_eps.append(episode_strm)
                continue
            if key and key in sent_targets:
                shared_trigger_eps.append(episode_strm)
                continue
            deleted = 0
            if not target:
                target_missing_eps.append(episode_strm)
            else:
                deleted = self._delete_episode_nfo(episode_strm)
                deleted_map[str(episode_strm)] = int(deleted or 0)
                deleted_total += int(deleted or 0)
            self._ensure_episode_scrape_task(
                state,
                episode_strm,
                show_root,
                reason="initial_missing_image",
                deleted_nfo=deleted,
                postcheck_batch_id=postcheck_batch_id,
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
                "scrape_msg": f"本次按{self._tv_scan_scope_label(scan_scope_type)}检查 {len(episode_paths)} 集，其中 {len(missing_eps)} 集缺少对应图片；已为需要立即单集补刮削的集删除同名 nfo {deleted_total} 个。目录刮削覆盖的集先等待10分钟检查，仍缺图才触发单集刮削；单集刮削成功后再计时10分钟检查，仍缺图才加入10天复查。{extra}",
                "note": names,
                "missing_count": len(missing_eps),
                "existing_count": existing_count,
                "deleted_nfo": deleted_total,
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

        # 兼容旧队列：历史版本可能把 scrape_target 保存成 .strm 路径。
        # 单集刮削必须使用 CD2 中的真实视频文件，不能刮削 .strm，也不能退回 Season 目录。
        stored_target = str(task.get("scrape_target") or "").strip()
        target = ""
        if stored_target:
            stored_path = Path(stored_target)
            if stored_path.suffix.lower() in self._media_suffixes() and stored_path.exists() and stored_path.is_file():
                target = stored_target
            elif stored_path.suffix.lower() == ".strm":
                logger.debug(f"监控strm刮削网盘：忽略旧队列中的 STRM 刮削目标，重新查找同名真实媒体：{stored_target}")

        if not target:
            target = self._map_episode_strm_to_scrape_target(episode_strm)

        if not target:
            mapped = Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
            candidates = self._episode_media_candidate_paths(episode_strm)
            result = {
                "time": self._now_iso(), "action": "episode_scrape_target_missing", "scope": str(episode_strm), "folder": str(mapped.parent if mapped.suffix.lower() == ".strm" else mapped),
                "scrape": False,
                "scrape_msg": "未在 MP 刮削目标目录中找到单集真实媒体文件，已跳过；不会退回刮削 Season 目录。",
                "deleted_nfo": int(task.get("deleted_nfo") or 0),
                "note": "插件不会刮削 .strm 文件；已按同名视频文件规则查找 mkv/mp4/ts/iso 等格式。" + (f" 已尝试：{', '.join(candidates[:5])}" if candidates else " 未生成候选路径。")
            }
            self._append_history(state, result)
            if self._notify:
                self._send_notify(result)
            return {"remove": True, **result}
        ok, msg = self._trigger_scrape(Path(str(target)))
        result = {
            "time": self._now_iso(), "action": "episode_delayed_scrape", "scope": str(episode_strm), "folder": str(target),
            "scrape": ok, "scrape_msg": msg, "deleted_nfo": int(task.get("deleted_nfo") or 0),
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
        self._append_history(state, result)
        if self._notify and not ok:
            self._send_notify(result)
        return {"remove": True, **result}

    def _process_tv_postcheck_task(self, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        show_root = Path(str(task.get("show_root") or ""))
        mode = str(task.get("mode") or "episodes")
        batch_id = str(task.get("batch_id") or "")
        if not show_root.exists() or not show_root.is_dir():
            result = {"time": self._now_iso(), "action": "tv_postcheck_show_missing", "scope": str(show_root), "folder": str(show_root), "scrape": None, "scrape_msg": ("手动检查时剧名目录不存在，保留原任务等待到期检查" if manual else "刮削后检查时剧名目录不存在，任务结束")}
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": not manual, **result}

        if mode == "root":
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
                result = {
                    "time": self._now_iso(), "action": "tv_season_postcheck_manual_incomplete_keep_waiting" if manual else "tv_season_postcheck_incomplete", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": (f"手动检查了 {checked_count} 个季目录，仍有 {len(incomplete)} 个季信息不完整；保留原任务到期后再检查。" if manual else f"季信息刮削后检查了 {checked_count} 个季目录，仍有 {len(incomplete)} 个季信息不完整；不会加入 10 天单集复查队列。"),
                    "missing_count": len(incomplete), "note": names
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

        # 先按当前 STRM 状态清理旧 10 天复查，避免页面继续显示已经有图的旧缓存。
        cleaned_count = self._sync_tv_recheck_tasks(state, show_root)

        if missing:
            names = "、".join([self._episode_label(ep, show_root) for ep in missing[:8]])
            if manual:
                task["missing_preview"] = self._preview_from_episodes(show_root, missing, limit=8)
                result = {
                    "time": self._now_iso(), "action": "tv_postcheck_manual_missing_keep_waiting", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": f"手动检查了 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片；保留原任务到期后再检查，不提前加入 10 天复查队列。" + (f" 同时清理旧复查中已完成 {cleaned_count} 集。" if cleaned_count else ""),
                    "missing_count": len(missing), "note": names,
                    **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                    **self._episode_history_payload(show_root, missing, prefix="missing"),
                }
                task["last_msg"] = result["scrape_msg"]
                self._append_history(state, result)
                return {"remove": False, **result}
            if str(task.get("on_missing") or "") == "episode_scrape":
                total_deleted = 0
                next_batch_id = self._make_task_batch_id("postdir")
                for ep in missing:
                    deleted = self._delete_episode_nfo(ep)
                    total_deleted += int(deleted or 0)
                    self._ensure_episode_scrape_task(
                        state,
                        ep,
                        show_root,
                        reason="metadata_directory_scrape_still_missing_image",
                        deleted_nfo=deleted,
                        postcheck_batch_id=next_batch_id,
                    )
                result = {
                    "time": self._now_iso(), "action": "tv_postcheck_missing_schedule_episode_scrape", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": f"目录刮削后检查了 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片，已删除同名 nfo {total_deleted} 个，并创建单集刮削任务；不会直接加入 10 天复查。" + (f" 同时清理旧复查中已完成 {cleaned_count} 集。" if cleaned_count else ""),
                    "missing_count": len(missing), "deleted_nfo": total_deleted, "note": names,
                    **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                    **self._episode_history_payload(show_root, missing, prefix="missing"),
                }
                self._append_history(state, result)
                return {"remove": True, **result}

            self._ensure_tv_recheck_task(
                state,
                show_root,
                episodes=missing,
                reason=str(task.get("reason") or "post_scrape_missing_image"),
                batch_id=batch_id,
            )
            result = {
                "time": self._now_iso(), "action": "tv_postcheck_missing_schedule_recheck", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                "scrape_msg": f"刮削后检查了 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片，已加入 {self._tv_recheck_days:g} 天复查队列。" + (f" 同时清理旧复查中已完成 {cleaned_count} 集。" if cleaned_count else ""),
                "missing_count": len(missing), "note": names,
                **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
                **self._episode_history_payload(show_root, missing, prefix="missing"),
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        # 本批全部已有图时，也要清理历史 10 天复查任务。
        cleaned_count += self._sync_tv_recheck_tasks(state, show_root)
        result = {
            "time": self._now_iso(), "action": "tv_postcheck_complete", "scope": str(show_root),
            "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
            "scrape_msg": f"刮削后检查完成，检查 {checked_count} 集，均已有对应图片；不加入 10 天复查队列。" + (f" 已同步清理旧复查中已完成 {cleaned_count} 集。" if cleaned_count else ""),
            **self._episode_history_payload(show_root, checked_episodes, prefix="checked"),
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    def _process_tv_recheck_task(self, task: Dict[str, Any], state: Dict[str, Any], manual: bool = False) -> Dict[str, Any]:
        show_root = Path(str(task.get("show_root") or ""))
        if not show_root.exists() or not show_root.is_dir():
            result = {"time": self._now_iso(), "action": "tv_recheck_show_missing", "scope": str(show_root), "folder": str(show_root), "scrape": None, "scrape_msg": ("手动10天复查时剧名目录不存在，保留原任务等待到期复查" if manual else "10天复查时剧名目录不存在，任务结束")}
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": not manual, **result}
        episode_values = task.get("episodes") or []
        if episode_values:
            candidates = self._unique_episode_paths(episode_values)
            missing = [ep for ep in candidates if not self._episode_image_status(ep).get("has_image")]
            checked_count = len(candidates)
        else:
            # 兼容旧队列：老任务没有 episodes 时才全剧扫描。
            missing = self._find_missing_episode_images(show_root)
            checked_count = self._count_strm_files(show_root)
        checked_payload = candidates if episode_values else self._list_strm_files(show_root)
        if not missing:
            result = {"time": self._now_iso(), "action": "tv_recheck_complete", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None, "scrape_msg": f"10天复查完成，检查 {checked_count} 集，均已有对应图片", **self._episode_history_payload(show_root, checked_payload, prefix="checked")}
            self._append_history(state, result)
            return {"remove": True, **result}
        if manual:
            task["missing_preview"] = self._preview_from_episodes(show_root, missing, limit=8)
            result = {
                "time": self._now_iso(), "action": "tv_recheck_manual_missing_keep_waiting", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)),
                "scrape": None, "scrape_msg": f"手动10天复查检查 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片；保留原任务，不提前删除 nfo 或触发刮削。",
                "missing_count": len(missing),
                "note": "、".join([self._episode_label(ep, show_root) for ep in missing[:8]]),
                **self._episode_history_payload(show_root, missing, prefix="missing"),
            }
            task["last_msg"] = result["scrape_msg"]
            self._append_history(state, result)
            return {"remove": False, **result}
        total_deleted = 0
        postcheck_batch_id = self._make_task_batch_id("recheck")
        for episode in missing:
            deleted = self._delete_episode_nfo(episode)
            total_deleted += deleted
            self._ensure_episode_scrape_task(
                state,
                episode,
                show_root,
                reason="tv_10day_recheck_missing_image",
                deleted_nfo=deleted,
                postcheck_batch_id=postcheck_batch_id,
            )
        result = {
            "time": self._now_iso(), "action": "tv_recheck_missing_episodes_schedule_scrape", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)),
            "scrape": None, "scrape_msg": f"10天复查检查 {checked_count} 集，仍有 {len(missing)} 集缺少对应图片，已删除同名 nfo {total_deleted} 个，{self._episode_scrape_delay_seconds:g} 秒后逐集刮削。",
            "missing_count": len(missing), "deleted_nfo": total_deleted,
            "note": "、".join([self._episode_label(ep, show_root) for ep in missing[:8]]),
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

    def _ensure_episode_scrape_task(self, state: Dict[str, Any], episode_strm: Path, show_root: Path, reason: str = "", deleted_nfo: int = 0, postcheck_batch_id: str = ""):
        queue = state.setdefault("queue", {})
        key = f"episode::{episode_strm}"
        due_ts = time.time() + max(self._episode_scrape_delay_seconds, 0)
        target = self._map_episode_strm_to_scrape_target(episode_strm)
        existing = queue.get(key)
        if existing:
            # 如果旧任务里残留 .strm 目标，或者当前已能找到真实媒体文件，刷新成真实媒体路径。
            old_target = str(existing.get("scrape_target") or "")
            if target and (not old_target or Path(old_target).suffix.lower() == ".strm" or not Path(old_target).exists()):
                existing["scrape_target"] = str(target)
            # 单集刮削任务本身可以取更早时间执行；真正的 10 分钟检查会在刮削事件发送成功后再创建。
            existing["due_ts"] = min(float(existing.get("due_ts") or due_ts), due_ts)
            existing["due_at"] = self._ts_to_str(float(existing.get("due_ts") or due_ts))
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["deleted_nfo"] = int(existing.get("deleted_nfo") or 0) + int(deleted_nfo or 0)
            if postcheck_batch_id and not existing.get("postcheck_batch_id"):
                existing["postcheck_batch_id"] = str(postcheck_batch_id)
            existing["last_msg"] = f"重复单集刮削任务已合并，等待：{existing['due_at']}；刮削成功后再计时 10 分钟检查。"
            self._schedule_delayed_check_until(float(existing.get("due_ts") or due_ts))
            return
        queue[key] = {
            "task_type": "episode_scrape",
            "key": key,
            "episode_strm": str(episode_strm),
            "show_root": str(show_root),
            "scrape_target": str(target),
            "reason": reason,
            "deleted_nfo": int(deleted_nfo or 0),
            "postcheck_batch_id": str(postcheck_batch_id or self._make_task_batch_id("ep")),
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_episode_scrape",
            "last_msg": f"等待 {self._episode_scrape_delay_seconds:g} 秒后刮削单集；刮削成功后再计时 10 分钟检查"
        }
        self._schedule_delayed_check(max(self._episode_scrape_delay_seconds, 0))

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
        batch_id = str(batch_id or "").strip()
        if mode == "episodes" and not batch_id:
            batch_id = self._make_task_batch_id("postcheck")
        if mode == "season" and not batch_id:
            batch_id = self._make_task_batch_id("seasoncheck")

        if due_ts is None:
            due_ts = time.time() + max(self._post_scrape_check_delay_minutes, 0) * 60
        due_ts = float(due_ts or 0)
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

            old_due = float(existing.get("due_ts") or 0)
            # 同一批次取更晚时间，避免最后加入的集数还没等够 10 分钟就被检查。
            existing["due_ts"] = max(old_due or due_ts, due_ts)
            existing["due_at"] = self._ts_to_str(float(existing.get("due_ts") or due_ts))
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["reason"] = reason or existing.get("reason") or ""
            if on_missing:
                existing["on_missing"] = str(on_missing)
            if batch_id:
                existing["batch_id"] = batch_id
            existing["last_msg"] = f"刮削后检查任务已合并，检查时间：{existing.get('due_at')}"
            self._schedule_delayed_check_until(float(existing.get("due_ts") or due_ts))
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
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_tv_postcheck",
            "last_msg": "等待刮削后检查 STRM 图片/季信息是否生成"
        }
        self._schedule_delayed_check_until(due_ts)

    def _ensure_tv_recheck_task(self, state: Dict[str, Any], show_root: Path, episodes: List[Path] = None, reason: str = "", batch_id: str = ""):
        """只把 10 分钟检查后仍缺图的单集加入 10 天复查。"""
        queue = state.setdefault("queue", {})
        batch_id = str(batch_id or "").strip()
        key = f"tv_recheck::{show_root}::{batch_id}" if batch_id else f"tv_recheck::{show_root}"
        due_ts = time.time() + max(self._tv_recheck_days, 0) * 86400
        existing = queue.get(key)

        # 再次过滤，避免已经生成图片的集数被加入 10 天复查。
        episode_strings = []
        for ep in self._unique_episode_paths(episodes or []):
            if not self._episode_image_status(ep).get("has_image"):
                text = str(ep)
                if text not in episode_strings:
                    episode_strings.append(text)
        if not episode_strings:
            self._sync_tv_recheck_tasks(state, show_root)
            return

        if existing:
            old_due = float(existing.get("due_ts") or 0)
            # 同一批 10 天复查以首次确认缺图时间为准，不因重复检查反复后延。
            if old_due and old_due < due_ts:
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
            if batch_id:
                existing["batch_id"] = batch_id
            self._sync_tv_recheck_tasks(state, show_root, task_keys=[key])
            if key in queue:
                queue[key]["last_msg"] = f"10天复查任务已合并，复查时间：{queue[key].get('due_at')}"
            return

        queue[key] = {
            "task_type": "tv_recheck",
            "key": key,
            "show_root": str(show_root),
            "episodes": episode_strings,
            "batch_id": batch_id,
            "scrape_target": self._map_strm_path_to_scrape_path(str(show_root)),
            "reason": reason,
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_tv_recheck",
            "missing_preview": self._preview_from_episodes(show_root, episode_strings, limit=8),
            "last_msg": f"10 分钟检查后仍缺图，等待 {self._tv_recheck_days:g} 天后复查"
        }

    def _schedule_delayed_check(self, delay_seconds: float):
        """按相对秒数安排一次短期队列检查。"""
        self._schedule_delayed_check_until(time.time() + max(float(delay_seconds or 0), 0))

    def _schedule_delayed_check_until(self, due_ts: float):
        """安排最近到期的短期任务。

        只用于 10 秒入库检查、30 秒单集刮削、10 分钟刮削后检查；
        10 天复查按用户要求交给兜底检查周期处理，避免创建长期 Timer。
        """
        try:
            due_ts = float(due_ts or 0)
            if not due_ts:
                return
            now_ts = time.time()
            delay = max(due_ts - now_ts, 0.5)

            # 清理已经结束的 Timer，避免长期运行后列表增大。
            self._timers = [t for t in list(getattr(self, "_timers", []) or []) if t.is_alive()]

            current_due = float(getattr(self, "_next_timer_due_ts", 0) or 0)
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
                if task_type not in {"initial", "movie_retry", "episode_scrape", "tv_postcheck"}:
                    continue
                due_ts = float(task.get("due_ts") or 0)
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
        root = self._strm_check_root.rstrip("/") or "/media"
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
            "relative_parts": self._relative_parts_under_root(path_text, self._strm_check_root.rstrip("/") or "/media"),
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
        return {"complete": not missing, "missing": missing, "nfo_count": nfo_count, "images": image_map, "files": sorted(list(files))[:50]}

    def _tv_root_metadata_status(self, show_root: Path) -> Dict[str, Any]:
        image_count = 0
        nfo_count = 0
        names: List[str] = []
        try:
            if show_root.exists() and show_root.is_dir():
                for item in show_root.iterdir():
                    if not item.is_file():
                        continue
                    suffix = item.suffix.lower()
                    if suffix in self._image_suffixes():
                        image_count += 1
                        names.append(item.name)
                    elif suffix == ".nfo":
                        nfo_count += 1
                        names.append(item.name)
        except Exception as err:
            return {"has_any_metadata": False, "error": str(err), "image_count": image_count, "nfo_count": nfo_count}
        return {"has_any_metadata": (image_count + nfo_count) > 0, "image_count": image_count, "nfo_count": nfo_count, "names": names[:50]}

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

    def _delete_episode_nfo(self, episode_strm: Path) -> int:
        deleted = 0
        for nfo in [episode_strm.with_suffix(".nfo")]:
            try:
                if nfo.exists() and nfo.is_file():
                    nfo.unlink()
                    deleted += 1
                    logger.info(f"监控strm刮削网盘：已删除单集 nfo：{nfo}")
            except Exception as err:
                logger.warning(f"监控strm刮削网盘：删除单集 nfo 失败：{nfo} - {err}")
        return deleted

    def _find_missing_episode_images(self, show_root: Path) -> List[Path]:
        missing: List[Path] = []
        try:
            for root, dirs, files in os.walk(show_root, followlinks=False):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]
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
        """刷新/清理指定剧的 10 天复查任务。

        已经生成图片的单集会从 10 天复查队列移除；如果某个复查任务已没有缺图集，直接删除。
        返回被移除的已完成单集数量。
        """
        queue = state.setdefault("queue", {})
        if not isinstance(queue, dict) or not queue:
            return 0
        show_text = str(show_root)
        selected = set(task_keys or [])
        removed_done = 0
        delete_keys: List[str] = []
        for key, item in list(queue.items()):
            if selected and key not in selected:
                continue
            if not isinstance(item, dict) or str(item.get("task_type") or "") != "tv_recheck":
                continue
            if str(item.get("show_root") or "") != show_text:
                continue

            episode_values = item.get("episodes") or []
            if episode_values:
                candidates = self._unique_episode_paths(episode_values)
                still_missing: List[Path] = []
                for ep in candidates:
                    if self._episode_image_status(ep).get("has_image"):
                        removed_done += 1
                    else:
                        still_missing.append(ep)
            else:
                # 兼容旧队列：没有记录具体集数时才扫描整剧。
                still_missing = self._find_missing_episode_images(show_root)

            if not still_missing:
                delete_keys.append(key)
                continue
            item["episodes"] = [str(ep) for ep in still_missing]
            item["missing_preview"] = self._preview_from_episodes(show_root, still_missing, limit=8)
            item["last_msg"] = f"等待 {self._tv_recheck_days:g} 天后复查缺图单集；页面已按当前 STRM 状态刷新。"

        for key in delete_keys:
            queue.pop(key, None)
        return removed_done

    def _refresh_queue_preview_cache_for_display(self, state: Dict[str, Any]) -> bool:
        """打开详情页时按当前 STRM 状态刷新 10 天复查任务。

        旧版本只刷新缓存名称，容易出现“图片已经生成但页面仍显示缺图”。
        这里会重新检查任务内记录的单集，已经有图的自动移出 10 天复查；全部完成则删除该复查任务。
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
                dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]
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

    def _map_strm_file_to_media_target(self, strm_path: Path) -> str:
        """把 STRM 文件映射到 CD2 真实媒体文件。

        只按同名真实视频匹配，不读取 STRM 内容里的 HTTP 短链，避免短链名与网盘实际文件名不一致。
        """
        mapped = Path(self._map_strm_path_to_scrape_path(str(strm_path)))
        if mapped.suffix.lower() != ".strm":
            return str(mapped) if mapped.exists() and mapped.is_file() else ""

        parent = mapped.parent
        stem = mapped.stem
        try:
            if not parent.exists() or not parent.is_dir():
                return ""

            for suffix in self._media_suffixes():
                candidate = parent / f"{stem}{suffix}"
                if candidate.exists() and candidate.is_file():
                    return str(candidate)

            for item in parent.iterdir():
                if item.is_file() and item.stem == stem and item.suffix.lower() in self._media_suffixes():
                    return str(item)
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：查找网盘真实媒体失败：{mapped} - {err}")
        return ""

    def _map_episode_strm_to_scrape_target(self, episode_strm: Path) -> str:
        # STRM 库是 .strm，MP 实际刮削必须使用 CD2 中的真实媒体文件。
        # 找不到时返回空字符串，由任务处理阶段记录失败并通知，不再退回刮削 Season 目录。
        return self._map_strm_file_to_media_target(episode_strm)

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
            if not self._path_same_or_under(text, scrape_root):
                return False
            if strm_root and self._path_same_or_under(text, strm_root):
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
            if self._storagechain is None:
                self._storagechain = StorageChain()
            fileitem = self._storagechain.get_file_item(storage=self._storage, path=target)
            if not fileitem:
                return False, f"无法获取电影真实文件项，等待 CD2 刷新：{target}"
            eventmanager.send_event(EventType.MetadataScrape, {
                "fileitem": fileitem,
                "file_list": [str(target)],
                "meta": None,
                "mediainfo": None
            })
            msg = f"已发送 MP 电影刮削事件，文件：{target}"
            logger.info(f"监控strm刮削网盘：{msg}")
            return True, msg
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：电影真实文件刮削候选暂不可用：{target} - {err}")
            return False, f"电影真实文件候选暂不可用：{target} - {err}"


    def _map_movie_strm_to_scrape_target(self, movie_strm: Path, movie_dir: Path) -> str:
        # 兼容旧调用：电影只返回 STRM 同名 CD2 真实视频文件。
        # 主流程已改为直接逐个请求 MP fileitem；这里不再扫描目录选最大文件，也不返回 STRM 路径。
        for candidate in self._movie_same_name_candidate_paths(movie_strm, movie_dir):
            try:
                path = Path(candidate)
                if path.exists() and path.is_file():
                    return str(path)
            except Exception:
                continue
        return ""


    def _trigger_tv_metadata_scrape(self, scope_dir: Path, episodes: List[Path] = None, purpose: str = "剧/季信息", skip_file_targets: set = None) -> Tuple[bool, str, str, bool]:
        """触发电视剧剧信息/季信息刮削。

        返回：(是否成功, 消息, 实际目标, 是否使用文件兜底)。
        规则：按 MP 原生模式处理，剧信息/季信息只传 CD2 目录，不使用真实单集文件兜底；第四项固定为 False。
        """
        mapped_dir = Path(self._map_strm_path_to_scrape_path(str(scope_dir)))
        if mapped_dir.exists() and mapped_dir.is_dir():
            ok, msg = self._trigger_scrape(mapped_dir)
            if ok:
                return True, msg, str(mapped_dir), False
            return False, f"{purpose}目录刮削失败：{msg}", str(mapped_dir), False
        return False, f"{purpose}目录不存在或不可访问：{mapped_dir}", str(mapped_dir), False

    def _episode_media_candidate_paths(self, episode_strm: Path) -> List[str]:
        """返回单集 STRM 映射到 CD2 后会尝试查找的同名视频路径。"""
        mapped = Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
        if mapped.suffix.lower() != ".strm":
            return [str(mapped)]
        return [str(mapped.with_suffix(suffix)) for suffix in self._media_suffixes()]

    def _movie_media_candidate_paths(self, movie_strm: Path, movie_dir: Path) -> List[str]:
        """返回电影补刮削会尝试的 CD2 同名真实视频候选路径，用于失败说明。"""
        return self._movie_same_name_candidate_paths(movie_strm, movie_dir)


    # --------------------------- 刮削 ---------------------------

    def _trigger_scrape(self, target: Path) -> Tuple[bool, str]:
        if not self._scrape:
            return False, "未启用刮削"
        if StorageChain is None:
            return False, "当前 MP 版本无法导入 StorageChain"
        try:
            if self._storagechain is None:
                self._storagechain = StorageChain()
            # 当前 MP 本地存储 get_item 需要 pathlib.Path，传 str 会触发：'str' object has no attribute 'exists'。
            # 因此这里统一使用 Path 对象，不再用字符串兜底。
            target = Path(target)
            fileitem = self._storagechain.get_file_item(storage=self._storage, path=target)
            if not fileitem:
                return False, f"无法获取文件项，请确认路径在 MP 容器内可见：{target}"
            file_list = self._file_list_for_scrape(target)
            eventmanager.send_event(EventType.MetadataScrape, {
                "fileitem": fileitem,
                "file_list": file_list,
                "meta": None,
                "mediainfo": None
            })
            scope = "文件" if target.is_file() else "目录"
            msg = f"已发送 MP 刮削事件，{scope}：{target}，媒体文件 {len(file_list)} 个"
            logger.info(f"监控strm刮削网盘：{msg}")
            return True, msg
        except Exception as err:
            logger.error(f"监控strm刮削网盘：触发刮削失败：{target} - {err}\n{traceback.format_exc()}")
            return False, str(err)

    def _file_list_for_scrape(self, target: Path) -> List[str]:
        if target.is_file():
            return [str(target)]
        files: List[str] = []
        try:
            for root, dirs, names in os.walk(target, followlinks=False):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]
                for name in names:
                    if Path(name).suffix.lower() in self._media_suffixes():
                        files.append(str(root_path / name))
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
        root = self._normalise_path_text(self._strm_check_root or self.DEFAULT_STRM_CHECK_ROOT).rstrip("/") or self.DEFAULT_STRM_CHECK_ROOT
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
        ts = float(data.get("ts") or 0) if isinstance(data, dict) else 0
        if time.time() - ts > 86400:
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
        with self._lock:
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
        self._restore_queue_timers()
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
        for key in ("show_root", "check_dir", "raw_path", "strm_path", "episode_strm"):
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
        for value in candidates:
            markers.pop(f"tv_root_whole_scrape::{value}", None)
            markers.pop(f"tv_season_scrape::{value}", None)

    def _load_state(self) -> Dict[str, Any]:
        data = self.get_data("state") or {}
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
        history = state.setdefault("history", [])
        if not isinstance(history, list):
            history = []
            state["history"] = history
        # 去重：同一动作、同一范围、同一刮削目标在短时间内重复出现时，只更新计数，不刷屏。
        key = self._history_dedupe_key(result)
        now_ts = time.time()
        for old in reversed(history[-20:]):
            if not isinstance(old, dict):
                continue
            if old.get("_dedupe_key") != key:
                continue
            old_ts = float(old.get("_dedupe_ts") or 0)
            if old_ts and now_ts - old_ts <= 600:
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
    def _path_same_or_under(child: str, parent: str) -> bool:
        child = LocalMetadataCleaner._normalise_path_text(child)
        parent = LocalMetadataCleaner._normalise_path_text(parent)
        if child == parent:
            return True
        return child.startswith(parent.rstrip("/") + "/")

    @staticmethod
    def _map_root(path: str, left: str, right: str) -> str:
        path = LocalMetadataCleaner._normalise_path_text(path)
        left = LocalMetadataCleaner._normalise_path_text(left)
        right = LocalMetadataCleaner._normalise_path_text(right)
        if path == left:
            return right
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
                cleaned.append(text.rstrip("/"))
            else:
                cleaned.append(text.strip("/"))
        return "/".join([x for x in cleaned if x]) or "/"

    @staticmethod
    def _image_suffixes() -> set:
        return {".jpg", ".jpeg", ".png", ".webp"}

    @staticmethod
    def _media_suffixes() -> tuple:
        # 按常见程度排序，单集刮削时会依次尝试把 .strm 替换成这些视频后缀。
        return (".mkv", ".mp4", ".ts", ".m2ts", ".iso", ".mov", ".avi", ".rmvb", ".wmv", ".flv", ".mpeg", ".mpg")

    def _send_notify(self, result: Dict[str, Any]):
        try:
            failed = result.get("scrape") is False
            title = "〖监控strm刮削网盘失败〗" if failed else "〖监控strm刮削网盘完成〗"
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=title,
                text=(
                    f"动作：{self.ACTION_LABELS.get(str(result.get('action') or ''), str(result.get('action') or ''))}\n"
                    f"范围：{result.get('scope', '')}\n"
                    f"刮削目标：{result.get('folder', '')}\n"
                    f"刮削：{result.get('scrape_msg', '')}\n"
                    f"说明：{result.get('note', '')}"
                )
            )
        except Exception as err:
            logger.error(f"监控strm刮削网盘：发送通知失败：{err}")

    def _queue_title(self, key: str, item: Dict[str, Any]) -> str:
        task_type = str(item.get("task_type") or "")
        if task_type == "tv_recheck":
            show_root = Path(str(item.get("show_root") or ""))
            name = show_root.name or str(show_root)
            preview = item.get("missing_preview") or {}
            if preview.get("total", 0) > 0:
                return f"10天复查：{name}（当前缺图 {preview.get('total')} 集）"
            return f"10天复查：{name}"
        if task_type == "tv_postcheck":
            show_root = Path(str(item.get("show_root") or ""))
            return f"刮削后检查：{show_root.name or str(show_root)}"
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
            "2. 原理：收到 Emby 入库通知后，先检查 STRM 路径里的刮削信息是否完整；判断需要刮削时，再去刮削 CD2 挂载的网盘文件。\n"
            "3. MP 必须同时映射 STRM 文件夹和 CD2 文件夹，建议 STRM 映射路径与 Emby 一致。例如 Emby 是 /media，MP 也建议映射为 /media。\n"
            "4. STRM 检查根路径填写 Emby/MP 看到的 STRM 根目录，例如 /media；MP 刮削目标根路径填写 MP 看到的 CD2 网盘媒体根目录，例如 /CD2/115/CMS影库/影视。兜底检查周期只用于补跑丢失或到期未执行的队列任务。\n"
            "5. 路径示例：/media/电影/华语电影/片名/xxx.strm 用于检查；需要刮削时映射为 /CD2/115/CMS影库/影视/电影/华语电影/片名。\n"
            "6. 电影：检查片名目录是否同时存在 backdrop、fanart、poster/folder/cover 类图片和任意 nfo；完整则跳过，不完整则立即刮削一次，不进入后续队列。\n"
            "7. 电视剧/番剧：定位 STRM 所在剧名根目录。剧名根目录没有任何图片/nfo 时，立即刮削整部剧；刮削事件发送成功后创建 10 分钟检查，缺图集才加入 10 天复查队列。\n"
            "8. 剧名根目录已有基础信息时，会先检查当前季信息；具体季号只认可 season02-poster 这类对应季海报，避免通用 season-poster 误判第二季完整。缺季信息会刮削当前季并在 10 分钟后复查季信息。\n"
            "9. 本次入库单集缺图时，先删除该集同名 nfo，等待 30 秒后只刮削该集；单集刮削事件发送成功后，才从成功时间开始计时 10 分钟检查。仍缺图时加入 10 天复查队列；已生成图片的旧复查任务会自动清理。\n"
            "10. 媒体库过滤会同时识别路径第一层和第二层，例如 /media/电视剧/国产剧/... 可命中‘电视剧’或‘国产剧’；如果媒体库名称和实际路径不一致，可在‘媒体库路径映射’里填写：媒体库名称|类型|路径1,路径2，例如 动漫|tv|/media/电视剧/国漫,/media/电视剧/日番。\n"
            "11. 待处理任务可在插件详情页单独立即执行、删除或清空，同一部剧的单集刮削任务会合并展示。检查类任务手动提前检查仍缺图时，会保留原到期时间，不会提前进入10天复查。"
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
            "tv_recheck_days": self._tv_recheck_days,
            "scrape": self._scrape,
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
            return float(value)
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

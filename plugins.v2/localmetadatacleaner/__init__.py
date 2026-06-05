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
    plugin_version = "1.1"
    plugin_author = "jidian"
    author_url = ""
    plugin_config_prefix = "localmetadatacleaner_"
    plugin_order = 99
    auth_level = 1

    # 基础开关
    _enabled: bool = False
    _notify: bool = True
    _onlyonce: bool = False

    # 媒体服务器 / 媒体库过滤
    _media_server: str = ""
    _include_libraries: List[str] = []
    _all_libraries: List[Dict[str, Any]] = []

    # 路径：检查看 STRM 库，刮削走网盘目标根路径
    _strm_check_root: str = "/media"
    _scrape_target_root: str = "/CD2/115/CMS影库/影视"
    _target_depth: int = 3

    # 时间配置
    _cron: str = "*/1 * * * *"
    _initial_check_delay_seconds: int = 10
    _episode_scrape_delay_minutes: float = 6
    _tv_recheck_days: float = 10

    # 刮削配置
    _scrape: bool = True
    _overwrite: bool = False
    _storage: str = "local"

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

            self._strm_check_root = str(config.get("strm_check_root") or "/media").strip().rstrip("/") or "/media"
            self._scrape_target_root = str(config.get("scrape_target_root") or "/CD2/115/CMS影库/影视").strip().rstrip("/") or "/CD2/115/CMS影库/影视"
            self._target_depth = 3

            self._cron = str(config.get("cron") or "*/1 * * * *").strip() or "*/1 * * * *"
            self._initial_check_delay_seconds = int(self._to_float(config.get("initial_check_delay_seconds"), 10))
            self._episode_scrape_delay_minutes = self._to_float(config.get("episode_scrape_delay_minutes"), 6)
            self._tv_recheck_days = self._to_float(config.get("tv_recheck_days"), 10)
            self._scrape = bool(config.get("scrape", True))
            self._overwrite = bool(config.get("overwrite", False))
            # 页面不再显示存储标识，普通本地映射固定使用 local
            self._storage = "local"

            self._queue_delete_items = self._to_list(config.get("queue_delete_items") or [])
            self._queue_delete_confirm = bool(config.get("queue_delete_confirm", False))
            self._queue_clear_all = bool(config.get("queue_clear_all", False))
            self._clear_history = bool(config.get("clear_history", False))

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

        if self._clear_history:
            state = self._load_state()
            old_count = len(state.get("history") or []) if isinstance(state.get("history"), list) else 0
            state["history"] = []
            self.save_data("state", state)
            logger.info(f"监控strm刮削网盘：已清空历史记录，共删除 {old_count} 条")
            self._clear_history = False
            queue_action_done = True

        if self._media_server and self._library_cache_needs_refresh():
            self._refresh_library_cache()
            self.__update_config()

        if self._onlyonce:
            logger.info("监控strm刮削网盘：立即运行一次")
            self.run_once()
            self._onlyonce = False
            self.__update_config()

        if queue_action_done:
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron or "*/1 * * * *")
        except Exception as err:
            logger.error(f"监控strm刮削网盘：cron 表达式错误，使用默认 */1 * * * *：{err}")
            trigger = CronTrigger.from_crontab("*/1 * * * *")
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
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSelect", "props": {"model": "media_server", "label": "媒体服务器", "items": self._get_media_server_select_items(), "clearable": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "cron", "label": "检查周期", "placeholder": "*/1 * * * *"}}]},
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
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "strm_check_root", "label": "STRM 检查根路径", "placeholder": "/media"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "scrape_target_root", "label": "MP 刮削目标根路径", "placeholder": "/CD2/115/CMS影库/影视"}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "episode_scrape_delay_minutes", "label": "单集刮削等待分钟", "placeholder": "6"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "tv_recheck_days", "label": "电视剧复查天数", "placeholder": "10"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "scrape", "label": "触发 MP 刮削"}}]}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "overwrite", "label": "刮削覆盖模式"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "clear_history", "label": "清空历史记录"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": []}
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [{"component": "VSelect", "props": {"model": "queue_delete_items", "label": "待处理队列操作", "placeholder": "选择要删除的队列任务；保存配置后生效", "items": self._get_queue_select_items(), "multiple": True, "chips": True, "closable-chips": True, "clearable": True}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [{"component": "VSwitch", "props": {"model": "queue_delete_confirm", "label": "删除所选任务"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [{"component": "VSwitch", "props": {"model": "queue_clear_all", "label": "清空全部队列"}}]}
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
            "strm_check_root": "/media",
            "scrape_target_root": "/CD2/115/CMS影库/影视",
            "cron": "*/1 * * * *",
            "initial_check_delay_seconds": 10,
            "episode_scrape_delay_minutes": 6,
            "tv_recheck_days": 10,
            "scrape": True,
            "overwrite": False,
            "clear_history": False,
            "queue_delete_items": [],
            "queue_delete_confirm": False,
            "queue_clear_all": False
        }

    def get_page(self) -> List[dict]:
        """插件详情页：参考签到历史页，使用概览卡片 + 紧凑记录列表。"""
        state = self._load_state()
        queue = state.get("queue") or {}
        history = state.get("history") or []
        queue_count = len(queue)
        history_display = self._history_for_display(history, limit=10)

        task_stats = self._queue_stats(queue)
        last_record = history[-1] if history else {}

        cards: List[Dict[str, Any]] = []

        cards.append({
            "component": "VRow",
            "props": {"class": "mb-3"},
            "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 rounded-lg"}, "content": [
                        self._card_header("mdi-view-dashboard-outline", "运行概览"),
                        {"component": "VDivider", "props": {"class": "my-3"}},
                        {"component": "VRow", "content": [
                            self._summary_metric("插件状态", "已启用" if self._enabled else "未启用", "success" if self._enabled else "grey"),
                            self._summary_metric("待处理", f"{queue_count} 个", "info" if queue_count else "success"),
                            self._summary_metric("10天复查", f"{task_stats.get('tv_recheck', 0)} 个", "warning" if task_stats.get('tv_recheck') else "grey"),
                            self._summary_metric("单集刮削", f"{task_stats.get('episode_scrape', 0)} 个", "purple" if task_stats.get('episode_scrape') else "grey"),
                        ]},
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mt-3", "text": self._overview_text(last_record)}}
                    ]}
                ]},
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 rounded-lg"}, "content": [
                        self._card_header("mdi-folder-sync-outline", "路径与规则"),
                        {"component": "VDivider", "props": {"class": "my-3"}},
                        {"component": "VRow", "content": [
                            self._path_metric("STRM 检查根路径", self._strm_check_root or "/media"),
                            self._path_metric("MP 刮削目标根路径", self._scrape_target_root or "/CD2/115/CMS影库/影视"),
                        ]},
                        {"component": "VAlert", "props": {"type": "success", "variant": "tonal", "density": "compact", "class": "mt-3", "text": "判断是否完整只检查 STRM 库；需要刮削时再把路径映射到 MP 刮削目标。"}}
                    ]}
                ]}
            ]
        })

        cards.append(self._queue_section(queue, queue_count))
        cards.append(self._history_section(history_display))

        if not queue and not history_display:
            cards.append({
                "component": "VAlert",
                "props": {"type": "success", "variant": "tonal", "class": "mt-3", "text": "暂无待处理任务和历史记录。"}
            })

        return [{"component": "div", "props": {"class": "pa-2"}, "content": cards}]

    def _queue_section(self, queue: Dict[str, Any], queue_count: int) -> Dict[str, Any]:
        queue_items = list(queue.items())[-10:] if queue else []
        content: List[Dict[str, Any]] = [
            self._section_title("mdi-timer-sand", f"待处理任务（{queue_count} 个）", "如需删除任务，请到插件设置页的“待处理队列操作”中选择并保存。")
        ]
        if queue_items:
            content.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [self._task_row_card(key, item)]}
                for key, item in queue_items
            ]})
            if queue_count > len(queue_items):
                content.append({"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "density": "compact", "class": "mt-2", "text": f"页面仅显示最新 {len(queue_items)} 个任务，实际队列共有 {queue_count} 个。"}})
        else:
            content.append({"component": "VAlert", "props": {"type": "success", "variant": "tonal", "density": "compact", "text": "当前没有待处理任务。"}})
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 mb-3 rounded-lg"}, "content": content}

    def _history_section(self, history_display: List[Dict[str, Any]]) -> Dict[str, Any]:
        content: List[Dict[str, Any]] = [
            self._section_title("mdi-history", "最近处理记录", "已做展示去重。")
        ]
        if history_display:
            content.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [self._history_row_card(item)]}
                for item in history_display
            ]})
        else:
            content.append({"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "text": "暂无最近处理记录。"}})
        return {"component": "VCard", "props": {"variant": "flat", "class": "pa-4 mb-3 rounded-lg"}, "content": content}

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
    def _summary_metric(label: str, value: str, color: str = "grey") -> Dict[str, Any]:
        return {"component": "VCol", "props": {"cols": 6, "sm": 3}, "content": [
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": label},
            {"component": "VChip", "props": {"color": color, "variant": "tonal", "size": "small", "class": "font-weight-medium"}, "text": value}
        ]}

    @staticmethod
    def _path_metric(label: str, value: str) -> Dict[str, Any]:
        return {"component": "VCol", "props": {"cols": 12, "sm": 6}, "content": [
            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": label},
            {"component": "VSheet", "props": {"class": "pa-2 rounded text-body-2", "color": "grey-lighten-4"}, "text": value}
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

    def _task_row_card(self, key: str, item: Dict[str, Any]) -> Dict[str, Any]:
        task_type = str(item.get("task_type") or "")
        title = self._queue_title(key, item)
        status = str(item.get("status") or "")
        due_at = str(item.get("due_at") or "")
        strm = str(item.get("strm_path") or item.get("episode_strm") or item.get("show_root") or "")
        target = str(item.get("scrape_target") or item.get("scrape_dir") or "")
        chips = [self._chip(self._task_type_label(task_type), self._task_color(task_type))]
        if status:
            chips.append(self._chip(status, "grey"))
        if due_at:
            chips.append(self._chip(f"到期 {due_at}", "info"))

        detail_lines: List[Dict[str, Any]] = []
        if strm:
            detail_lines.append(self._mini_line("STRM", self._short_path(strm)))
        if target:
            detail_lines.append(self._mini_line("刮削目标", self._short_path(target)))

        if task_type == "tv_recheck":
            preview = item.get("missing_preview") or {}
            if preview.get("exists"):
                total = int(preview.get("total") or 0)
                detail_lines.append(self._mini_line("当前缺图", f"{total} 集" if total else "暂未发现，到期会复查全部 STRM"))
                names = preview.get("names") or []
                if names:
                    detail_lines.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-1 mt-2"}, "content": [self._chip(str(name), "warning") for name in names]})
                    if preview.get("truncated"):
                        detail_lines.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": "仅展示前 8 集，到期仍会检查全部 STRM。"})
            else:
                detail_lines.append(self._mini_line("当前缺图", "剧名目录暂不可访问，到期会再次检查"))

        msg = str(item.get("last_msg") or "")
        if msg:
            detail_lines.append(self._mini_line("说明", msg))

        return {"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 rounded-lg"}, "content": [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2"}, "content": [
                {"component": "div", "props": {"class": "text-subtitle-1 font-weight-medium"}, "text": title},
                {"component": "div", "props": {"class": "d-flex flex-wrap ga-1"}, "content": chips}
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
        dup = int(item.get("duplicate_count") or 0)
        if dup > 1:
            detail_lines.append(self._mini_line("重复合并", f"{dup} 次"))

        return {"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 rounded-lg"}, "content": [
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

    @staticmethod
    def _task_type_label(task_type: str) -> str:
        return {
            "initial": "入库检查",
            "episode_scrape": "单集刮削",
            "tv_recheck": "10天复查",
        }.get(task_type, task_type or "任务")

    @staticmethod
    def _task_color(task_type: str) -> str:
        return {
            "initial": "primary",
            "episode_scrape": "purple",
            "tv_recheck": "warning",
        }.get(task_type, "grey")

    @staticmethod
    def _action_label(action: str) -> str:
        mapping = {
            "movie_complete_skip": "电影完整跳过",
            "movie_incomplete_scrape": "电影补刮削",
            "tv_root_no_metadata_scrape_whole_show": "整剧刮削",
            "tv_root_no_metadata_merge_existing_whole_show_scrape": "整剧事件合并",
            "episode_missing_image_schedule_scrape": "单集缺图",
            "episode_scrape_done": "单集刮削完成",
            "episode_scrape_failed": "单集刮削失败",
            "episode_delayed_scrape": "单集延迟刮削",
            "tv_episode_missing_image_schedule_scrape": "单集缺图",
            "tv_episode_image_exists_skip_initial": "单集完整跳过",
            "movie_metadata_complete_skip": "电影完整跳过",
            "movie_metadata_incomplete_scrape": "电影补刮削",
            "tv_recheck_missing_episodes_schedule_scrape": "10天复查缺图",
            "tv_recheck_complete": "10天复查完成",
            "queue_cleared_by_user": "清空队列",
            "queue_deleted_by_user": "删除任务",
        }
        return mapping.get(action, action or "记录")

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
            if event_name and not self._is_item_added_event(event_name):
                return
            channel = str(self._get_obj_value(event_info, "channel") or payload.get("channel") or "").strip().lower()
            source = str(self._get_obj_value(event_info, "source") or payload.get("source") or "").strip().lower()
            if channel and channel not in {"emby", ""}:
                return
            if source and "emby" not in source and source not in {"media_server", "mediaserver", ""}:
                # 不强拦截 source，避免不同 MP 版本字段不同；仅排除明显非 Emby 源。
                pass

            raw_path = self._event_info_path(event_info, payload)
            if not raw_path:
                item_id = str(self._get_obj_value(event_info, "item_id") or self._get_obj_value(event_info, "itemid") or payload.get("item_id") or payload.get("itemid") or "").strip()
                server_name = str(self._get_obj_value(event_info, "server_name") or self._get_obj_value(event_info, "source") or payload.get("server_name") or payload.get("source") or self._media_server or "").strip()
                item_info = self._query_media_item_info(item_id=item_id, server_name=server_name)
                if item_info:
                    payload.setdefault("_mp_item_info", item_info)
                    raw_path = self._extract_payload_path(item_info)
            if not raw_path:
                logger.warning("监控strm刮削网盘：收到 MP Webhook 入库事件，但未取得媒体路径")
                return
            result = self._register_incoming_path(payload=payload, raw_path=raw_path, event_name=event_name or "library.new", source="mp_webhook_event")
            if result.get("duplicate"):
                logger.debug(f"监控strm刮削网盘：重复入库事件已合并：{result.get('task')}")
            elif result.get("success") and not result.get("ignored"):
                logger.info(f"监控strm刮削网盘：已通过 MP 全局 Webhook 事件登记任务：{result.get('task')}")
            else:
                logger.info(f"监控strm刮削网盘：MP 全局 Webhook 事件未登记任务：{result}")
        except Exception as err:
            logger.error(f"监控strm刮削网盘：处理 MP 全局 Webhook 事件失败：{err}\n{traceback.format_exc()}")

    def _register_incoming_path(self, payload: Any, raw_path: str, event_name: str = "", source: str = "") -> Dict[str, Any]:
        lib_allowed, lib_msg, lib_info = self._library_allowed(payload, raw_path)
        if not lib_allowed:
            logger.info(f"监控strm刮削网盘：媒体库过滤忽略：{lib_msg}，路径：{raw_path}")
            return {"success": True, "ignored": True, "message": lib_msg, "library_info": lib_info}

        raw_path = self._normalise_path_text(raw_path)
        info = self._analyse_incoming_strm_path(raw_path)
        if not info.get("success"):
            return {"success": False, "message": info.get("message") or "路径分析失败", "raw_path": raw_path}

        now_ts = time.time()
        now_iso = self._now_iso()

        # 电影按影片路径登记；电视剧按剧名根目录合并，避免一次入库几十集时创建几十个初始任务。
        if info.get("media_type") == "tv":
            task_key = f"initial_tv::{info.get('show_root')}"
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
                # 保留最早的初次检查时间，不因重复入库事件反复后延。
                self.save_data("state", state)
                return {"success": True, "duplicate": True, "message": "重复入库事件已合并", "task": task_key}

            task = {
                "task_type": "initial",
                "key": task_key,
                "raw_path": raw_path,
                "media_type": info.get("media_type"),
                "strm_path": info.get("strm_path"),
                "check_dir": info.get("check_dir"),
                "show_root": info.get("show_root"),
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
                task["missing_preview"] = self._build_missing_preview(Path(str(info.get("show_root") or "")), limit=8)
            queue[task_key] = task
            self.save_data("state", state)
        self._schedule_delayed_check(max(self._initial_check_delay_seconds, 0))
        return {"success": True, "message": "已登记入库任务", "task": task_key, "info": info}

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
                    if not isinstance(task, dict):
                        remove_keys.append(key)
                        changed = True
                        continue
                    due_ts = float(task.get("due_ts") or 0)
                    if due_ts and now_ts < due_ts:
                        continue

                    task_type = str(task.get("task_type") or "initial")
                    if task_type == "initial":
                        result = self._process_initial_task(task, state)
                    elif task_type == "episode_scrape":
                        result = self._process_episode_scrape_task(task, state)
                    elif task_type == "tv_recheck":
                        result = self._process_tv_recheck_task(task, state)
                    else:
                        result = {"success": True, "action": "drop_unknown_task", "scope": key, "folder": key, "message": f"未知任务类型：{task_type}"}
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
            except Exception as err:
                logger.error(f"监控strm刮削网盘：队列检查失败：{err}\n{traceback.format_exc()}")

    def _process_initial_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        raw_path = str(task.get("raw_path") or "")
        media_type = str(task.get("media_type") or "")
        if media_type == "movie":
            movie_dir = Path(str(task.get("movie_dir") or task.get("check_dir") or ""))
            scrape_dir = Path(str(task.get("scrape_dir") or self._map_strm_path_to_scrape_path(str(movie_dir))))
            status = self._movie_metadata_status(movie_dir)
            if status.get("complete"):
                result = {
                    "time": self._now_iso(), "action": "movie_metadata_complete_skip", "scope": str(movie_dir), "folder": str(scrape_dir),
                    "scrape": None, "scrape_msg": "电影刮削信息完整，跳过刮削", "metadata": status
                }
                self._append_history(state, result)
                return {"remove": True, **result}
            ok, msg = self._trigger_scrape(scrape_dir, overwrite=False)
            result = {
                "time": self._now_iso(), "action": "movie_metadata_incomplete_scrape", "scope": str(movie_dir), "folder": str(scrape_dir),
                "scrape": ok, "scrape_msg": msg, "metadata": status,
                "note": "电影缺少完整刮削信息，已立即触发一次刮削；电影不进入后续复查队列。"
            }
            self._append_history(state, result)
            if self._notify and not ok:
                self._send_notify(result)
            return {"remove": True, **result}

        if media_type == "tv":
            show_root = Path(str(task.get("show_root") or task.get("check_dir") or ""))
            episode_values = self._to_list(task.get("episodes") or [])
            if not episode_values:
                episode_values = [str(task.get("episode_strm") or task.get("strm_path") or raw_path)]
            episode_paths: List[Path] = []
            seen_episode = set()
            for value in episode_values:
                ep = Path(str(value or ""))
                if not str(ep):
                    continue
                key = str(ep)
                if key in seen_episode:
                    continue
                seen_episode.add(key)
                episode_paths.append(ep)

            root_status = self._tv_root_metadata_status(show_root)
            if not root_status.get("has_any_metadata"):
                scrape_dir = Path(self._map_strm_path_to_scrape_path(str(show_root)))
                markers = state.setdefault("markers", {})
                marker_key = f"tv_root_whole_scrape::{show_root}"
                marker = markers.get(marker_key) if isinstance(markers, dict) else None
                now_ts = time.time()
                if isinstance(marker, dict) and now_ts - float(marker.get("ts") or 0) < 3600:
                    self._ensure_tv_recheck_task(state, show_root, reason="root_no_metadata_merged")
                    return {
                        "remove": True,
                        "time": self._now_iso(),
                        "action": "tv_root_no_metadata_merge_existing_whole_show_scrape",
                        "scope": str(show_root),
                        "folder": str(scrape_dir),
                        "scrape": None,
                        "scrape_msg": "同一剧名根目录已触发过整剧刮削，本次入库事件已合并，仅保留10天复查任务。",
                        "skip_history": True,
                    }
                if isinstance(markers, dict):
                    markers[marker_key] = {"ts": now_ts, "time": self._now_iso(), "show_root": str(show_root)}
                ok, msg = self._trigger_scrape(scrape_dir, overwrite=False)
                result = {
                    "time": self._now_iso(), "action": "tv_root_no_metadata_scrape_whole_show", "scope": str(show_root), "folder": str(scrape_dir),
                    "scrape": ok, "scrape_msg": msg, "metadata": root_status,
                    "note": f"剧名根目录没有任何图片/nfo，已触发整部剧刮削；本次合并入库 {len(episode_paths)} 集，并加入10天复查队列。"
                }
                self._append_history(state, result)
                if self._notify and not ok:
                    self._send_notify(result)
                self._ensure_tv_recheck_task(state, show_root, reason="root_no_metadata")
                return {"remove": True, **result}

            # 根目录已有刮削信息：只检查本次入库的单集列表。
            missing_eps: List[Path] = []
            existing_count = 0
            deleted_total = 0
            for episode_strm in episode_paths:
                episode_status = self._episode_image_status(episode_strm)
                if episode_status.get("has_image"):
                    existing_count += 1
                    continue
                missing_eps.append(episode_strm)
                deleted = self._delete_episode_nfo(episode_strm)
                deleted_total += deleted
                self._ensure_episode_scrape_task(state, episode_strm, show_root, reason="initial_missing_image", deleted_nfo=deleted)

            self._ensure_tv_recheck_task(state, show_root, reason="episode_batch_initial")
            if missing_eps:
                names = "、".join([self._episode_label(ep, show_root) for ep in missing_eps[:8]])
                result = {
                    "time": self._now_iso(), "action": "tv_episode_missing_image_schedule_scrape", "scope": str(show_root),
                    "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                    "scrape_msg": f"本次合并入库 {len(episode_paths)} 集，其中 {len(missing_eps)} 集缺少对应图片，已删除同名 nfo {deleted_total} 个，{self._episode_scrape_delay_minutes:g} 分钟后逐集刮削。",
                    "note": names,
                    "missing_count": len(missing_eps),
                    "existing_count": existing_count,
                }
                self._append_history(state, result)
                return {"remove": True, **result}

            result = {
                "time": self._now_iso(), "action": "tv_episode_image_exists_skip_initial", "scope": str(show_root),
                "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None,
                "scrape_msg": f"本次合并入库 {len(episode_paths)} 集均已有对应图片，跳过立即刮削；整剧已加入10天复查队列。",
            }
            self._append_history(state, result)
            return {"remove": True, **result}

        result = {"time": self._now_iso(), "action": "skip_unknown_media_type", "scope": raw_path, "folder": raw_path, "scrape": None, "scrape_msg": "无法判断电影/电视剧类型，已跳过"}
        self._append_history(state, result)
        return {"remove": True, **result}

    def _process_episode_scrape_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        episode_strm = Path(str(task.get("episode_strm") or ""))
        target = task.get("scrape_target") or self._map_episode_strm_to_scrape_target(episode_strm)
        ok, msg = self._trigger_scrape(Path(str(target)), overwrite=False)
        result = {
            "time": self._now_iso(), "action": "episode_delayed_scrape", "scope": str(episode_strm), "folder": str(target),
            "scrape": ok, "scrape_msg": msg, "deleted_nfo": int(task.get("deleted_nfo") or 0),
            "note": str(task.get("reason") or "")
        }
        self._append_history(state, result)
        if self._notify and not ok:
            self._send_notify(result)
        return {"remove": True, **result}

    def _process_tv_recheck_task(self, task: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        show_root = Path(str(task.get("show_root") or ""))
        if not show_root.exists() or not show_root.is_dir():
            result = {"time": self._now_iso(), "action": "tv_recheck_show_missing", "scope": str(show_root), "folder": str(show_root), "scrape": None, "scrape_msg": "10天复查时剧名目录不存在，任务结束"}
            self._append_history(state, result)
            return {"remove": True, **result}
        missing = self._find_missing_episode_images(show_root)
        if not missing:
            result = {"time": self._now_iso(), "action": "tv_recheck_complete", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)), "scrape": None, "scrape_msg": "10天复查完成，每一集都有对应图片"}
            self._append_history(state, result)
            return {"remove": True, **result}
        total_deleted = 0
        for episode in missing:
            deleted = self._delete_episode_nfo(episode)
            total_deleted += deleted
            self._ensure_episode_scrape_task(state, episode, show_root, reason="tv_10day_recheck_missing_image", deleted_nfo=deleted)
        result = {
            "time": self._now_iso(), "action": "tv_recheck_missing_episodes_schedule_scrape", "scope": str(show_root), "folder": self._map_strm_path_to_scrape_path(str(show_root)),
            "scrape": None, "scrape_msg": f"10天复查发现 {len(missing)} 集缺少对应图片，已删除同名 nfo {total_deleted} 个，{self._episode_scrape_delay_minutes:g} 分钟后逐集刮削。",
            "missing_count": len(missing), "deleted_nfo": total_deleted
        }
        self._append_history(state, result)
        return {"remove": True, **result}

    # --------------------------- 队列与任务创建 ---------------------------

    def _ensure_episode_scrape_task(self, state: Dict[str, Any], episode_strm: Path, show_root: Path, reason: str = "", deleted_nfo: int = 0):
        queue = state.setdefault("queue", {})
        key = f"episode::{episode_strm}"
        due_ts = time.time() + max(self._episode_scrape_delay_minutes, 0) * 60
        target = self._map_episode_strm_to_scrape_target(episode_strm)
        existing = queue.get(key)
        if existing:
            existing["due_ts"] = min(float(existing.get("due_ts") or due_ts), due_ts)
            existing["due_at"] = self._ts_to_str(float(existing.get("due_ts") or due_ts))
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["deleted_nfo"] = int(existing.get("deleted_nfo") or 0) + int(deleted_nfo or 0)
            existing["last_msg"] = f"重复单集刮削任务已合并，等待：{existing['due_at']}"
            return
        queue[key] = {
            "task_type": "episode_scrape",
            "key": key,
            "episode_strm": str(episode_strm),
            "show_root": str(show_root),
            "scrape_target": str(target),
            "reason": reason,
            "deleted_nfo": int(deleted_nfo or 0),
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_episode_scrape",
            "last_msg": f"等待 {self._episode_scrape_delay_minutes:g} 分钟后刮削单集"
        }
        self._schedule_delayed_check(max(self._episode_scrape_delay_minutes, 0) * 60)

    def _ensure_tv_recheck_task(self, state: Dict[str, Any], show_root: Path, reason: str = ""):
        queue = state.setdefault("queue", {})
        key = f"tv_recheck::{show_root}"
        due_ts = time.time() + max(self._tv_recheck_days, 0) * 86400
        existing = queue.get(key)
        if existing:
            old_due = float(existing.get("due_ts") or 0)
            if due_ts > old_due:
                existing["due_ts"] = due_ts
                existing["due_at"] = self._ts_to_str(due_ts)
            existing["duplicate_count"] = int(existing.get("duplicate_count") or 0) + 1
            existing["reason"] = reason or existing.get("reason") or ""
            existing.setdefault("missing_preview", self._build_missing_preview(show_root, limit=8))
            existing["last_msg"] = f"10天复查任务已合并，复查时间：{existing.get('due_at')}"
            return
        queue[key] = {
            "task_type": "tv_recheck",
            "key": key,
            "show_root": str(show_root),
            "scrape_target": self._map_strm_path_to_scrape_path(str(show_root)),
            "reason": reason,
            "first_seen_ts": time.time(),
            "first_seen": self._now_iso(),
            "due_ts": due_ts,
            "due_at": self._ts_to_str(due_ts),
            "status": "waiting_tv_recheck",
            "missing_preview": self._build_missing_preview(show_root, limit=8),
            "last_msg": f"等待 {self._tv_recheck_days:g} 天后检查每一集是否都有对应图片"
        }


    def _schedule_delayed_check(self, delay_seconds: float):
        """创建轻量 Timer，避免必须等到下一次 cron 才处理 10 秒/6 分钟任务；同一时间段只保留一个。"""
        try:
            delay = max(float(delay_seconds or 0), 0)
            due_ts = time.time() + delay
            # 大量剧集同时入库时会创建几十个10秒Timer，这里合并为一个，减少重复队列检查日志。
            if getattr(self, "_next_timer_due_ts", 0) and abs(float(self._next_timer_due_ts) - due_ts) <= 3:
                return
            self._next_timer_due_ts = due_ts
            timer = Timer(delay, self.run_once)
            timer.daemon = True
            timer.start()
            self._timers.append(timer)
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：创建延迟检查任务失败：{err}")

    # --------------------------- 路径与元数据判断 ---------------------------

    def _analyse_incoming_strm_path(self, raw_path: str) -> Dict[str, Any]:
        path_text = self._normalise_path_text(raw_path)
        root = self._strm_check_root.rstrip("/") or "/media"
        if not self._path_same_or_under(path_text, root):
            return {"success": False, "message": f"路径不在 STRM 检查根路径下：{path_text}"}
        rel_parts = self._relative_parts_under_root(path_text, root)
        if len(rel_parts) < self._target_depth:
            return {"success": False, "message": f"路径层级不足，至少需要 {self._target_depth} 级：{path_text}"}
        first = rel_parts[0].strip().lower()
        media_type = "tv" if first in {"电视剧", "剧集", "番剧", "动漫", "tv", "series", "shows"} else "movie"
        root_dir_text = self._join_posix(root, *rel_parts[:self._target_depth])
        strm_path = Path(path_text)
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

    def _episode_image_status(self, episode_strm: Path) -> Dict[str, Any]:
        candidates = self._episode_image_candidates(episode_strm)
        exists = [str(p) for p in candidates if p.exists() and p.is_file()]
        return {"has_image": bool(exists), "exists": exists, "candidates": [str(p) for p in candidates]}

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

    def _build_missing_preview(self, show_root: Path, limit: int = 30) -> Dict[str, Any]:
        try:
            if not show_root.exists() or not show_root.is_dir():
                return {"exists": False, "total": 0, "names": []}
            missing = self._find_missing_episode_images(show_root)
            names = [self._episode_label(p, show_root) for p in missing[:max(int(limit or 0), 0)]]
            return {"exists": True, "total": len(missing), "names": names, "truncated": len(missing) > len(names)}
        except Exception as err:
            logger.debug(f"监控strm刮削网盘：生成缺图集预览失败：{show_root} - {err}")
            return {"exists": False, "total": 0, "names": []}

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

    def _map_episode_strm_to_scrape_target(self, episode_strm: Path) -> str:
        mapped = Path(self._map_strm_path_to_scrape_path(str(episode_strm)))
        # STRM 库是 .strm，网盘里通常是 .mkv/.mp4 等真实媒体，优先找同名真实媒体文件。
        if mapped.suffix.lower() == ".strm":
            parent = mapped.parent
            stem = mapped.stem
            try:
                if parent.exists() and parent.is_dir():
                    for suffix in self._media_suffixes():
                        candidate = parent / f"{stem}{suffix}"
                        if candidate.exists() and candidate.is_file():
                            return str(candidate)
                    # 大小写或后缀不标准时，按 stem 扫同目录。
                    for item in parent.iterdir():
                        if item.is_file() and item.stem == stem and item.suffix.lower() in self._media_suffixes():
                            return str(item)
            except Exception as err:
                logger.debug(f"监控strm刮削网盘：查找网盘同名单集媒体失败：{mapped} - {err}")
            # 找不到真实文件就退回季目录，避免直接把不存在的 .strm 交给 MP。
            return str(parent)
        return str(mapped)

    # --------------------------- 刮削 ---------------------------

    def _trigger_scrape(self, target: Path, overwrite: Optional[bool] = None) -> Tuple[bool, str]:
        if not self._scrape:
            return False, "未启用刮削"
        if StorageChain is None:
            return False, "当前 MP 版本无法导入 StorageChain"
        try:
            if self._storagechain is None:
                self._storagechain = StorageChain()
            fileitem = self._storagechain.get_file_item(storage=self._storage, path=target)
            if not fileitem:
                fileitem = self._storagechain.get_file_item(storage=self._storage, path=str(target))
            if not fileitem:
                return False, f"无法获取文件项，请确认路径在 MP 容器内可见：{target}"
            file_list = self._file_list_for_scrape(target)
            eventmanager.send_event(EventType.MetadataScrape, {
                "fileitem": fileitem,
                "file_list": file_list,
                "meta": None,
                "mediainfo": None,
                "overwrite": bool(self._overwrite if overwrite is None else overwrite)
            })
            scope = "文件" if target.is_file() else "目录"
            return True, f"已发送 MP 刮削事件，{scope}：{target}，媒体文件 {len(file_list)} 个"
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
        norm_selected = {self._bare_library_name(x) for x in selected if self._bare_library_name(x)}
        norm_candidates = {self._bare_library_name(x) for x in candidates if self._bare_library_name(x)}
        matched = sorted(norm_selected & norm_candidates)
        if matched:
            return True, "命中已选媒体库", {"mode": "selected", "selected": selected, "candidates": candidates, "matched": matched, "path_parts": path_parts}
        return False, "未命中已选媒体库，已忽略", {"mode": "not_selected", "selected": selected, "candidates": candidates, "path_parts": path_parts, "raw_path": raw_path}

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

    def _get_queue_select_items(self) -> List[Dict[str, str]]:
        state = self._load_state()
        queue = state.get("queue") or {}
        if not isinstance(queue, dict) or not queue:
            return []
        items: List[Dict[str, str]] = []
        for key, meta in sorted(queue.items(), key=lambda kv: float((kv[1] or {}).get("due_ts") or 0)):
            if not isinstance(meta, dict):
                meta = {}
            task_type = str(meta.get("task_type") or "")
            due = str(meta.get("due_at") or "")
            title = self._queue_title(str(key), meta)
            if due:
                title = f"{title} ｜ {due}"
            items.append({"title": title, "value": str(key)})
        return items

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
                self._append_history(state, {"time": self._now_iso(), "action": "queue_deleted_by_user", "scope": key, "folder": key, "scrape": None, "scrape_msg": "用户手动删除队列任务，未执行刮削"})
            self.save_data("state", state)
        return deleted, missing

    def _clear_queue_items(self) -> int:
        with self._lock:
            state = self._load_state()
            queue = state.setdefault("queue", {})
            count = len(queue) if isinstance(queue, dict) else 0
            if count:
                state["queue"] = {}
                self._append_history(state, {"time": self._now_iso(), "action": "queue_cleared_by_user", "scope": "全部待处理队列", "folder": "全部待处理队列", "scrape": None, "scrape_msg": f"用户手动清空队列，共 {count} 个任务"})
                self.save_data("state", state)
        return count

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
                old["duplicate_count"] = int(old.get("duplicate_count") or 1) + 1
                old["_dedupe_ts"] = now_ts
                return
        result["_dedupe_key"] = key
        result["_dedupe_ts"] = now_ts
        history.append(result)
        state["history"] = history[-100:]

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
    def _media_suffixes() -> set:
        return {".mp4", ".mkv", ".ts", ".iso", ".avi", ".rmvb", ".wmv", ".mov", ".m2ts", ".flv", ".mpeg", ".mpg"}

    def _send_notify(self, result: Dict[str, Any]):
        try:
            failed = result.get("scrape") is False
            title = "〖监控strm刮削网盘失败〗" if failed else "〖监控strm刮削网盘完成〗"
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=title,
                text=(
                    f"动作：{result.get('action', '')}\n"
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
            "1. 本插件复用 MP 全局 WebhookMessage 入库事件，建议先启用‘媒体库服务器通知’插件；只要它能收到 Emby 新入库，本插件也能监听同一条事件。\n"
            "2. 判断是否刮削完整只看 STRM 库路径，不扫描 CD2：默认 STRM 检查根路径为 /media。\n"
            "3. 需要刮削时，通过 MP 刮削目标根路径触发 MP 刮削，例如 /media/电影/华语电影/片名/xxx.strm → /CD2/115/CMS影库/影视/电影/华语电影/片名。\n"
            "4. 电影：检查片名目录是否同时存在 backdrop、fanart、poster/folder/cover 类图片和任意 nfo；完整则跳过，不完整则立即刮削一次，不进入后续队列。\n"
            "5. 电视剧/番剧：定位 STRM 所在剧名根目录。剧名根目录没有任何图片/nfo 时，立即刮削整部剧；已有根目录信息时，只检查入库单集是否有对应图片，缺图则删除该集同名 nfo，等待 6 分钟后刮削该集。\n"
            "6. 电视剧/番剧会按剧名根目录合并入库事件，并合并一个 10 天复查任务；到期后遍历该剧所有 strm，发现缺少对应图片的单集，删除同名 nfo 并等待 6 分钟后逐集刮削。\n"
            "7. 媒体库过滤会同时识别路径第一层和第二层，例如 /media/电视剧/国产剧/... 可命中‘电视剧’或‘国产剧’。\n"
            "8. 队列操作只删除插件里的等待任务，不会删除实际文件；清空历史记录只清空页面记录。"
        )

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": False,
            "media_server": self._media_server,
            "include_libraries": self._include_libraries,
            "all_libraries": self._all_libraries,
            "strm_check_root": self._strm_check_root,
            "scrape_target_root": self._scrape_target_root,
            "cron": self._cron,
            "initial_check_delay_seconds": self._initial_check_delay_seconds,
            "episode_scrape_delay_minutes": self._episode_scrape_delay_minutes,
            "tv_recheck_days": self._tv_recheck_days,
            "scrape": self._scrape,
            "overwrite": self._overwrite,
            "clear_history": False,
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

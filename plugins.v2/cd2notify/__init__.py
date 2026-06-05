from datetime import datetime
import posixpath
from threading import Lock, Timer
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from app import schemas
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, Notification


class CD2ChangeItem(BaseModel):
    action: Optional[str] = ""
    is_dir: Any = ""
    source_file: Optional[str] = ""
    destination_file: Optional[str] = ""


class CD2NotifyRequest(BaseModel):
    device_name: Optional[str] = ""
    user_name: Optional[str] = ""
    version: Optional[str] = ""
    event_category: Optional[str] = ""
    event_name: Optional[str] = ""
    event_time: Optional[str] = ""
    send_time: Optional[str] = ""
    data: List[CD2ChangeItem] = Field(default_factory=list)


class CD2MountItem(BaseModel):
    action: Optional[str] = ""
    mount_point: Optional[str] = ""
    status: Any = ""
    reason: Optional[str] = ""


class CD2MountRequest(BaseModel):
    device_name: Optional[str] = ""
    user_name: Optional[str] = ""
    version: Optional[str] = ""
    event_category: Optional[str] = ""
    event_name: Optional[str] = ""
    event_time: Optional[str] = ""
    send_time: Optional[str] = ""
    data: List[CD2MountItem] = Field(default_factory=list)


class CD2Notify(_PluginBase):
    # 插件名称
    plugin_name = "CloudDrive2通知"
    # 插件描述
    plugin_desc = "接收 CloudDrive2 Webhook，解析新增、删除、移动、重命名等动作，并通过 MoviePilot 通知渠道推送。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/webhook.png"
    # 插件版本
    plugin_version = "1"
    # 插件作者
    plugin_author = "jidian"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "cd2notify_"
    # 加载顺序
    plugin_order = 30
    # 可使用的用户级别
    auth_level = 1

    _enabled: bool = True
    _notify: bool = True
    _msgtype: str = "Manual"
    _detail_limit: int = 10
    _show_file_name: bool = True
    _aggregate_notify: bool = False
    _aggregate_wait: int = 20
    _history_limit: int = 50
    _clear_history: bool = False
    _history: List[Dict[str, Any]] = []
    _pending_file_items: List[CD2ChangeItem] = []
    _pending_file_meta: Dict[str, Any] = {}
    _pending_timer: Optional[Timer] = None
    _pending_lock: Optional[Lock] = None

    def init_plugin(self, config: dict = None):
        self._enabled = True
        self._notify = True
        self._msgtype = "Manual"
        self._detail_limit = 10
        self._show_file_name = True
        self._aggregate_notify = False
        self._aggregate_wait = 20
        self._history_limit = 50
        self._clear_history = False

        # 初始化聚合通知缓存。开启“等待任务结束后通知”时，会先缓存多次 CD2 回调，
        # 等待一段时间没有新回调后再统一发送一条汇总通知。
        if self._pending_lock is None:
            self._pending_lock = Lock()
        try:
            if self._pending_timer:
                self._pending_timer.cancel()
        except Exception:
            pass
        self._pending_file_items = []
        self._pending_file_meta = {}
        self._pending_timer = None

        if config:
            self._enabled = self._to_bool(config.get("enabled", True))
            self._notify = self._to_bool(config.get("notify", True))
            self._msgtype = config.get("msgtype") or "Manual"
            self._detail_limit = self._to_int(config.get("detail_limit", 10), 10)
            self._show_file_name = self._to_bool(config.get("show_file_name", True))
            self._aggregate_notify = self._to_bool(config.get("aggregate_notify", False))
            self._aggregate_wait = self._to_int(config.get("aggregate_wait", 20), 20)
            self._history_limit = self._to_int(config.get("history_limit", 50), 50)
            self._clear_history = self._to_bool(config.get("clear_history", False))

        # v1.0.7 以前历史记录保存在配置中；这里兼容读取并迁移到插件数据。
        history = self.get_data("history") or []
        if not history and config:
            old_history = config.get("history") or []
            if isinstance(old_history, list):
                history = old_history
        self._history = history if isinstance(history, list) else []

        if self._clear_history:
            self._history = []
            self.save_data("history", [])
            self._clear_history = False
            try:
                self.update_config(self._current_config())
            except Exception as err:
                logger.warning(f"{self.plugin_name}: 重置清空历史开关失败 - {err}")

        self._trim_history(save=True)

    def get_state(self) -> bool:
        return bool(self._enabled)

    @staticmethod
    def _to_bool(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in ("true", "1", "yes", "y", "on", "开启", "是")
        return bool(val)

    @staticmethod
    def _to_int(val: Any, default: int = 0) -> int:
        try:
            return int(val)
        except Exception:
            return default

    @staticmethod
    def _bool_text(value: Any) -> Tuple[bool, str]:
        v = str(value).strip().lower()
        is_dir = v in ("true", "1", "yes", "y", "目录", "dir", "directory")
        return is_dir, "目录" if is_dir else "文件"

    @staticmethod
    def _normalize_action(action: str) -> str:
        return (action or "").lower().strip()

    @staticmethod
    def _classify_rename(source_file: str, destination_file: str) -> str:
        """CloudDrive2 的 rename 同时代表移动和重命名。

        这里通过原路径/新路径做近似判断：
        - 同目录、文件名变化：重命名
        - 目录变化、文件名相同：移动
        - 目录和文件名都变化：移动并重命名
        - 信息不足：移动/重命名
        """
        src = (source_file or "").strip()
        dst = (destination_file or "").strip()
        if not src or not dst:
            return "rename_unknown"

        src_dir = posixpath.dirname(src.rstrip("/"))
        dst_dir = posixpath.dirname(dst.rstrip("/"))
        src_name = posixpath.basename(src.rstrip("/"))
        dst_name = posixpath.basename(dst.rstrip("/"))

        dir_changed = src_dir != dst_dir
        name_changed = src_name != dst_name

        if dir_changed and name_changed:
            return "move_rename"
        if dir_changed:
            return "move"
        if name_changed:
            return "rename"
        return "rename_unknown"

    @staticmethod
    def _action_text(action: str) -> str:
        mapping = {
            "create": "新增",
            "delete": "删除",
            "move": "移动",
            "rename": "重命名",
            "move_rename": "移动并重命名",
            "rename_unknown": "移动/重命名",
            "mount": "挂载",
            "unmount": "卸载",
        }
        return mapping.get((action or "").lower(), action or "未知")

    @staticmethod
    def _action_icon(action: str) -> str:
        mapping = {
            "create": "➕",
            "delete": "🗑️",
            "move": "📦",
            "rename": "✏️",
            "move_rename": "🔀",
            "rename_unknown": "🔁",
            "mount": "🔌",
            "unmount": "🔌",
        }
        return mapping.get((action or "").lower(), "📌")

    @staticmethod
    def _action_color(action: str) -> str:
        mapping = {
            "create": "success",
            "delete": "error",
            "move": "info",
            "rename": "warning",
            "move_rename": "info",
            "rename_unknown": "info",
            "mount": "success",
            "unmount": "warning",
        }
        return mapping.get((action or "").lower(), "secondary")

    @staticmethod
    def _action_mdi(action: str) -> str:
        mapping = {
            "create": "mdi-plus-circle-outline",
            "delete": "mdi-delete-outline",
            "move": "mdi-folder-move-outline",
            "rename": "mdi-rename-outline",
            "move_rename": "mdi-swap-horizontal",
            "rename_unknown": "mdi-swap-horizontal",
            "mount": "mdi-harddisk-plus",
            "unmount": "mdi-harddisk-remove",
        }
        return mapping.get((action or "").lower(), "mdi-information-outline")

    def _get_msgtype(self):
        try:
            return NotificationType[self._msgtype]
        except Exception:
            return NotificationType.Manual

    @staticmethod
    def _check_apikey(apikey: str) -> bool:
        """校验 CloudDrive2 Webhook 传入的 apikey。

        使用 MoviePilot 系统 API_TOKEN 作为唯一密钥；如果 MP 未配置
        API_TOKEN，接口一律拒绝，避免空 token 被误放行。
        """
        token = getattr(settings, "API_TOKEN", "") or ""
        return bool(token) and bool(apikey) and apikey == token

    def _current_config(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "msgtype": self._msgtype,
            "detail_limit": self._detail_limit,
            "show_file_name": self._show_file_name,
            "aggregate_notify": self._aggregate_notify,
            "aggregate_wait": self._aggregate_wait,
            "history_limit": self._history_limit,
            "clear_history": False,
        }

    def _trim_history(self, save: bool = False) -> None:
        limit = max(self._history_limit, 0)
        if limit:
            self._history = self._history[:limit]
        else:
            self._history = []
        if save:
            try:
                self.save_data("history", self._history)
            except Exception as err:
                logger.warning(f"{self.plugin_name}: 保存历史记录失败 - {err}")

    def _add_history(self, record: Dict[str, Any]) -> None:
        if self._history_limit == 0:
            return
        self._history.insert(0, record)
        self._trim_history(save=True)

    def _operation_summary(self, counts: Dict[str, int], total: int) -> str:
        parts: List[str] = []
        if counts.get("create"):
            parts.append(f"新增{counts['create']}项")
        if counts.get("delete"):
            parts.append(f"删除{counts['delete']}项")
        if counts.get("move"):
            parts.append(f"移动{counts['move']}项")
        if counts.get("rename"):
            parts.append(f"重命名{counts['rename']}项")
        if counts.get("move_rename"):
            parts.append(f"移动并重命名{counts['move_rename']}项")
        if counts.get("rename_unknown"):
            parts.append(f"移动/重命名{counts['rename_unknown']}项")
        if counts.get("other"):
            parts.append(f"其他{counts['other']}项")
        return "、".join(parts) if parts else f"文件变动{total}项"

    def _build_file_message(self, payload: CD2NotifyRequest) -> Tuple[str, str, Dict[str, Any]]:
        items = payload.data or []

        counts = {
            "create": 0,
            "delete": 0,
            "move": 0,
            "rename": 0,
            "move_rename": 0,
            "rename_unknown": 0,
            "other": 0,
            "file": 0,
            "dir": 0,
        }
        detail_lines: List[str] = []
        first_action = ""
        first_type = "文件"

        for idx, item in enumerate(items):
            raw_action = self._normalize_action(item.action or "")
            src = item.source_file or ""
            dst = item.destination_file or ""
            action = self._classify_rename(src, dst) if raw_action in ("rename", "move") else raw_action
            is_dir, type_name = self._bool_text(item.is_dir)
            if idx == 0:
                first_action = action
                first_type = type_name

            if action in ("create", "delete", "move", "rename", "move_rename", "rename_unknown"):
                counts[action] += 1
            else:
                counts["other"] += 1

            if is_dir:
                counts["dir"] += 1
            else:
                counts["file"] += 1

            action_cn = self._action_text(action)
            icon = self._action_icon(action)
            if action in ("move", "rename", "move_rename", "rename_unknown") and dst:
                detail_lines.append(f"{icon} {action_cn}{type_name}：{src} → {dst}")
            else:
                detail_lines.append(f"{icon} {action_cn}{type_name}：{src}")

        total = len(items)
        operation = self._operation_summary(counts, total)
        # 通知标题只保留插件名称，具体变动放在正文统计里，避免标题和正文重复。
        title = "【☁️CloudDrive2】"

        text_lines: List[str] = []

        action_parts: List[str] = []
        if counts["create"]:
            action_parts.append(f"➕ 新增：{counts['create']} 项")
        if counts["delete"]:
            action_parts.append(f"🗑️ 删除：{counts['delete']} 项")
        if counts["move"]:
            action_parts.append(f"📦 移动：{counts['move']} 项")
        if counts["rename"]:
            action_parts.append(f"✏️ 重命名：{counts['rename']} 项")
        if counts["move_rename"]:
            action_parts.append(f"🔀 移动并重命名：{counts['move_rename']} 项")
        if counts["rename_unknown"]:
            action_parts.append(f"🔁 移动/重命名：{counts['rename_unknown']} 项")
        if counts["other"]:
            action_parts.append(f"📌 其他：{counts['other']} 项")

        type_parts: List[str] = []
        if counts["file"]:
            type_parts.append(f"📄 文件：{counts['file']} 项")
        if counts["dir"]:
            type_parts.append(f"📁 目录：{counts['dir']} 项")

        text_lines.extend([
            "━━━━━━━━━━━━━━",
            f"📊 本次变动：{total} 项",
        ])
        if action_parts:
            text_lines.append("｜".join(action_parts))
        if type_parts:
            text_lines.append("｜".join(type_parts))

        detail_preview = detail_lines
        if self._show_file_name and detail_lines:
            limit = self._detail_limit
            shown = detail_lines if limit <= 0 else detail_lines[:limit]
            if shown:
                text_lines.append("📌 详情")
                text_lines.extend(shown)
            if limit > 0 and len(detail_lines) > limit:
                text_lines.append(f"……还有 {len(detail_lines) - limit} 项未展示")

        if payload.event_time:
            text_lines.extend([
                "━━━━━━━━━━━━━━",
                f"🕐 时间：{payload.event_time}",
            ])

        summary_parts = [operation, f"文件{counts['file']}项", f"目录{counts['dir']}项"]
        if counts["other"]:
            summary_parts.append(f"其他{counts['other']}项")
        summary = "；".join(summary_parts)
        record = {
            "time": payload.event_time or payload.send_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": "文件变动",
            "title": title,
            "text": "\n".join(text_lines),
            "summary": summary,
            "action": first_action if total == 1 else "mixed",
            "action_text": self._action_text(first_action) if total == 1 else "汇总",
            "total": total,
            "create": counts["create"],
            "delete": counts["delete"],
            "move": counts["move"],
            "rename": counts["rename"],
            "move_rename": counts["move_rename"],
            "rename_unknown": counts["rename_unknown"],
            "other": counts["other"],
            "file": counts["file"],
            "dir": counts["dir"],
            "detail": "\n".join(detail_preview),
        }
        return title, "\n".join(text_lines), record

    def _build_mount_message(self, payload: CD2MountRequest) -> Tuple[str, str, Dict[str, Any]]:
        items = payload.data or []
        first = items[0] if items else None
        action = self._normalize_action((first.action if first else payload.event_name) or "")
        title = f"【☁️CloudDrive2】{self._action_text(action)}状态" if action else "【☁️CloudDrive2】挂载状态"

        text_lines: List[str] = ["━━━━━━━━━━━━━━", f"✨ 事件：{self._action_text(action) if action else '挂载状态'}"]
        detail_lines: List[str] = []
        history_detail_lines: List[str] = []

        if items:
            text_lines.extend(["━━━━━━━━━━━━━━", f"📊 本次挂载事件：{len(items)} 项"])
            for item in items:
                item_action = self._normalize_action(item.action or payload.event_name or "")
                action_cn = self._action_text(item_action)
                status_text = "成功" if str(item.status).lower() in ("true", "1", "yes") else "失败"
                reason = item.reason or ""
                mount_point = item.mount_point or ""
                full_line = f"{self._action_icon(item_action)} {action_cn}：{mount_point}，状态：{status_text}" + (f"，原因：{reason}" if reason else "")
                safe_line = f"{self._action_icon(item_action)} {action_cn}，状态：{status_text}" + (f"，原因：{reason}" if reason else "")
                history_detail_lines.append(full_line)
                detail_lines.append(full_line if self._show_file_name else safe_line)
        else:
            detail_lines.append("未收到具体挂载点信息")
            history_detail_lines.append("未收到具体挂载点信息")

        if self._show_file_name and detail_lines:
            limit = self._detail_limit
            shown = detail_lines if limit <= 0 else detail_lines[:limit]
            if shown:
                text_lines.extend(["📌 详情"])
                text_lines.extend(shown)
            if limit > 0 and len(detail_lines) > limit:
                text_lines.append(f"……还有 {len(detail_lines) - limit} 项未展示")

        if payload.event_time:
            text_lines.extend(["━━━━━━━━━━━━━━", f"🕐 时间：{payload.event_time}"])

        record = {
            "time": payload.event_time or payload.send_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": "挂载状态",
            "title": title,
            "text": "\n".join(text_lines),
            "summary": f"挂载事件{len(items)}项",
            "action": action,
            "action_text": self._action_text(action),
            "total": len(items),
            "create": 0,
            "delete": 0,
            "move": 0,
            "rename": 0,
            "move_rename": 0,
            "rename_unknown": 0,
            "other": 0,
            "file": 0,
            "dir": 0,
            "detail": "\n".join(history_detail_lines),
        }
        return title, "\n".join(text_lines), record

    def _queue_file_notify(self, payload: CD2NotifyRequest) -> None:
        """缓存 CloudDrive2 文件事件，等待一段静默时间后统一通知。"""
        if not self._pending_lock:
            self._pending_lock = Lock()

        wait_seconds = max(self._aggregate_wait, 1)
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with self._pending_lock:
            if not self._pending_file_items:
                self._pending_file_meta = {
                    "first_event_time": payload.event_time or payload.send_time or now_text,
                    "device_name": payload.device_name,
                    "user_name": payload.user_name,
                    "version": payload.version,
                    "event_category": payload.event_category,
                    "event_name": payload.event_name,
                }

            self._pending_file_items.extend(payload.data or [])
            self._pending_file_meta.update({
                "last_event_time": payload.event_time or payload.send_time or now_text,
                "last_send_time": payload.send_time or now_text,
                "device_name": payload.device_name or self._pending_file_meta.get("device_name", ""),
                "user_name": payload.user_name or self._pending_file_meta.get("user_name", ""),
                "version": payload.version or self._pending_file_meta.get("version", ""),
                "event_category": payload.event_category or self._pending_file_meta.get("event_category", ""),
                "event_name": payload.event_name or self._pending_file_meta.get("event_name", ""),
            })

            if self._pending_timer:
                try:
                    self._pending_timer.cancel()
                except Exception:
                    pass

            timer = Timer(wait_seconds, self._flush_file_notify)
            timer.daemon = True
            self._pending_timer = timer
            timer.start()

        logger.info(f"{self.plugin_name}: 已缓存CloudDrive2文件事件，等待 {wait_seconds} 秒无新变化后统一通知")

    def _flush_file_notify(self) -> None:
        """发送聚合后的 CloudDrive2 文件通知。"""
        if not self._pending_lock:
            self._pending_lock = Lock()

        with self._pending_lock:
            items = list(self._pending_file_items or [])
            meta = dict(self._pending_file_meta or {})
            self._pending_file_items = []
            self._pending_file_meta = {}
            self._pending_timer = None

        if not items:
            return

        first_time = meta.get("first_event_time") or ""
        last_time = meta.get("last_event_time") or ""
        if first_time and last_time and first_time != last_time:
            event_time = f"{first_time} ~ {last_time}"
        else:
            event_time = last_time or first_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        payload = CD2NotifyRequest(
            device_name=meta.get("device_name", ""),
            user_name=meta.get("user_name", ""),
            version=meta.get("version", ""),
            event_category=meta.get("event_category", "file"),
            event_name=meta.get("event_name", "notify"),
            event_time=event_time,
            send_time=meta.get("last_send_time", ""),
            data=items,
        )

        title, text, record = self._build_file_message(payload)
        record["category"] = "文件变动汇总"
        self._log_event("发送CloudDrive2聚合文件事件", title, text, record)
        self._add_history(record)

        if self._notify and not self._send_mp_notice(title, text):
            logger.error(f"{self.plugin_name}: 聚合通知发送失败")

    def _log_event(self, prefix: str, title: str, text: str, record: Dict[str, Any]) -> None:
        """记录插件日志。

        MP通知会按照“显示具体文件名称”开关决定是否隐藏路径；但插件日志和
        查看数据历史记录始终保留完整路径，便于自己排查 CloudDrive2 回调。
        """
        log_text = text or ""
        detail = (record or {}).get("detail") or ""
        if detail and detail not in log_text:
            log_text = f"{log_text}\n━━━━━━━━━━━━━━\n📌 历史详情（仅日志/查看数据）\n{detail}"
        logger.info(f"{prefix}:\n{title}\n{log_text}")

    def _send_mp_notice(self, title: str, text: str) -> bool:
        """发送 MP 通知。

        这里不使用 _PluginBase.post_message()，因为它会在未传 link 时
        自动补充插件详情页链接，部分通知渠道会显示“点击查看”。
        直接调用 chain.post_message 并设置 link=None，可以让通知正文不再附带插件页面链接。
        """
        try:
            mtype = self._get_msgtype()
            self.chain.post_message(Notification(mtype=mtype, title=title, text=text, link=None))
            return True
        except Exception as err:
            logger.error(f"{self.plugin_name}: 发送 MoviePilot 通知失败 - {err}")
            return False

    def file_notify(self, apikey: str, request: CD2NotifyRequest) -> schemas.Response:
        """CloudDrive2 文件系统 watcher 回调接口"""
        if not self._check_apikey(apikey):
            return schemas.Response(success=False, message="API令牌错误")
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用")
        if not request.data:
            logger.warning(f"{self.plugin_name}: 收到CloudDrive2文件事件，但data为空，已忽略")
            return schemas.Response(success=True, message="没有可处理的文件事件")

        if self._aggregate_notify:
            self._queue_file_notify(request)
            return schemas.Response(success=True, message=f"已加入汇总队列，将在 {max(self._aggregate_wait, 1)} 秒内无新变化后统一通知")

        title, text, record = self._build_file_message(request)
        self._log_event("收到CloudDrive2文件事件", title, text, record)
        self._add_history(record)

        if self._notify and not self._send_mp_notice(title, text):
            return schemas.Response(success=False, message="通知发送失败，请查看MoviePilot日志")

        return schemas.Response(success=True, message="发送成功")

    def mount_notify(self, apikey: str, request: CD2MountRequest) -> schemas.Response:
        """CloudDrive2 挂载点 watcher 回调接口"""
        if not self._check_apikey(apikey):
            return schemas.Response(success=False, message="API令牌错误")
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用")

        title, text, record = self._build_mount_message(request)
        self._log_event("收到CloudDrive2挂载事件", title, text, record)
        self._add_history(record)

        if self._notify and not self._send_mp_notice(title, text):
            return schemas.Response(success=False, message="通知发送失败，请查看MoviePilot日志")

        return schemas.Response(success=True, message="发送成功")

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/file_notify",
                "endpoint": self.file_notify,
                "methods": ["POST"],
                "summary": "CloudDrive2文件变动通知",
                "description": "接收CloudDrive2 file_system_watcher webhook并推送到MoviePilot通知渠道",
            },
            {
                "path": "/mount_notify",
                "endpoint": self.mount_notify,
                "methods": ["POST"],
                "summary": "CloudDrive2挂载状态通知",
                "description": "接收CloudDrive2 mount_point_watcher webhook并推送到MoviePilot通知渠道",
            },
        ]

    def _card_title(self, icon: str, text: str, color: str = "#16b1ff") -> Dict[str, Any]:
        return {
            "component": "VCardItem",
            "props": {"class": "px-6 pb-0"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center text-h6"},
                    "content": [
                        {
                            "component": "VIcon",
                            "props": {"style": f"color: {color};", "class": "mr-3", "size": "default"},
                            "text": icon,
                        },
                        {"component": "span", "text": text},
                    ],
                }
            ],
        }

    def _switch_col(self, model: str, label: str, color: str = "primary", cols: int = 12, sm: int = 3, hint: str = "") -> Dict[str, Any]:
        props: Dict[str, Any] = {"model": model, "label": label, "color": color, "hide-details": False if hint else True}
        if hint:
            props["hint"] = hint
            props["persistent-hint"] = True
        return {
            "component": "VCol",
            "props": {"cols": cols, "sm": sm},
            "content": [{"component": "VSwitch", "props": props}],
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        msg_type_options = []
        for item in NotificationType:
            msg_type_options.append({"title": item.value, "value": item.name})

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "flat", "class": "mb-4", "color": "surface"},
                        "content": [
                            self._card_title("mdi-cog-outline", "运行控制", "#16b1ff"),
                            {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                            {
                                "component": "VCardText",
                                "props": {"class": "px-6 pb-6"},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {"type": "info", "variant": "tonal", "class": "mb-4"},
                                        "text": "建议先开启插件和通知，再到 CloudDrive2 中配置 Webhook；批量操作建议开启汇总通知，减少刷屏。",
                                    },
                                    {
                                        "component": "VRow",
                                        "content": [
                                            self._switch_col("enabled", "启用插件", "primary", sm=4),
                                            self._switch_col("notify", "开启通知", "info", sm=4),
                                            self._switch_col(
                                                "aggregate_notify",
                                                "启用汇总通知",
                                                "success",
                                                sm=4,
                                                hint="开启后先缓存连续文件变化，等待一段时间没有新变化后统一发送一条通知。",
                                            ),
                                        ],
                                    },
                                    {
                                        "component": "VRow",
                                        "content": [
                                            self._switch_col(
                                                "show_file_name",
                                                "显示具体文件名称",
                                                "primary",
                                                sm=6,
                                                hint="开启后 MP 通知显示详情和完整路径；关闭后 MP 通知不显示详情，查看数据和插件日志仍保留完整路径。",
                                            ),
                                            self._switch_col(
                                                "clear_history",
                                                "清空历史记录",
                                                "error",
                                                sm=6,
                                                hint="打开后保存配置，会立即清空查看数据中的历史记录，并自动关闭。",
                                            ),
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "flat", "class": "mb-6", "color": "surface"},
                        "content": [
                            self._card_title("mdi-tune-variant", "通知与记录", "#4F46E5"),
                            {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                            {
                                "component": "VCardText",
                                "props": {"class": "px-6 pb-6"},
                                "content": [
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "sm": 6},
                                                "content": [
                                                    {
                                                        "component": "VSelect",
                                                        "props": {
                                                            "model": "msgtype",
                                                            "label": "消息类型",
                                                            "items": msg_type_options,
                                                            "prepend-inner-icon": "mdi-message-alert-outline",
                                                            "hint": "建议选择 Manual/手动处理，并在通知渠道中允许该类型。",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "sm": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "aggregate_wait",
                                                            "label": "汇总等待时间（秒）",
                                                            "type": "number",
                                                            "prepend-inner-icon": "mdi-timer-sand",
                                                            "hint": "建议 20 秒。比如填 20，表示 20 秒没有新文件变化后再统一通知。",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "sm": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "detail_limit",
                                                            "label": "最多显示详情条数",
                                                            "type": "number",
                                                            "prepend-inner-icon": "mdi-format-list-numbered",
                                                            "hint": "避免一次性大量文件变化刷屏，默认 10 条；填 0 表示不限制。",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "sm": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "history_limit",
                                                            "label": "历史记录保留条数",
                                                            "type": "number",
                                                            "prepend-inner-icon": "mdi-history",
                                                            "hint": "查看数据页面显示最近操作，默认 50 条；填 0 则不记录。",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "flat", "class": "mb-6", "color": "surface"},
                        "content": [
                            self._card_title("mdi-cloud-cog-outline", "CloudDrive2 Webhook 设置", "#4CAF50"),
                            {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                            {
                                "component": "VCardText",
                                "props": {"class": "px-6 pb-6"},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {"type": "info", "variant": "tonal", "class": "mb-3"},
                                        "text": "把插件包里的 cd2_webhook_template.toml 复制到 CloudDrive2 的 Webhook 配置里，只替换 MP 地址和 MP API_TOKEN。",
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {"type": "success", "variant": "tonal", "class": "mb-3"},
                                        "text": "文件变动地址：你的MP地址/api/v1/plugin/CD2Notify/file_notify?apikey=你的MP_API_TOKEN",
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {"type": "success", "variant": "tonal", "class": "mb-3"},
                                        "text": "挂载状态地址：你的MP地址/api/v1/plugin/CD2Notify/mount_notify?apikey=你的MP_API_TOKEN",
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {"type": "warning", "variant": "tonal"},
                                        "text": "注意：这两个地址填在 CloudDrive2 Webhook 里，不要填到 MP 通知渠道里。保存后建议重启 CloudDrive2，再新建/删除/重命名文件夹测试。",
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "flat", "class": "mb-6", "color": "surface"},
                        "content": [
                            self._card_title("mdi-information-outline", "使用说明", "#9C27B0"),
                            {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                            {
                                "component": "VCardText",
                                "props": {"class": "px-6 py-0"},
                                "content": [
                                    {
                                        "component": "VList",
                                        "props": {"lines": "two", "density": "comfortable"},
                                        "content": [
                                            self._usage_item("mdi-plus-circle-outline", "新增 / 删除 / 移动 / 重命名", "自动识别 CloudDrive2 的 create、delete、rename 动作；rename 会按路径判断为移动、重命名或移动并重命名。", "success"),
                                            self._usage_item("mdi-timer-sand", "启用汇总通知", "开启后会把连续文件变化合并成一条通知，适合一次性复制、删除或移动很多文件。", "success"),
                                            self._usage_item("mdi-eye-off-outline", "显示具体文件名称", "开启后MP通知显示详情和完整路径；关闭后MP通知不显示详情，查看数据和插件日志仍保留完整路径。", "warning"),
                                            self._usage_item("mdi-history", "查看数据", "插件详情页展示最近的 CloudDrive2 操作历史，方便排查是否收到回调。", "info"),
                                            self._usage_item("mdi-content-copy", "复制说明", "CloudDrive2 没有 copy 动作，复制文件通常会被识别为新增。", "primary"),
                                        ],
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": True,
            "notify": True,
            "msgtype": "Manual",
            "detail_limit": 10,
            "show_file_name": True,
            "aggregate_notify": False,
            "aggregate_wait": 20,
            "history_limit": 50,
            "clear_history": False,
        }

    def _usage_item(self, icon: str, title: str, text: str, color: str) -> Dict[str, Any]:
        return {
            "component": "VListItem",
            "props": {"lines": "two"},
            "content": [
                {
                    "component": "div",
                    "props": {"class": "d-flex align-items-start"},
                    "content": [
                        {"component": "VIcon", "props": {"color": color, "class": "mt-1 mr-2"}, "text": icon},
                        {"component": "div", "props": {"class": "text-subtitle-1 font-weight-regular mb-1"}, "text": title},
                    ],
                },
                {"component": "div", "props": {"class": "text-body-2 ml-8"}, "text": text},
            ],
        }

    def _stat_card(self, title: str, value: str, color: str, icon: str) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": 12, "sm": 6, "md": 3},
            "content": [
                {
                    "component": "VCard",
                    "props": {"variant": "tonal", "color": color, "class": "h-100"},
                    "content": [
                        {
                            "component": "VCardText",
                            "props": {"class": "d-flex align-center"},
                            "content": [
                                {"component": "VIcon", "props": {"size": "large", "class": "mr-3"}, "text": icon},
                                {
                                    "component": "div",
                                    "content": [
                                        {"component": "div", "props": {"class": "text-caption"}, "text": title},
                                        {"component": "div", "props": {"class": "text-h6 font-weight-bold"}, "text": value},
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ],
        }

    def get_page(self) -> List[dict]:
        history = self._history[: max(self._history_limit, 0)] if self._history_limit else []
        latest = history[0] if history else {}
        enabled_text = "已启用" if self._enabled else "未启用"
        notify_text = "已开启" if self._notify else "未开启"
        latest_text = latest.get("title") or "暂无记录"
        aggregate_text = f"开启，静默{max(self._aggregate_wait, 1)}秒" if self._aggregate_notify else "未开启"

        rows: List[dict] = []
        for item in history:
            action = item.get("action") or ""
            action_text = item.get("action_text") or ("汇总" if action == "mixed" else self._action_text(action))
            color = "secondary" if action == "mixed" else self._action_color(action)
            icon = "mdi-view-dashboard-outline" if action == "mixed" else self._action_mdi(action)
            detail_text = item.get("summary") or ""
            if item.get("detail"):
                detail_text = f"{detail_text}\n{item.get('detail')}" if detail_text else item.get("detail")
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "props": {"class": "text-no-wrap"}, "text": item.get("time", "")},
                    {"component": "td", "text": item.get("category", "")},
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {"color": color, "size": "small", "variant": "tonal"},
                                "content": [
                                    {"component": "VIcon", "props": {"size": "small", "start": True}, "text": icon},
                                    {"component": "span", "text": action_text},
                                ],
                            }
                        ],
                    },
                    {"component": "td", "props": {"class": "text-center"}, "text": str(item.get("total", "-"))},
                    {"component": "td", "props": {"style": "white-space: pre-line;"}, "text": detail_text or item.get("title", "")},
                ],
            })

        if not rows:
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "props": {"colspan": 5, "class": "text-center text-medium-emphasis"}, "text": "暂无历史操作记录，请先在 CloudDrive2 中新建、删除或重命名一个文件夹测试。"}
                ],
            })

        return [
            {
                "component": "VRow",
                "content": [
                    self._stat_card("插件状态", enabled_text, "success" if self._enabled else "grey", "mdi-power"),
                    self._stat_card("通知状态", notify_text, "info" if self._notify else "grey", "mdi-bell-ring-outline"),
                    self._stat_card("汇总通知", aggregate_text, "success" if self._aggregate_notify else "grey", "mdi-timer-sand"),
                    self._stat_card("历史记录", f"{len(history)} 条", "primary", "mdi-history"),
                    self._stat_card("最近操作", latest_text, "warning" if latest else "grey", "mdi-clock-outline"),
                ],
            },
            {
                "component": "VCard",
                "props": {"variant": "flat", "class": "mt-6", "color": "surface"},
                "content": [
                    self._card_title("mdi-format-list-bulleted", "最近操作历史", "#9C27B0"),
                    {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                    {
                        "component": "VCardText",
                        "props": {"class": "px-6 pb-6"},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {"type": "info", "variant": "tonal", "class": "mb-4"},
                                "text": f"最多保留 {self._history_limit} 条历史。开启汇总通知后，会在连续变化结束后记录一条汇总；关闭“显示具体文件名称”只影响MP通知；这里和插件日志仍会展示完整路径，方便排查。",
                            },
                            {
                                "component": "VTable",
                                "props": {"density": "comfortable", "hover": True},
                                "content": [
                                    {
                                        "component": "thead",
                                        "content": [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {"component": "th", "text": "时间"},
                                                    {"component": "th", "text": "类型"},
                                                    {"component": "th", "text": "操作"},
                                                    {"component": "th", "props": {"class": "text-center"}, "text": "数量"},
                                                    {"component": "th", "text": "说明"},
                                                ],
                                            }
                                        ],
                                    },
                                    {"component": "tbody", "content": rows},
                                ],
                            },
                        ],
                    },
                ],
            },
        ]

    def stop_service(self):
        try:
            if self._pending_timer:
                self._pending_timer.cancel()
        except Exception:
            pass

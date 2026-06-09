"""
MoviePilot V2 插件：聚影签到

接口来源：用户提供的青龙脚本。
登录接口：/api/app/login/
签到接口：/api/app/checkin/do/
认证请求头：x-app-user-token

作者：jidian
说明：感谢大胖提供的支持。
"""

import datetime
import json
import os
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase

try:
    from app.schemas import Notification, NotificationType
except Exception:  # 兼容极少数旧版本或测试环境
    Notification = None
    NotificationType = None


class JuyingCheckin(_PluginBase):
    """聚影自动签到插件。"""

    plugin_name = "聚影签到"
    plugin_desc = "用于聚影自动签到，支持账号密码、多账号、定时执行、代理、失败重试和签到历史。感谢大胖提供的支持。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/signin.png"
    plugin_version = "1.1"
    plugin_author = "jidian"
    author_url = "https://share.huamucang.top"
    plugin_config_prefix = "juyingcheckin_"
    plugin_order = 66
    auth_level = 1

    _enabled: bool = False
    _notify: bool = True
    _onlyonce: bool = False
    _cron: str = "10 8 * * *"
    _site_url: str = "https://share.huamucang.top"
    _login_api: str = "/api/app/login/"
    _checkin_api: str = "/api/app/checkin/do/"
    _username: str = ""
    _password: str = ""
    _accounts: str = ""
    _use_proxy: bool = True
    _notify_type: str = "Plugin"
    _history_count: int = 30
    _random_time_range: str = ""
    _retry_times: int = 2
    _retry_interval: int = 10
    _connect_timeout: int = 10
    _read_timeout: int = 60
    _clear_history: bool = False
    _check_proxy: bool = False
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        """读取配置并初始化插件。"""
        self.stop_service()
        config = config or {}

        self._enabled = self.__safe_bool(config.get("enabled", False))
        self._notify = self.__safe_bool(config.get("notify", True))
        self._onlyonce = self.__safe_bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "10 8 * * *").strip()
        self._site_url = str(config.get("site_url") or "https://share.huamucang.top").strip().rstrip("/")
        self._login_api = str(config.get("login_api") or "/api/app/login/").strip()
        self._checkin_api = str(config.get("checkin_api") or "/api/app/checkin/do/").strip()
        self._username = str(config.get("username") or "").strip()
        self._password = str(config.get("password") or config.get("usr_password") or "").strip()
        self._accounts = str(config.get("accounts") or "").strip()
        self._use_proxy = self.__safe_bool(config.get("use_proxy", True))
        self._notify_type = str(config.get("notify_type") or "Plugin").strip()
        self._history_count = self.__safe_int(config.get("history_count"), default=30, minimum=1, maximum=200)
        self._random_time_range = str(config.get("random_time_range") or "").strip()
        self._retry_times = self.__safe_int(config.get("retry_times"), default=2, minimum=0, maximum=10)
        self._retry_interval = self.__safe_int(config.get("retry_interval"), default=10, minimum=0, maximum=600)
        self._clear_history = self.__safe_bool(config.get("clear_history", False))
        self._check_proxy = self.__safe_bool(config.get("check_proxy", False))

        # 兼容旧版 timeout：旧配置只有一个 timeout 时，作为 read_timeout 使用。
        old_timeout = config.get("timeout")
        self._connect_timeout = self.__safe_int(config.get("connect_timeout"), default=10, minimum=3, maximum=120)
        self._read_timeout = self.__safe_int(
            config.get("read_timeout", old_timeout), default=60, minimum=5, maximum=300
        )

        need_save = False
        if self._clear_history:
            self.save_data("history", [])
            self.save_data("latest_result", {})
            logger.info("聚影签到：已清空签到历史记录")
            self._clear_history = False
            need_save = True

        if self._check_proxy:
            self.__check_proxy_status()
            self._check_proxy = False
            need_save = True

        if need_save:
            self.__save_config()

        if self._onlyonce:
            logger.info("聚影签到：收到保存后运行一次请求")
            self._onlyonce = False
            self.__save_config()
            tz = self.__get_timezone()
            self._scheduler = BackgroundScheduler(timezone=tz)
            self._scheduler.add_job(
                func=self.checkin,
                trigger="date",
                run_date=datetime.datetime.now(tz=tz) + datetime.timedelta(seconds=3),
                name="聚影签到立即运行",
            )
            self._scheduler.start()

        if self._enabled and self._cron:
            logger.info(f"聚影签到：插件已启用，定时周期 {self._cron}，下次运行 {self.__next_run_time()}")
        elif self._enabled:
            logger.warning("聚影签到：插件已启用，但 Cron 表达式为空，不会自动定时签到")

    def get_state(self) -> bool:
        return bool(self._enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """注册 MoviePilot 公共定时服务。"""
        if not self._enabled:
            return []
        if not self._cron:
            logger.warning("聚影签到：Cron 表达式为空，未注册定时服务")
            return []
        try:
            tz = self.__get_timezone()
            # 明确指定时区，避免部分环境下按 UTC 触发，导致 08:10 实际变成 16:10。
            trigger = CronTrigger.from_crontab(self._cron, timezone=tz)
        except Exception as err:
            logger.error(f"聚影签到：cron 表达式不正确：{self._cron}，错误：{err}")
            return []
        logger.info(f"聚影签到：已注册定时服务，cron={self._cron}，timezone={tz.zone}")
        return [
            {
                # 使用稳定的小写服务 ID，避免包含点号/大写导致部分版本服务列表或调度器识别异常。
                "id": "juyingcheckin",
                "name": "聚影签到定时服务",
                "trigger": trigger,
                "func": self._scheduled_checkin,
                "kwargs": {},
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置页 JSON 和默认配置模型。"""
        return [
            {
                "component": "VForm",
                "content": [
                    self.__form_basic_card(),
                    self.__form_account_card(),
                    self.__form_schedule_card(),
                    self.__form_advanced_card(),
                    self.__form_help_card(),
                ],
            }
        ], self.__default_config()

    def get_page(self) -> List[dict]:
        """插件详情页，展示设置状态、用户信息和最近签到历史。"""
        latest = self.get_data("latest_result") or {}
        profile = self.get_data("profile") or {}
        history = self.get_data("history") or []
        accounts_count = len(self.__get_accounts(silent=True))
        configured = accounts_count > 0

        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [self.__page_status_card(latest, configured, accounts_count)],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [self.__page_profile_card(profile, latest)],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [self.__page_history_card(history)],
                    },
                ],
            }
        ]

    def stop_service(self):
        """停用插件时清理即时调度器。"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:
            logger.error(f"聚影签到：停止服务失败：{err}")

    def _scheduled_checkin(self):
        """定时任务入口；按配置执行随机延迟。"""
        delay_minutes = self.__parse_random_delay_minutes()
        if delay_minutes > 0:
            logger.info(f"聚影签到：随机延迟 {delay_minutes} 分钟后执行")
            time.sleep(delay_minutes * 60)
        self.checkin()

    def checkin(self):
        """执行聚影签到。"""
        accounts = self.__get_accounts()
        if not accounts:
            msg = "未配置账号密码，请在插件配置中填写用户名和密码，或在多账号中填写：账号#密码。"
            logger.warning(f"聚影签到：{msg}")
            title = "聚影签到｜未配置账号"
            self.__save_run_result(title=title, text=msg, results=[])
            if self._notify:
                self.__post_notify(title, msg)
            return

        results: List[Dict[str, Any]] = []
        success_count = 0
        already_count = 0
        fail_count = 0

        for account in accounts:
            username = account.get("username") or ""
            password = account.get("password") or ""
            result = self.__login_and_checkin_with_retry(username=username, password=password)
            results.append({"username": username, **result})

            if result.get("success"):
                success_count += 1
            elif result.get("already"):
                already_count += 1
            else:
                fail_count += 1

            user_info = result.get("user") or {}
            if user_info:
                # 登录返回的是签到前的用户信息；签到接口返回的累计天数/积分更接近最新状态。
                user_info = dict(user_info)
                if result.get("days") not in [None, ""]:
                    user_info["checkin_days"] = result.get("days")
                if result.get("points") not in [None, ""] and result.get("success"):
                    try:
                        user_info["points_awarded_latest"] = result.get("points")
                    except Exception:
                        pass
                self.save_data("profile", user_info)

        total = len(accounts)
        title = self.__build_notify_title(
            total=total,
            success=success_count,
            already=already_count,
            fail=fail_count,
        )

        text = self.__format_notify_text(
            total=total,
            success=success_count,
            already=already_count,
            fail=fail_count,
            results=results,
        )
        logger.info(f"聚影签到：{title}\n{text}")
        self.__save_run_result(title=title, text=text, results=results)
        if self._notify:
            self.__post_notify(title, text)

    def __login_and_checkin_with_retry(self, username: str, password: str) -> Dict[str, Any]:
        """执行单账号签到；只对网络异常、超时、限流或服务端错误重试。"""
        max_attempts = max(1, int(self._retry_times) + 1)
        last_result: Dict[str, Any] = {
            "success": False,
            "already": False,
            "message": "未执行签到",
            "points": 0,
            "days": 0,
            "raw": {},
            "user": {},
            "proxy": False,
            "attempt": 0,
            "max_attempts": max_attempts,
            "retryable": False,
        }

        for attempt in range(1, max_attempts + 1):
            try:
                result = self.__login_and_checkin(username=username, password=password)
            except Exception as err:
                logger.error(f"聚影签到：账号 {self.__mask_account(username)} 第 {attempt}/{max_attempts} 次执行失败：{err}")
                result = {
                    "success": False,
                    "already": False,
                    "message": self.__friendly_error(err),
                    "points": 0,
                    "days": 0,
                    "raw": {},
                    "user": {},
                    "proxy": bool(self._use_proxy),
                    "retryable": True,
                }

            result["attempt"] = attempt
            result["max_attempts"] = max_attempts
            if "retryable" not in result:
                result["retryable"] = self.__is_retryable_result(result)
            last_result = result

            if result.get("success") or result.get("already"):
                return result

            if not result.get("retryable"):
                logger.warning(
                    f"聚影签到：账号 {self.__mask_account(username)} 返回明确失败，不再重试。原因：{result.get('message') or '未知错误'}"
                )
                return result

            if attempt < max_attempts:
                logger.warning(
                    f"聚影签到：账号 {self.__mask_account(username)} 签到失败，{self._retry_interval} 秒后重试 "
                    f"{attempt + 1}/{max_attempts}。原因：{result.get('message') or '未知错误'}"
                )
                if self._retry_interval > 0:
                    time.sleep(self._retry_interval)

        return last_result

    def __login_and_checkin(self, username: str, password: str) -> Dict[str, Any]:
        """使用账号密码登录，再用 token 签到。"""
        login_url = self.__build_url(self._login_api)
        checkin_url = self.__build_url(self._checkin_api)
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        }

        logger.info(f"聚影签到：正在登录账号 {self.__mask_account(username)}")
        login_resp, login_used_proxy = self.__request(
            session=session,
            method="post",
            url=login_url,
            json={"username": username, "password": password},
            headers=headers,
            timeout=(self._connect_timeout, self._read_timeout),
        )
        login_data = self.__response_to_json(login_resp)
        user_info = login_data.get("user") if isinstance(login_data.get("user"), dict) else {}

        if not self.__is_login_success(login_data):
            return {
                "success": False,
                "already": False,
                "message": f"登录失败：{self.__pick_message(login_data)}",
                "points": 0,
                "days": 0,
                "raw": login_data,
                "user": user_info,
                "proxy": bool(login_used_proxy),
                "retryable": self.__is_retryable_payload(login_data),
            }

        token = self.__pick_token(login_data)
        if not token:
            return {
                "success": False,
                "already": False,
                "message": "登录成功但未获取到 token",
                "points": 0,
                "days": int(user_info.get("checkin_days") or 0),
                "raw": login_data,
                "user": user_info,
                "proxy": bool(login_used_proxy),
                "retryable": False,
            }

        checkin_headers = dict(headers)
        checkin_headers["x-app-user-token"] = token
        logger.info(f"聚影签到：账号 {self.__mask_account(username)} 登录成功，开始签到")
        checkin_resp, checkin_used_proxy = self.__request(
            session=session,
            method="post",
            url=checkin_url,
            headers=checkin_headers,
            timeout=(self._connect_timeout, self._read_timeout),
        )
        checkin_data = self.__response_to_json(checkin_resp)
        used_proxy = bool(login_used_proxy or checkin_used_proxy)

        message = self.__pick_message(checkin_data)
        status = str(checkin_data.get("status") or checkin_data.get("code") or "").lower()
        already = any(key in message for key in ["已签到", "已经签到", "今日已", "重复签到", "明天再来"])
        success = status == "success" or checkin_data.get("success") is True
        # 成功优先；如果接口把“已签到”放在 400 JSON 中返回，则按已签到处理，不触发重试。
        if success:
            already = False
        points = self.__first_not_none(checkin_data, ["points_awarded", "points"], default=0)
        days = self.__first_not_none(
            checkin_data,
            ["my_total_days", "total_days"],
            default=user_info.get("checkin_days") if isinstance(user_info, dict) else 0,
        )

        return {
            "success": bool(success),
            "already": bool(already),
            "message": message or ("签到成功" if success else "今日已签到" if already else "签到失败"),
            "points": points,
            "days": days,
            "raw": checkin_data,
            "user": user_info,
            "proxy": used_proxy,
            "retryable": False if (success or already) else self.__is_retryable_payload(checkin_data),
        }

    def __get_accounts(self, silent: bool = False) -> List[Dict[str, str]]:
        """解析单账号、多账号和青龙变量格式。"""
        accounts: List[Dict[str, str]] = []

        if self._username:
            accounts.append({"username": self._username, "password": self._password})

        raw = self.__extract_ql_value(self._accounts)
        if raw:
            raw = raw.replace("\r", "\n")
            parts: List[str] = []
            for line in raw.split("\n"):
                line = line.strip().strip("'").strip('"')
                if not line:
                    continue
                parts.extend([item.strip() for item in line.split("@") if item.strip()])

            for item in parts:
                username = ""
                password = ""
                if "#" in item:
                    username, password = item.split("#", 1)
                elif "|" in item:
                    username, password = item.split("|", 1)
                elif "," in item:
                    username, password = item.split(",", 1)
                else:
                    if not silent:
                        logger.warning(f"聚影签到：忽略格式不正确的账号配置：{item}")
                    continue
                username = username.strip()
                password = password.strip()
                if username:
                    accounts.append({"username": username, "password": password})

        deduped: List[Dict[str, str]] = []
        seen = set()
        for account in accounts:
            key = f"{account['username']}#{account.get('password', '')}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(account)
        return deduped

    @staticmethod
    def __extract_ql_value(raw: str) -> str:
        raw = (raw or "").strip()
        if not raw:
            return ""
        match = re.search(r"JUYING_ACCOUNT\s*=\s*['\"]?(.+?)['\"]?\s*$", raw, flags=re.I | re.S)
        if match:
            return match.group(1).strip()
        return raw

    def __build_url(self, api_path: str) -> str:
        api_path = (api_path or "").strip()
        if api_path.startswith("http://") or api_path.startswith("https://"):
            return api_path
        return urljoin(self._site_url.rstrip("/") + "/", api_path.lstrip("/"))

    def __request(self, session: requests.Session, method: str, url: str, **kwargs) -> Tuple[requests.Response, bool]:
        """按配置直连或使用 MoviePilot 的 PROXY_HOST 代理请求。"""
        if self._use_proxy:
            proxy_host = self.__get_mp_proxy_host()
            if not proxy_host:
                raise RuntimeError("已开启使用代理，但未检测到 PROXY_HOST。")
            proxies = {"http": proxy_host, "https": proxy_host}
            try:
                logger.info(f"聚影签到：使用 MP 代理请求：{url}")
                return session.request(method=method, url=url, proxies=proxies, **kwargs), True
            except requests.exceptions.RequestException as proxy_err:
                raise RuntimeError(f"MP代理请求失败：{proxy_err}") from proxy_err

        try:
            return session.request(method=method, url=url, **kwargs), False
        except requests.exceptions.RequestException as direct_err:
            raise RuntimeError(f"直连请求失败：{direct_err}") from direct_err

    @staticmethod
    def __get_mp_proxy_host() -> str:
        proxy_host = getattr(settings, "PROXY_HOST", None) or os.environ.get("PROXY_HOST") or ""
        proxy_host = str(proxy_host).strip()
        if proxy_host.lower() in ["", "none", "null", "false"]:
            return ""
        return proxy_host

    @staticmethod
    def __get_timezone():
        """获取 MoviePilot 时区；配置缺失或不合法时回退到 Asia/Shanghai。"""
        tz_name = getattr(settings, "TZ", None) or os.environ.get("TZ") or "Asia/Shanghai"
        try:
            return pytz.timezone(str(tz_name))
        except Exception:
            logger.warning(f"聚影签到：时区配置不正确：{tz_name}，已回退到 Asia/Shanghai")
            return pytz.timezone("Asia/Shanghai")

    @staticmethod
    def __response_to_json(resp: requests.Response) -> Dict[str, Any]:
        """把响应转换为 JSON；即使 HTTP 400/500，只要返回 JSON 也优先解析。"""
        if resp is None:
            return {"status": "http_error", "message": "响应为空"}

        try:
            data = resp.json()
            if isinstance(data, dict):
                if not resp.ok:
                    data.setdefault("_http_status", resp.status_code)
                return data
            return {"status": "not_dict", "message": json.dumps(data, ensure_ascii=False)[:500]}
        except ValueError:
            pass

        try:
            resp.raise_for_status()
        except Exception as err:
            text = resp.text[:500] if resp.text else ""
            return {"status": "http_error", "message": f"{err} {text}"}
        return {"status": "not_json", "message": resp.text[:500]}

    @staticmethod
    def __is_login_success(data: Dict[str, Any]) -> bool:
        if not isinstance(data, dict):
            return False
        if data.get("status") == "success":
            return True
        if data.get("success") is True:
            return True
        if data.get("code") in [0, 200, "0", "200"] and JuyingCheckin.__pick_token(data):
            return True
        return bool(JuyingCheckin.__pick_token(data))

    @staticmethod
    def __pick_token(data: Dict[str, Any]) -> str:
        if not isinstance(data, dict):
            return ""
        token = data.get("token") or data.get("access_token")
        if token:
            return str(token)
        inner = data.get("data") or data.get("result") or {}
        if isinstance(inner, dict):
            token = inner.get("token") or inner.get("access_token")
            if token:
                return str(token)
        return ""

    @staticmethod
    def __pick_message(data: Dict[str, Any]) -> str:
        if not isinstance(data, dict):
            return str(data)
        for key in ["message", "msg", "detail", "error"]:
            if data.get(key):
                return str(data.get(key))
        return json.dumps(data, ensure_ascii=False)[:300]

    @staticmethod
    def __first_not_none(data: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
        """按顺序取第一个存在且不为 None/空字符串的字段，避免 0 被误判为空。"""
        if not isinstance(data, dict):
            return default
        for key in keys:
            if key in data and data.get(key) not in [None, ""]:
                return data.get(key)
        return default


    @staticmethod
    def __is_retryable_payload(data: Dict[str, Any]) -> bool:
        """只对临时性问题重试：限流、5xx、网关/超时等。"""
        if not isinstance(data, dict):
            return False
        try:
            http_status = int(data.get("_http_status") or 0)
        except Exception:
            http_status = 0
        if http_status in [408, 429, 500, 502, 503, 504]:
            return True
        message = str(data.get("message") or data.get("msg") or data.get("error") or "")
        return any(key in message for key in ["超时", "timeout", "temporarily", "临时", "服务不可用", "网关", "重试"])

    @staticmethod
    def __is_retryable_result(result: Dict[str, Any]) -> bool:
        if not isinstance(result, dict):
            return False
        if result.get("success") or result.get("already"):
            return False
        if result.get("retryable") is not None:
            return bool(result.get("retryable"))
        raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
        if JuyingCheckin.__is_retryable_payload(raw):
            return True
        message = str(result.get("message") or "")
        return any(key in message for key in ["网络异常", "请求超时", "连接被重置", "DNS", "代理请求失败"])

    @staticmethod
    def __build_notify_title(total: int, success: int, already: int, fail: int) -> str:
        if fail:
            return "聚影签到失败" if total <= 1 else f"聚影签到失败｜{fail}/{total}"
        if success:
            return "聚影签到成功" if total <= 1 else f"聚影签到成功｜{success}/{total}"
        if already:
            return "聚影今日已签到" if total <= 1 else f"聚影今日已签到｜{already}/{total}"
        return "聚影签到完成"

    def __format_notify_text(self, total: int, success: int, already: int, fail: int, results: List[Dict[str, Any]]) -> str:
        """按最终状态只展示最需要关注的账号：失败只列失败，成功只列成功，已签只列已签。"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if fail:
            display_results = [item for item in results if not item.get("success") and not item.get("already")]
            header = f"🔴 签到失败：{fail}/{total}" if total > 1 else "🔴 签到失败"
        elif success:
            display_results = [item for item in results if item.get("success")]
            header = f"🟢 签到成功：{success}/{total}" if total > 1 else "🟢 签到成功"
        else:
            display_results = [item for item in results if item.get("already")]
            header = f"🟡 今日已签到：{already}/{total}" if total > 1 else "🟡 今日已签到"

        lines = [header, f"⏰ {now}"]
        for result in display_results:
            lines.append("")
            lines.extend(self.__format_result_lines(result))
        return "\n".join(lines)

    @staticmethod
    def __format_result_lines(result: Dict[str, Any]) -> List[str]:
        status_text, status_icon, _ = JuyingCheckin.__status_meta(result)
        account = JuyingCheckin.__mask_account(result.get("username") or "")
        message = str(result.get("message") or "无返回信息").strip()
        points = result.get("points", 0)
        days = result.get("days", 0)
        network = "代理" if result.get("proxy") else "直连"
        attempt = int(result.get("attempt") or 1)
        max_attempts = int(result.get("max_attempts") or attempt)

        lines = [f"{status_icon} {account}｜{status_text}"]
        if result.get("success"):
            lines.append(f"积分 +{points}｜累计 {days}天")
        elif result.get("already"):
            lines.append(f"累计 {days}天")
        else:
            if max_attempts > 1:
                lines.append(f"网络：{network}｜尝试 {attempt}/{max_attempts} 次")
            else:
                lines.append(f"网络：{network}")
        if message:
            lines.append(message)
        return lines

    @staticmethod
    def __status_meta(result: Dict[str, Any]) -> Tuple[str, str, str]:
        if result.get("success"):
            return "签到成功", "🟢", "success"
        if result.get("already"):
            return "今日已签到", "🟡", "warning"
        return "签到失败", "🔴", "error"

    @staticmethod
    def __mask_account(username: str) -> str:
        username = str(username or "").strip()
        if not username:
            return "未知账号"
        if "@" in username:
            name, domain = username.split("@", 1)
            if len(name) <= 3:
                masked = name[:1] + "***"
            else:
                masked = name[:3] + "***" + name[-2:]
            return f"{masked}@{domain}"
        if len(username) <= 4:
            return username[:1] + "***"
        return username[:3] + "***" + username[-2:]

    @staticmethod
    def __friendly_error(err: Exception) -> str:
        text = str(err)
        if "Connection reset" in text or "ConnectionResetError" in text or "reset by peer" in text:
            return "网络异常：连接被重置。建议开启“使用代理”，并确认 MP 的 PROXY_HOST 可用。"
        if "timed out" in text or "Read timed out" in text or "ConnectTimeout" in text:
            return "网络异常：请求超时。建议开启“使用代理”，或适当调大读取超时、重试次数和重试间隔。"
        if "NameResolutionError" in text or "Failed to resolve" in text or "Temporary failure in name resolution" in text:
            return "网络异常：DNS 解析失败。建议检查容器 DNS，或开启 MP 代理后重试。"
        if "MP代理请求失败" in text:
            return f"网络异常：MP 代理请求失败。详情：{text}"
        if "未检测到 PROXY_HOST" in text or "PROXY_HOST" in text:
            return f"网络异常：已开启使用代理，但未检测到可用 PROXY_HOST。详情：{text}"
        return f"请求异常：{text}"

    def __post_notify(self, title: str, text: str):
        """发送 MoviePilot 通知。

        注意：_PluginBase.post_message 在 link 为空时会自动补插件详情页链接。
        为了保留“插件消息”类别且不附加“点击查看”链接，这里直接调用
        chain.post_message 并传入 link=None 的 Notification。
        """
        try:
            mtype = self.__get_notification_type(self._notify_type)
            if Notification is not None:
                self.chain.post_message(Notification(mtype=mtype, title=title, text=text, link=None))
            elif mtype:
                self.post_message(mtype=mtype, title=title, text=text)
            else:
                self.post_message(title=title, text=text)
        except Exception as err:
            logger.error(f"聚影签到：发送通知失败：{err}")

    @staticmethod
    def __get_notification_type(notify_type: str):
        if NotificationType is None:
            return None
        key = str(notify_type or "Plugin").strip()
        mapping = {
            "插件消息": "Plugin",
            "站点消息": "SiteMessage",
            "资源下载": "Download",
            "整理入库": "Organize",
            "订阅": "Subscribe",
            "媒体服务器通知": "MediaServer",
            "手动处理通知": "Manual",
            "手动处理": "Manual",
            "站点": "SiteMessage",
            "插件": "Plugin",
            "Plugin": "Plugin",
            "SiteMessage": "SiteMessage",
            "Download": "Download",
            "Organize": "Organize",
            "Subscribe": "Subscribe",
            "MediaServer": "MediaServer",
            "Manual": "Manual",
        }
        enum_name = mapping.get(key, "Plugin")
        if hasattr(NotificationType, enum_name):
            return getattr(NotificationType, enum_name)
        return getattr(NotificationType, "SiteMessage", None)

    def __save_run_result(self, title: str, text: str, results: List[Dict[str, Any]]):
        """保存最近签到结果和历史表格数据。"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            history = self.get_data("history") or []
            for result in results:
                status_text, _, status_color = self.__status_meta(result)
                history.append(
                    {
                        "timestamp": now,
                        "username": self.__mask_account(result.get("username") or ""),
                        "status_text": status_text,
                        "status_color": status_color,
                        "success": bool(result.get("success")),
                        "already": bool(result.get("already")),
                        "checkin_days": result.get("days", 0),
                        "points_awarded": result.get("points", 0),
                        "attempt": result.get("attempt", 1),
                        "max_attempts": result.get("max_attempts", 1),
                        "is_retry_task": int(result.get("attempt") or 1) > 1,
                        "network": "代理" if result.get("proxy") else "直连",
                        "message": result.get("message") or "--",
                    }
                )
            if not results:
                history.append(
                    {
                        "timestamp": now,
                        "username": "--",
                        "status_text": "待配置",
                        "status_color": "warning",
                        "success": False,
                        "already": False,
                        "checkin_days": "--",
                        "points_awarded": "--",
                        "attempt": 0,
                        "max_attempts": 0,
                        "is_retry_task": False,
                        "network": "--",
                        "message": text,
                    }
                )

            self.save_data("history", history[-self._history_count:])
            self.save_data(
                "latest_result",
                {
                    "timestamp": now,
                    "title": title,
                    "text": text,
                    "message": self.__compact_message(text),
                    "success": bool(results) and all(r.get("success") or r.get("already") for r in results),
                    "has_fail": any(not (r.get("success") or r.get("already")) for r in results),
                    "total": len(results),
                    "ok": sum(1 for r in results if r.get("success") or r.get("already")),
                },
            )
        except Exception as err:
            logger.error(f"聚影签到：保存历史记录失败：{err}")

    @staticmethod
    def __compact_message(text: str) -> str:
        text = str(text or "").replace("\n", " ").strip()
        return text[:80] + ("..." if len(text) > 80 else "")

    def __save_config(self):
        self.update_config(self.__current_config())

    def __current_config(self) -> Dict[str, Any]:
        data = self.__default_config()
        data.update(
            {
                "enabled": self._enabled,
                "notify": self._notify,
                "onlyonce": self._onlyonce,
                "cron": self._cron,
                "site_url": self._site_url,
                "login_api": self._login_api,
                "checkin_api": self._checkin_api,
                "username": self._username,
                "password": self._password,
                "accounts": self._accounts,
                "use_proxy": self._use_proxy,
                "notify_type": self._notify_type,
                "history_count": self._history_count,
                "random_time_range": self._random_time_range,
                "retry_times": self._retry_times,
                "retry_interval": self._retry_interval,
                "connect_timeout": self._connect_timeout,
                "read_timeout": self._read_timeout,
                "clear_history": self._clear_history,
                "check_proxy": self._check_proxy,
            }
        )
        return data

    @staticmethod
    def __default_config() -> Dict[str, Any]:
        return {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cron": "10 8 * * *",
            "site_url": "https://share.huamucang.top",
            "login_api": "/api/app/login/",
            "checkin_api": "/api/app/checkin/do/",
            "username": "",
            "password": "",
            "accounts": "",
            "use_proxy": True,
            "notify_type": "Plugin",
            "history_count": 30,
            "random_time_range": "",
            "retry_times": 2,
            "retry_interval": 10,
            "connect_timeout": 10,
            "read_timeout": 60,
            "clear_history": False,
            "check_proxy": False,
        }

    def __parse_random_delay_minutes(self) -> int:
        raw = (self._random_time_range or "").strip()
        if not raw:
            return 0
        try:
            if "-" in raw:
                start, end = raw.split("-", 1)
                low = max(0, int(start.strip()))
                high = max(low, int(end.strip()))
                return random.randint(low, high)
            high = max(0, int(raw))
            return random.randint(0, high) if high > 0 else 0
        except Exception:
            logger.warning(f"聚影签到：随机时间范围格式不正确：{raw}，本次不延迟")
            return 0


    def __next_run_time(self) -> str:
        """计算下一次定时运行时间，仅用于详情页展示。"""
        if not self._enabled or not self._cron:
            return "--"
        try:
            tz = self.__get_timezone()
            trigger = CronTrigger.from_crontab(self._cron, timezone=tz)
            now = datetime.datetime.now(tz)
            next_dt = trigger.get_next_fire_time(None, now)
            if not next_dt:
                return "--"
            return next_dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as err:
            logger.warning(f"聚影签到：计算下次运行时间失败：{err}")
            return "Cron 异常"

    def __check_proxy_status(self):
        """保存后手动检测代理和站点连通性。"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = {
            "timestamp": now,
            "ok": False,
            "message": "未检测",
            "proxy_host": self.__get_mp_proxy_host() or "--",
        }
        try:
            session = requests.Session()
            url = self.__build_url(self._login_api)
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"}
            if self._use_proxy:
                proxy_host = self.__get_mp_proxy_host()
                if not proxy_host:
                    status["message"] = "已开启代理，但未检测到 PROXY_HOST"
                else:
                    proxies = {"http": proxy_host, "https": proxy_host}
                    resp = session.options(url, headers=headers, proxies=proxies, timeout=(self._connect_timeout, self._read_timeout))
                    status["ok"] = resp.status_code < 500
                    status["message"] = f"代理可连接，HTTP {resp.status_code}"
            else:
                resp = session.options(url, headers=headers, timeout=(self._connect_timeout, self._read_timeout))
                status["ok"] = resp.status_code < 500
                status["message"] = f"直连可连接，HTTP {resp.status_code}"
        except Exception as err:
            status["message"] = self.__friendly_error(err)
        self.save_data("proxy_status", status)
        logger.info(f"聚影签到：代理/网络检测结果：{status.get('message')}")

    @staticmethod
    def __safe_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            number = int(value)
        except Exception:
            number = default
        return max(minimum, min(maximum, number))

    @staticmethod
    def __safe_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() not in ["", "0", "false", "no", "off", "关闭"]
        return bool(value)

    # ----------------------------- 页面构建 -----------------------------

    def __form_basic_card(self) -> Dict[str, Any]:
        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-6", "color": "surface"},
            "content": [
                self.__card_title("mdi-calendar-check", "基础设置", "#16b1ff"),
                {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                {
                    "component": "VCardText",
                    "props": {"class": "px-6 pb-6"},
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                self.__col_switch("enabled", "启用插件", 3),
                                self.__col_switch("use_proxy", "使用代理", 3),
                                self.__col_switch("notify", "开启通知", 3),
                                self.__col_switch("onlyonce", "保存后运行一次", 3),
                            ],
                        },
                    ],
                },
            ],
        }

    def __form_account_card(self) -> Dict[str, Any]:
        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-6", "color": "surface"},
            "content": [
                self.__card_title("mdi-account-key", "账号设置", "#4CAF50"),
                {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                {
                    "component": "VCardText",
                    "props": {"class": "px-6 pb-6"},
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                self.__col_text("username", "用户名", "填写聚影账号", 6, icon="mdi-account"),
                                self.__col_text("password", "密码", "填写聚影密码", 6, field_type="password", icon="mdi-lock"),
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12},
                                    "content": [
                                        {
                                            "component": "VTextarea",
                                            "props": {
                                                "model": "accounts",
                                                "label": "多账号，可选",
                                                "rows": 4,
                                                "placeholder": "每行一个：账号#密码\n也支持：JUYING_ACCOUNT='账号1#密码1@账号2#密码2'",
                                                "clearable": True,
                                                "prepend-inner-icon": "mdi-account-multiple",
                                            },
                                        }
                                    ],
                                }
                            ],
                        },
                    ],
                },
            ],
        }

    def __form_schedule_card(self) -> Dict[str, Any]:
        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-6", "color": "surface"},
            "content": [
                self.__card_title("mdi-timetable", "定时与重试", "#FF9800"),
                {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                {
                    "component": "VCardText",
                    "props": {"class": "px-6 pb-6"},
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                self.__col_text("cron", "Cron 表达式", "10 8 * * *", 6, hint="默认每天 08:10 执行", icon="mdi-clock-outline"),
                                self.__col_text("random_time_range", "随机延迟分钟", "例如：0-30，留空关闭", 6, hint="定时任务会在该范围内随机延迟执行。", icon="mdi-timer-sand"),
                            ],
                        },
                        {
                            "component": "VRow",
                            "content": [
                                self.__col_text("retry_times", "失败重试次数", "2", 3, field_type="number", hint="填 3 表示最多执行 1+3 次；成功、已签到、账号密码错误不会继续重试。", icon="mdi-refresh"),
                                self.__col_text("retry_interval", "重试间隔秒数", "10", 3, field_type="number", hint="每次可重试失败后等待多少秒。", icon="mdi-timer-refresh-outline"),
                                self.__col_text("connect_timeout", "连接超时秒数", "10", 3, field_type="number", hint="建立连接的超时时间。", icon="mdi-lan-connect"),
                                self.__col_text("read_timeout", "读取超时秒数", "60", 3, field_type="number", hint="等待服务器响应的超时时间。", icon="mdi-timer-outline"),
                            ],
                        },
                    ],
                },
            ],
        }

    def __form_advanced_card(self) -> Dict[str, Any]:
        return {
            "component": "VExpansionPanels",
            "props": {"class": "mb-6", "variant": "accordion"},
            "content": [
                {
                    "component": "VExpansionPanel",
                    "content": [
                        {
                            "component": "VExpansionPanelTitle",
                            "content": [
                                {"component": "VIcon", "props": {"class": "mr-3", "color": "deep-purple"}, "text": "mdi-tune"},
                                {"component": "span", "text": "高级设置"},
                            ],
                        },
                        {
                            "component": "VExpansionPanelText",
                            "content": [
                                {
                                    "component": "VRow",
                                    "content": [
                                        self.__col_text("site_url", "站点地址", "https://share.huamucang.top", 4, icon="mdi-web"),
                                        self.__col_text("login_api", "登录接口", "/api/app/login/", 4, icon="mdi-login"),
                                        self.__col_text("checkin_api", "签到接口", "/api/app/checkin/do/", 4, icon="mdi-check-decagram"),
                                    ],
                                },
                                {
                                    "component": "VRow",
                                    "content": [
                                        self.__col_select_notify_type(),
                                        self.__col_text("history_count", "历史保留条数", "30", 4, field_type="number", hint="默认保留最近 30 条。", icon="mdi-history"),
                                        self.__col_switch("check_proxy", "保存后检测代理", 2),
                                        self.__col_switch("clear_history", "保存后清空历史", 2),
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
        }

    def __col_select_notify_type(self) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": 4},
            "content": [
                {
                    "component": "VSelect",
                    "props": {
                        "model": "notify_type",
                        "label": "消息通知类型",
                        "items": [
                            {"title": "插件消息", "value": "Plugin"},
                            {"title": "站点消息", "value": "SiteMessage"},
                            {"title": "资源下载", "value": "Download"},
                            {"title": "整理入库", "value": "Organize"},
                            {"title": "订阅", "value": "Subscribe"},
                            {"title": "媒体服务器通知", "value": "MediaServer"},
                            {"title": "手动处理通知", "value": "Manual"},
                        ],
                        "prepend-inner-icon": "mdi-message-badge-outline",
                    },
                }
            ],
        }

    def __form_help_card(self) -> Dict[str, Any]:
        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4", "color": "surface"},
            "content": [
                self.__card_title("mdi-information", "使用说明", "#03a9f4"),
                {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                {
                    "component": "VCardText",
                    "props": {"class": "px-6 pb-6"},
                    "content": [
                        self.__help_item("mdi-account-key", "单账号", "填写上方用户名和密码即可。"),
                        self.__help_item("mdi-account-multiple", "多账号", "每行一个：账号#密码；也支持青龙变量 JUYING_ACCOUNT。"),
                        self.__help_item("mdi-play-circle", "保存后运行一次", "打开后保存，会在几秒后执行一次签到测试。"),
                        self.__help_item("mdi-proxy", "代理", "站点直连失败时开启“使用代理”，插件会读取 MoviePilot 的 PROXY_HOST。"),
                        self.__help_item("mdi-refresh", "重试", "只对网络异常、超时、限流或服务端错误重试；今日已签到和账号密码错误不会重试。"),
                        self.__help_item("mdi-heart", "感谢", "感谢大胖提供的支持。"),
                    ],
                },
            ],
        }

    def __page_status_card(self, latest: Dict[str, Any], configured: bool, accounts_count: int) -> Dict[str, Any]:
        last_status = "暂无状态"
        last_color = "info"
        if latest:
            if latest.get("has_fail"):
                last_status = "有失败"
                last_color = "error"
            elif latest.get("success"):
                last_status = "正常"
                last_color = "success"
            else:
                last_status = "已执行"
                last_color = "warning"

        proxy_status = self.get_data("proxy_status") or {}
        if proxy_status:
            proxy_text = "正常" if proxy_status.get("ok") else "异常"
            proxy_color = "success" if proxy_status.get("ok") else "error"
            proxy_detail = f"代理检测：{proxy_status.get('timestamp') or '--'}；{proxy_status.get('message') or '--'}"
        else:
            proxy_text = "未检测"
            proxy_color = "info"
            proxy_detail = "代理检测：暂无记录；可在高级设置打开“保存后检测代理”。"

        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-6 h-100", "color": "surface"},
            "content": [
                self.__card_title("mdi-view-dashboard-outline", "运行状态", "#2196F3"),
                {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                {
                    "component": "VCardText",
                    "props": {"class": "px-6 pb-6"},
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                self.__stat_chip("插件状态", "已启用" if self._enabled else "未启用", "success" if self._enabled else "grey"),
                                self.__stat_chip("账号状态", f"已配置 {accounts_count} 个" if configured else "未配置", "success" if configured else "warning"),
                                self.__stat_chip("定时周期", self._cron or "未设置", "info"),
                                self.__stat_chip("下次运行", self.__next_run_time(), "primary"),
                                self.__stat_chip("最近结果", last_status, last_color),
                                self.__stat_chip("代理状态", proxy_text, proxy_color),
                            ],
                        },
                        {
                            "component": "VAlert",
                            "props": {
                                "type": "info",
                                "variant": "tonal",
                                "density": "comfortable",
                                "class": "mt-4",
                                "text": f"最近执行：{latest.get('timestamp') or '--'}；{latest.get('message') or '暂无执行记录'}。{proxy_detail}",
                            },
                        },
                    ],
                },
            ],
        }

    def __page_profile_card(self, profile: Dict[str, Any], latest: Dict[str, Any]) -> Dict[str, Any]:
        joined_at = profile.get("date_joined") or ""
        registered_days = "--"
        if joined_at:
            try:
                joined_dt = datetime.datetime.fromisoformat(str(joined_at).replace("Z", "+00:00"))
                registered_days = str(max((datetime.datetime.now(joined_dt.tzinfo) - joined_dt).days, 0))
            except Exception:
                registered_days = "--"

        username = profile.get("username") or "--"
        email = profile.get("email") or "--"
        user_id = profile.get("id") or "--"
        level_name = profile.get("level_name") or "--"
        level = profile.get("level") or "--"
        points = profile.get("points", profile.get("reward_points", "--"))
        days = profile.get("checkin_days") if profile.get("checkin_days") is not None else "--"
        favorite_count = profile.get("favorite_count") if profile.get("favorite_count") is not None else "--"
        upload_count = profile.get("upload_count") if profile.get("upload_count") is not None else "--"

        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-6 h-100", "color": "surface"},
            "content": [
                self.__card_title("mdi-account-circle-outline", "用户信息", "#4CAF50"),
                {"component": "VDivider", "props": {"class": "mx-4 my-2"}},
                {
                    "component": "VCardText",
                    "props": {"class": "px-6 pb-6"},
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "text-h6 font-weight-medium mb-2"},
                            "text": username,
                        },
                        {
                            "component": "div",
                            "props": {"class": "text-body-2 text-medium-emphasis mb-4"},
                            "content": [
                                {"component": "VIcon", "props": {"size": "small", "class": "mr-1"}, "text": "mdi-email-outline"},
                                {"component": "span", "text": email},
                            ],
                        },
                        {
                            "component": "div",
                            "content": [
                                self.__inline_chip(f"等级：Lv.{level} {level_name}", "purple"),
                                self.__inline_chip(f"ID：{user_id}", "grey"),
                                self.__inline_chip(f"积分：{points}", "success"),
                                self.__inline_chip(f"签到天数：{days}", "info"),
                                self.__inline_chip(f"收藏数量：{favorite_count}", "default"),
                                self.__inline_chip(f"上传数量：{upload_count}", "default"),
                                self.__inline_chip(f"已注册：{registered_days}天", "deep-purple"),
                            ],
                        },
                    ],
                },
            ],
        }

    def __page_history_card(self, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        rows = []
        for item in list(reversed(history))[: self._history_count]:
            status_color = item.get("status_color") or ("success" if item.get("success") else "warning" if item.get("already") else "error")
            status_text = item.get("status_text") or "--"
            status_icon = "mdi-check-circle" if status_color == "success" else "mdi-alert-circle" if status_color == "warning" else "mdi-close-circle"
            attempt = item.get("attempt", 1)
            max_attempts = item.get("max_attempts", attempt)
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        self.__td_icon_text("mdi-clock-time-four-outline", item.get("timestamp") or "--", "info"),
                        self.__td_text(item.get("username") or "--"),
                        {
                            "component": "td",
                            "props": {"class": "text-center"},
                            "content": [
                                {
                                    "component": "VChip",
                                    "props": {"color": status_color, "size": "small", "variant": "tonal"},
                                    "content": [
                                        {"component": "VIcon", "props": {"size": "small", "start": True}, "text": status_icon},
                                        {"component": "span", "text": status_text},
                                    ],
                                }
                            ],
                        },
                        self.__td_icon_text("mdi-counter", f"{item.get('checkin_days', '--')}天", "info"),
                        self.__td_icon_text("mdi-star-circle-outline", self.__format_points(item.get("points_awarded", "--")), "warning"),
                        self.__td_icon_text("mdi-lan-connect", f"{item.get('network', '--')}｜{attempt}/{max_attempts}", "primary"),
                        self.__td_text(item.get("message") or "--"),
                    ],
                }
            )

        if not rows:
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "props": {"colspan": 7, "class": "text-center text-medium-emphasis"}, "text": "暂无签到历史"}
                    ],
                }
            )

        return {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4 elevation-2", "color": "surface", "style": "border-radius: 16px;"},
            "content": [
                self.__card_title("mdi-table-clock", "最近签到历史", "#9C27B0"),
                {
                    "component": "VCardText",
                    "props": {"class": "pa-6"},
                    "content": [
                        {
                            "component": "VTable",
                            "props": {"hover": True, "density": "comfortable", "class": "rounded-lg"},
                            "content": [
                                {
                                    "component": "thead",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                self.__th("mdi-clock-time-four-outline", "签到时间", "info"),
                                                self.__th("mdi-account", "账号", "primary"),
                                                self.__th("mdi-check-circle", "签到状态", "success"),
                                                self.__th("mdi-counter", "签到天数", "info"),
                                                self.__th("mdi-star-circle-outline", "奖励积分", "warning"),
                                                self.__th("mdi-refresh-auto", "网络/请求", "primary"),
                                                self.__th("mdi-text-box-outline", "结果说明", "deep-purple"),
                                            ],
                                        }
                                    ],
                                },
                                {"component": "tbody", "content": rows},
                            ],
                        },
                        {
                            "component": "div",
                            "props": {"class": "text-caption text-grey mt-2"},
                            "content": [
                                {"component": "VIcon", "props": {"size": "x-small", "class": "mr-1"}, "text": "mdi-format-list-bulleted"},
                                {"component": "span", "text": f"共显示 {min(len(history), self._history_count)} 条签到记录"},
                            ],
                        },
                    ],
                },
            ],
        }

    @staticmethod
    def __card_title(icon: str, title: str, color: str) -> Dict[str, Any]:
        return {
            "component": "VCardItem",
            "props": {"class": "px-6 pb-0"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "d-flex align-center text-h6"},
                    "content": [
                        {"component": "VIcon", "props": {"class": "mr-3", "style": f"color: {color};", "size": "default"}, "text": icon},
                        {"component": "span", "text": title},
                    ],
                }
            ],
        }

    @staticmethod
    def __col_switch(model: str, label: str, md: int, hint: str = "") -> Dict[str, Any]:
        props = {"model": model, "label": label}
        if hint:
            props.update({"hint": hint, "persistent-hint": True})
        return {"component": "VCol", "props": {"cols": 12, "sm": md}, "content": [{"component": "VSwitch", "props": props}]}

    @staticmethod
    def __col_text(model: str, label: str, placeholder: str, md: int, field_type: str = "text", hint: str = "", icon: str = "") -> Dict[str, Any]:
        props = {"model": model, "label": label, "placeholder": placeholder, "clearable": True}
        if field_type != "text":
            props["type"] = field_type
        if hint:
            props.update({"hint": hint, "persistent-hint": True})
        if icon:
            props["prepend-inner-icon"] = icon
        return {"component": "VCol", "props": {"cols": 12, "md": md}, "content": [{"component": "VTextField", "props": props}]}

    @staticmethod
    def __help_item(icon: str, title: str, text: str) -> Dict[str, Any]:
        return {
            "component": "div",
            "props": {"class": "d-flex mb-4"},
            "content": [
                {"component": "VIcon", "props": {"class": "mr-4 mt-1", "color": "primary"}, "text": icon},
                {
                    "component": "div",
                    "content": [
                        {"component": "div", "props": {"class": "font-weight-medium mb-1"}, "text": title},
                        {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis"}, "text": text},
                    ],
                },
            ],
        }

    @staticmethod
    def __stat_chip(label: str, value: str, color: str) -> Dict[str, Any]:
        return {
            "component": "VCol",
            "props": {"cols": 12, "sm": 6, "md": 4, "lg": 2},
            "content": [
                {
                    "component": "div",
                    "props": {"class": "d-flex flex-column justify-space-between", "style": "min-height: 64px;"},
                    "content": [
                        {"component": "div", "props": {"class": "text-subtitle-2 text-medium-emphasis"}, "text": label},
                        {"component": "VChip", "props": {"color": color, "class": "mt-2 align-self-start", "variant": "tonal"}, "text": value},
                    ],
                }
            ],
        }

    @staticmethod
    def __inline_chip(text: str, color: str) -> Dict[str, Any]:
        return {"component": "VChip", "props": {"color": color, "variant": "tonal", "class": "ma-1"}, "text": text}

    @staticmethod
    def __th(icon: str, text: str, color: str) -> Dict[str, Any]:
        return {
            "component": "th",
            "props": {"class": "text-center text-body-1 font-weight-bold"},
            "content": [
                {"component": "VIcon", "props": {"color": color, "size": "small", "class": "mr-1"}, "text": icon},
                {"component": "span", "text": text},
            ],
        }

    @staticmethod
    def __td_text(text: str) -> Dict[str, Any]:
        return {"component": "td", "props": {"class": "text-center text-high-emphasis"}, "text": str(text)}

    @staticmethod
    def __format_points(points: Any) -> str:
        if points in [None, "", "--"]:
            return "--"
        return f"+{points}"

    @staticmethod
    def __td_icon_text(icon: str, text: str, color: str) -> Dict[str, Any]:
        return {
            "component": "td",
            "props": {"class": "text-center text-high-emphasis"},
            "content": [
                {"component": "VIcon", "props": {"color": color, "size": "x-small", "class": "mr-1"}, "text": icon},
                {"component": "span", "text": str(text)},
            ],
        }

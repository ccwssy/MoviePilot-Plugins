import os
import re
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType


class StrmLinkChecker(_PluginBase):
    """
    STRM 文件源链接检查插件
    """
    plugin_name = "STRM源链接检查"
    plugin_desc = "监控STRM文件中的源链接，如果源链接失效，通过整理记录同步删除已入库Emby中对应的条目及附属文件。"
    plugin_icon = "strmcheck.png"
    plugin_version = "1.0"
    plugin_author = "ccwssy"
    author_url = "https://github.com/ccwssy/MoviePilot-Plugins"
    plugin_config_prefix = "strmlinkchecker_"
    plugin_order = 24
    auth_level = 1

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled = False
    _cron = None
    _onlyonce = False
    _notify = False
    _strm_path = None
    _delete_strm = False
    _delete_sidecar = False
    _delete_history = False
    _request_timeout = 10
    _emby_host = None
    _emby_apikey = None
    _running = False
    _transferhis = None

    def init_plugin(self, config: dict = None):
        self._transferhis = TransferHistoryOper()

        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._notify = config.get("notify")
            self._strm_path = config.get("strm_path")
            self._delete_strm = config.get("delete_strm", False)
            self._delete_sidecar = config.get("delete_sidecar", False)
            self._delete_history = config.get("delete_history", False)
            self._request_timeout = config.get("request_timeout", 10)
            self._emby_host = config.get("emby_host")
            self._emby_apikey = config.get("emby_apikey")

        if self._enabled:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info(f"STRM源链接检查服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self.__check_all,
                    trigger='date',
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                    name="STRM源链接检查"
                )
                self._onlyonce = False
                self.update_config({
                    "onlyonce": False,
                    "cron": self._cron,
                    "enabled": self._enabled,
                    "notify": self._notify,
                    "strm_path": self._strm_path,
                    "delete_strm": self._delete_strm,
                    "delete_sidecar": self._delete_sidecar,
                    "delete_history": self._delete_history,
                    "request_timeout": self._request_timeout,
                    "emby_host": self._emby_host,
                    "emby_apikey": self._emby_apikey,
                })

            if self._scheduler and self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/strm_check",
                "event": EventType.PluginAction,
                "desc": "STRM源链接检查",
                "category": "媒体管理",
                "data": {"action": "strm_check"}
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/run_check",
                "endpoint": self.api_run_check,
                "methods": ["GET"],
                "summary": "手动触发STRM源链接检查",
                "description": "手动触发一次检查",
            }
        ]

    def api_run_check(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        thread = threading.Thread(target=self.__check_all)
        thread.start()
        return {"success": True, "message": "检查任务已启动"}

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [
                {
                    "id": "StrmLinkChecker",
                    "name": "STRM源链接检查",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__check_all,
                    "kwargs": {}
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'delete_strm',
                                            'label': '删除失效STRM',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'delete_sidecar',
                                            'label': '删除附属文件(jpg/nfo)',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'delete_history',
                                            'label': '删除整理记录',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期(cron)',
                                            'placeholder': '5位cron表达式，如 0 6 * * *'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'request_timeout',
                                            'label': '请求超时(秒)',
                                            'placeholder': '10'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'strm_path',
                                            'rows': '2',
                                            'label': 'STRM文件目录路径',
                                            'placeholder': 'STRM文件所在目录路径，如 /media/movies（一行一个目录）'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'emby_host',
                                            'label': 'Emby地址',
                                            'placeholder': 'http://192.168.1.100:8096'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'emby_apikey',
                                            'label': 'Emby API Key',
                                            'placeholder': 'Emby API密钥'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '插件会扫描STRM文件中的源链接，'
                                                    '通过HTTP请求检查源链接是否有效。'
                                                    '如果链接失效，将通过Emby API删除该STRM文件对应的单个条目（不会删除整个视频项目）。'
                                                    '如果Emby地址和API Key留空，将使用系统配置的媒体服务器。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '注意：删除Emby媒体库条目操作不可逆，请谨慎使用。'
                                                    '建议先手动运行一次检查结果，确认无误后再开启定时任务。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "cron": "0 6 * * *",
            "onlyonce": False,
            "notify": True,
            "strm_path": "",
            "delete_strm": False,
            "delete_sidecar": False,
            "delete_history": False,
            "request_timeout": 10,
            "emby_host": "",
            "emby_apikey": "",
        }

    def get_page(self) -> List[dict]:
        history = self.get_data('history') or []
        if not history:
            return [
                {
                    'component': 'div',
                    'text': '暂无检查记录',
                    'props': {'class': 'text-center'}
                }
            ]
        history = sorted(history, key=lambda x: x.get('check_time', ''), reverse=True)
        contents = []
        for item in history[:50]:
            strm_path = item.get("strm_path", "")
            source_url = item.get("source_url", "")
            status = item.get("status", "")
            title = item.get("title", "")
            check_time = item.get("check_time", "")
            action = item.get("action", "")

            status_color = "success" if status == "有效" else "error"
            contents.append({
                'component': 'VCard',
                'content': [
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2'},
                        'text': f'文件：{Path(strm_path).name}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2'},
                        'text': f'媒体：{title}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2'},
                        'text': f'状态：{status}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2'},
                        'text': f'操作：{action}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2'},
                        'text': f'时间：{check_time}'
                    }
                ]
            })
        return [
            {
                'component': 'div',
                'props': {'class': 'grid gap-3 grid-info-card'},
                'content': contents
            }
        ]

    def __check_all(self):
        """
        检查所有STRM文件
        """
        if self._running:
            logger.warning("STRM源链接检查任务正在运行中，跳过本次执行")
            return

        self._running = True
        try:
            if not self._strm_path:
                logger.error("STRM文件目录未配置")
                return

            strm_dirs = [d.strip() for d in self._strm_path.split("\n") if d.strip()]
            if not strm_dirs:
                logger.error("STRM文件目录未配置")
                return

            # 获取Emby配置
            emby_host = self._emby_host
            emby_apikey = self._emby_apikey
            if not emby_host or not emby_apikey:
                # 从系统配置获取
                from app.helper.service import ServiceConfigHelper
                mediaservers = ServiceConfigHelper.get_mediaserver_configs()
                for server in mediaservers:
                    if server.type == "emby" and server.enabled:
                        emby_host = server.config.get("host", "")
                        emby_apikey = server.config.get("apikey", "")
                        break

            if not emby_host or not emby_apikey:
                logger.error("未配置Emby服务器信息，无法删除Emby条目")
                return

            # 标准化Emby地址
            if emby_host and not emby_host.endswith("/"):
                emby_host += "/"

            total_checked = 0
            total_dead = 0
            total_deleted_emby = 0
            total_deleted_strm = 0
            total_deleted_sidecar = 0
            failed_items = []

            for strm_dir in strm_dirs:
                if not os.path.isdir(strm_dir):
                    logger.warning(f"STRM目录不存在: {strm_dir}")
                    continue

                logger.info(f"开始扫描STRM目录: {strm_dir}")
                for root, dirs, files in os.walk(strm_dir):
                    for filename in files:
                        if not filename.lower().endswith('.strm'):
                            continue

                        strm_path = os.path.join(root, filename)
                        total_checked += 1

                        try:
                            result = self.__check_single_strm(
                                strm_path=strm_path,
                                emby_host=emby_host,
                                emby_apikey=emby_apikey
                            )
                            if result.get("dead"):
                                total_dead += 1
                                if result.get("emby_deleted"):
                                    total_deleted_emby += 1
                                if result.get("strm_deleted"):
                                    total_deleted_strm += 1
                                if result.get("sidecar_deleted"):
                                    total_deleted_sidecar += 1
                                if result.get("failed"):
                                    failed_items.append(result.get("strm_path", ""))
                        except Exception as e:
                            logger.error(f"检查STRM文件失败 {strm_path}: {str(e)}")
                            failed_items.append(strm_path)

            # 发送通知
            if self._notify:
                msg = (f"扫描STRM文件: {total_checked}个\n"
                       f"源链接失效: {total_dead}个\n"
                       f"删除Emby条目: {total_deleted_emby}个\n"
                       f"删除STRM文件: {total_deleted_strm}个\n"
                       f"删除附属文件: {total_deleted_sidecar}个\n")
                if failed_items:
                    msg += f"失败: {len(failed_items)}个"
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="STRM源链接检查完成",
                    text=msg
                )

            logger.info(f"STRM源链接检查完成: "
                        f"扫描{total_checked}个, "
                        f"失效{total_dead}个, "
                        f"删除Emby{total_deleted_emby}个, "
                        f"删除STRM{total_deleted_strm}个, "
                        f"删除附属文件{total_deleted_sidecar}个")

        except Exception as e:
            logger.error(f"STRM源链接检查异常: {str(e)}")
        finally:
            self._running = False

    def __check_single_strm(self, strm_path: str, emby_host: str, emby_apikey: str) -> dict:
        """
        检查单个STRM文件
        """
        result = {
            "strm_path": strm_path,
            "dead": False,
            "emby_deleted": False,
            "strm_deleted": False,
            "sidecar_deleted": False,
            "failed": False,
        }

        try:
            # 读取STRM文件内容（源链接）
            with open(strm_path, 'r', encoding='utf-8') as f:
                source_url = f.read().strip()

            if not source_url:
                logger.warning(f"STRM文件为空: {strm_path}")
                return result

            # 检查源链接是否有效
            is_valid = self.__check_url_valid(source_url)

            if is_valid:
                logger.debug(f"源链接有效: {strm_path}")
                self.__save_check_record(strm_path, source_url, "有效", "无操作")
                return result

            # 源链接失效
            logger.warning(f"源链接失效: {strm_path} -> {source_url[:100]}...")
            result["dead"] = True

            # 通过转移记录查找媒体信息
            title = ""
            transfer_his = self._transferhis.get_by_dest(dest=strm_path)
            if not transfer_his:
                # 尝试模糊匹配
                strm_name = Path(strm_path).stem
                transfer_his_list = self._transferhis.get_by_title(strm_name)
                if transfer_his_list:
                    transfer_his = transfer_his_list[0]

            if transfer_his:
                title = transfer_his.title or ""

                # 通过Emby API删除该STRM文件对应的单个条目（不是整个视频项目）
                emby_ok = self.__delete_emby_item_by_path(
                    emby_host=emby_host,
                    emby_apikey=emby_apikey,
                    strm_path=strm_path
                )
                if emby_ok:
                    result["emby_deleted"] = True

                # 删除整理记录
                if self._delete_history:
                    try:
                        self._transferhis.delete(transfer_his.id)
                        logger.info(f"已删除整理记录: {transfer_his.id}")
                    except Exception as e:
                        logger.error(f"删除整理记录失败: {e}")

            # 删除附属文件（jpg/nfo）
            if self._delete_sidecar:
                sidecar_deleted = self.__delete_sidecar_files(strm_path)
                if sidecar_deleted:
                    result["sidecar_deleted"] = True

            # 删除STRM文件
            if self._delete_strm:
                try:
                    os.remove(strm_path)
                    logger.info(f"已删除失效STRM文件: {strm_path}")
                    result["strm_deleted"] = True
                    # 尝试删除空父目录
                    self.__remove_empty_parent(Path(strm_path).parent)
                except Exception as e:
                    logger.error(f"删除STRM文件失败: {strm_path}: {e}")

            action_parts = []
            if result["emby_deleted"]:
                action_parts.append("已删除Emby条目")
            if result["strm_deleted"]:
                action_parts.append("已删除STRM文件")
            if result["sidecar_deleted"]:
                action_parts.append("已删除附属文件")
            if not action_parts:
                action_parts.append("未操作(仅报告)")

            self.__save_check_record(
                strm_path, source_url, "失效",
                ", ".join(action_parts),
                title=title
            )

        except Exception as e:
            logger.error(f"检查STRM文件异常 {strm_path}: {str(e)}")
            result["failed"] = True
            self.__save_check_record(strm_path, "", "检查失败", str(e))

        return result

    def __check_url_valid(self, url: str) -> bool:
        """
        检查URL是否有效（通过HTTP HEAD请求）
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            resp = requests.head(
                url,
                headers=headers,
                timeout=self._request_timeout,
                allow_redirects=True,
                verify=False
            )
            # 2xx 或 3xx 认为有效
            if resp.status_code < 400:
                return True

            # 如果HEAD不支持，尝试GET range
            if resp.status_code in [400, 405, 501]:
                headers['Range'] = 'bytes=0-0'
                resp2 = requests.get(
                    url,
                    headers=headers,
                    timeout=self._request_timeout,
                    allow_redirects=True,
                    verify=False
                )
                return resp2.status_code in [200, 206, 301, 302, 303, 304, 307, 308]

            return False
        except requests.ConnectionError:
            return False
        except requests.Timeout:
            logger.debug(f"请求超时: {url[:80]}...")
            return False
        except Exception as e:
            logger.debug(f"请求异常: {url[:80]}... {str(e)}")
            return False

    def __delete_emby_item_by_path(self, emby_host: str, emby_apikey: str,
                                   strm_path: str) -> bool:
        """
        通过STRM文件路径精确匹配并删除Emby中的单个条目。
        只删除该STRM文件对应的条目，不会删除整个视频项目（同一视频的其他版本不受影响）。
        """
        try:
            # 通过路径精确搜索Emby中的条目
            search_url = f"{emby_host}emby/Items"
            params = {
                "api_key": emby_apikey,
                "Recursive": "true",
                "Fields": "Path,ProviderIds",
            }

            # 先尝试用文件名搜索
            strm_name = Path(strm_path).name
            params2 = {
                "api_key": emby_apikey,
                "SearchTerm": Path(strm_path).stem,
                "Recursive": "true",
                "Fields": "Path,ProviderIds",
                "IncludeItemTypes": "Movie,Episode",
            }
            resp2 = requests.get(search_url, params=params2, timeout=30, verify=False)
            if resp2.status_code == 200:
                data2 = resp2.json()
                items = data2.get("Items", []) or data2.get("items", [])
                # 精确匹配路径
                items = [item for item in items if item.get("Path", "") == strm_path]
            else:
                items = []

            # 如果没找到，全量扫描匹配路径
            if not items:
                params3 = {
                    "api_key": emby_apikey,
                    "Recursive": "true",
                    "Fields": "Path,ProviderIds",
                }
                resp3 = requests.get(search_url, params=params3, timeout=60, verify=False)
                if resp3.status_code == 200:
                    data3 = resp3.json()
                    all_items = data3.get("Items", []) or data3.get("items", [])
                    items = [item for item in all_items
                             if item.get("Path", "") == strm_path]

            if not items:
                logger.warning(f"未在Emby中找到匹配该路径的条目: {strm_path}")
                return False

            # 删除匹配的条目（只删除精确匹配该STRM路径的条目）
            deleted_count = 0
            for item in items:
                item_id = item.get("Id")
                if not item_id:
                    continue

                del_url = f"{emby_host}emby/Items/{item_id}?api_key={emby_apikey}"
                del_resp = requests.delete(del_url, timeout=30, verify=False)
                if del_resp.status_code in [200, 204]:
                    logger.info(f"已删除Emby条目: {item.get('Name', '')} (ID: {item_id}, Path: {strm_path})")
                    deleted_count += 1
                else:
                    logger.error(f"删除Emby条目失败: {item_id}, 状态码: {del_resp.status_code}")

            return deleted_count > 0

        except Exception as e:
            logger.error(f"删除Emby条目异常: {str(e)}")
            return False

    def __delete_sidecar_files(self, strm_path: str) -> bool:
        """
        删除STRM文件同名的附属文件（jpg, nfo）
        """
        deleted = False
        strm_stem = Path(strm_path).stem
        strm_dir = Path(strm_path).parent

        for ext in ['.jpg', '.nfo']:
            sidecar_path = strm_dir / f"{strm_stem}{ext}"
            if sidecar_path.exists():
                try:
                    sidecar_path.unlink()
                    logger.info(f"已删除附属文件: {sidecar_path}")
                    deleted = True
                except Exception as e:
                    logger.error(f"删除附属文件失败 {sidecar_path}: {e}")

        return deleted

    def __remove_empty_parent(self, dir_path: Path):
        """
        递归删除空父目录
        """
        try:
            if not dir_path.exists():
                return
            if not any(dir_path.iterdir()):
                dir_path.rmdir()
                logger.info(f"已删除空目录: {dir_path}")
                self.__remove_empty_parent(dir_path.parent)
        except Exception:
            pass

    def __save_check_record(self, strm_path: str, source_url: str,
                            status: str, action: str, title: str = ""):
        """
        保存检查记录
        """
        history = self.get_data('history') or []
        history.append({
            "strm_path": strm_path,
            "source_url": source_url[:200] if source_url else "",
            "status": status,
            "action": action,
            "title": title,
            "check_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        })
        # 只保留最近500条记录
        if len(history) > 500:
            history = history[-500:]
        self.save_data('history', history)

    def stop_service(self):
        """
        停止插件
        """
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._scheduler.shutdown()
            self._scheduler = None

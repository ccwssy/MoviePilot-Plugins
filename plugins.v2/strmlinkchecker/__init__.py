import os
import re
import time
import threading
from datetime import datetime, timedelta, time as dtime
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
    plugin_name = "Strm失效清理"
    plugin_desc = "通过转移记录对比Emby媒体库STRM文件与源STRM文件，如果源文件已删除，同步清理Emby条目及附属文件。"
    plugin_icon = "strmcheck.png"
    plugin_version = "1.1.0"
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
    _emby_host = None
    _emby_apikey = None
    _running = False
    _transferhis = None

    # 远程可达性验证（风险功能）相关
    _verify_enabled = False
    _verify_threads = 1
    _verify_cooldown = 5
    _verify_max_daily = 50
    _verify_peak_avoid = True
    _verify_peak_start = "23:00"
    _verify_peak_end = "07:00"
    _verify_count = 0
    _verify_lock = threading.Lock()

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
            self._emby_host = config.get("emby_host")
            self._emby_apikey = config.get("emby_apikey")
            # 远程可达性验证配置
            self._verify_enabled = config.get("verify_enabled", False)
            self._verify_threads = max(1, int(config.get("verify_threads", 1)))
            self._verify_cooldown = max(1, int(config.get("verify_cooldown", 5)))
            self._verify_max_daily = max(1, int(config.get("verify_max_daily", 50)))
            self._verify_peak_avoid = config.get("verify_peak_avoid", True)
            self._verify_peak_start = config.get("verify_peak_start", "23:00")
            self._verify_peak_end = config.get("verify_peak_end", "07:00")
            self._verify_count = 0

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
                    "emby_host": self._emby_host,
                    "emby_apikey": self._emby_apikey,
                    "verify_enabled": self._verify_enabled,
                    "verify_threads": self._verify_threads,
                    "verify_cooldown": self._verify_cooldown,
                    "verify_max_daily": self._verify_max_daily,
                    "verify_peak_avoid": self._verify_peak_avoid,
                    "verify_peak_start": self._verify_peak_start,
                    "verify_peak_end": self._verify_peak_end,
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
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
                                            'label': 'MP2入库后的STRM目录路径',
                                            'placeholder': 'MP2容器内看到的入库STRM文件目录，如 /clouddata/movies/云盘（一行一个目录）'
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
                                            'text': '插件会扫描MP2入库后的STRM文件，通过转移记录查找对应的源STRM文件路径。'
                                                    '如果源STRM文件在磁盘上已不存在，说明源文件已被删除，'
                                                    '将通过Emby API删除该STRM文件对应的条目及附属文件。'
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
                    },
                    # ─── 远程可达性验证（风险功能） ───
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
                                            'type': 'error',
                                            'variant': 'tonal',
                                            'title': '⚠️ 远程可达性验证（风险功能）',
                                            'text': '当STRM文件无转移记录时，读取文件内URL并发送HEAD请求验证是否可达。'
                                                    '开启后可能触发网盘/源站的风控限制，请严格控制频率。'
                                                    '建议在使用前确认你的STRM源站允许HEAD请求。'
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
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'verify_enabled',
                                            'label': '启用远程验证',
                                            'color': 'error',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'verify_threads',
                                            'label': '线程数',
                                            'placeholder': '1-2',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'verify_cooldown',
                                            'label': '请求间隔(秒)',
                                            'placeholder': '5',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'verify_max_daily',
                                            'label': '单次最大验证数',
                                            'placeholder': '50',
                                            'type': 'number'
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
                                            'model': 'verify_peak_avoid',
                                            'label': '避开高峰时段',
                                            'color': 'error',
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
                                            'model': 'verify_peak_start',
                                            'label': '低峰起始时间',
                                            'placeholder': '23:00',
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
                                            'model': 'verify_peak_end',
                                            'label': '低峰结束时间',
                                            'placeholder': '07:00',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
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
            "emby_host": "",
            "emby_apikey": "",
            "verify_enabled": False,
            "verify_threads": 1,
            "verify_cooldown": 5,
            "verify_max_daily": 50,
            "verify_peak_avoid": True,
            "verify_peak_start": "23:00",
            "verify_peak_end": "07:00",
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

        # 按状态分类
        valid_items = [h for h in history if h.get('status') == '有效']
        remote_valid_items = [h for h in history if h.get('status') == '远程有效']
        remote_dead_items = [h for h in history if h.get('status') == '远程不可达']
        no_record_items = [h for h in history if h.get('status') == '无转移记录' or h.get('status') == '无源路径']
        dead_items = [h for h in history if h.get('status') == '源文件缺失']
        failed_items = [h for h in history if h.get('status') == '检查失败']
        other_items = [h for h in history if h not in valid_items and h not in remote_valid_items
                       and h not in remote_dead_items and h not in no_record_items
                       and h not in dead_items and h not in failed_items]

        def render_section(title: str, items: list, color: str, max_count: int = 20):
            if not items:
                return []
            cards = []
            for item in items[:max_count]:
                strm_path = item.get("strm_path", "")
                status = item.get("status", "")
                title_text = item.get("title", "")
                action = item.get("action", "")
                check_time = item.get("check_time", "")
                cards.append({
                    'component': 'VCard',
                    'props': {'class': f'border-left-{color}'},
                    'content': [
                        {
                            'component': 'VCardText',
                            'props': {'class': 'pa-0 px-2 py-1'},
                            'text': f'{Path(strm_path).name}'
                        },
                        {
                            'component': 'VCardText',
                            'props': {'class': 'pa-0 px-2 py-1 text-caption text-grey'},
                            'text': f'{title_text} | {action} | {check_time}'
                        }
                    ]
                })
            return [
                {
                    'component': 'div',
                    'props': {'class': 'mt-3 mb-1 font-weight-bold'},
                    'text': f'{title} ({len(items)})'
                },
                {
                    'component': 'div',
                    'props': {'class': 'grid gap-2 grid-info-card'},
                    'content': cards
                }
            ]

        sections = []
        sections.extend(render_section('✅ 有效', valid_items, 'success', 10))
        sections.extend(render_section('🌐 远程有效', remote_valid_items, 'info', 20))
        sections.extend(render_section('📡 远程不可达', remote_dead_items, 'warning', 20))
        sections.extend(render_section('⚠️ 无转移记录', no_record_items, 'warning', 20))
        sections.extend(render_section('❌ 源文件缺失', dead_items, 'error', 50))
        sections.extend(render_section('💥 检查失败', failed_items, 'error', 20))
        sections.extend(render_section('📋 其他', other_items, 'grey', 20))

        return sections

    def __check_all(self):
        """
        检查所有STRM文件
        """
        if self._running:
            logger.warning("STRM源链接检查任务正在运行中，跳过本次执行")
            return

        self._running = True
        # 重置验证计数器
        self._verify_count = 0
        try:
            if not self._strm_path:
                logger.error("Emby媒体库STRM目录未配置")
                return

            strm_dirs = [d.strip() for d in self._strm_path.split("\n") if d.strip()]
            if not strm_dirs:
                logger.error("Emby媒体库STRM目录未配置")
                return

            # 获取Emby配置
            emby_host = self._emby_host
            emby_apikey = self._emby_apikey
            if not emby_host or not emby_apikey:
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

            if emby_host and not emby_host.endswith("/"):
                emby_host += "/"

            total_checked = 0
            total_dead = 0
            total_deleted_emby = 0
            total_deleted_strm = 0
            total_deleted_sidecar = 0
            total_no_record = 0
            total_verify_valid = 0
            total_verify_dead = 0
            failed_items = []

            # 扫描Emby媒体库中的STRM文件
            for lib_dir in strm_dirs:
                if not os.path.isdir(lib_dir):
                    logger.warning(f"Emby媒体库STRM目录不存在: {lib_dir}")
                    continue

                logger.info(f"开始扫描Emby媒体库STRM目录: {lib_dir}")
                for root, dirs, files in os.walk(lib_dir):
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
                                if result.get("no_record"):
                                    total_no_record += 1
                                if result.get("failed"):
                                    failed_items.append(result.get("strm_path", ""))
                                continue
                            # 非dead的记录：统计验证结果
                            if result.get("verify_valid"):
                                total_verify_valid += 1
                            if result.get("verify_dead"):
                                total_verify_dead += 1
                        except Exception as e:
                            logger.error(f"检查STRM文件失败 {strm_path}: {str(e)}")
                            failed_items.append(strm_path)

            # 发送通知
            if self._notify:
                msg_lines = [
                    "━━━━━━━━━━━━━━━━━━",
                    f"📊 扫描统计",
                    f"  ├ 检查文件: {total_checked} 个",
                    f"  └ 源文件缺失: {total_dead} 个",
                    "",
                    f"🔧 清理操作",
                    f"  ├ 删除 Emby 条目: {total_deleted_emby} 个",
                    f"  ├ 删除 STRM 文件: {total_deleted_strm} 个",
                    f"  └ 删除附属文件: {total_deleted_sidecar} 个",
                ]
                if total_no_record > 0:
                    msg_lines.insert(4, f"     └ 无转移记录: {total_no_record} 个")
                # 远程验证统计（仅在启用时显示）
                if self._verify_enabled:
                    msg_lines.insert(-1, "")
                    msg_lines.insert(-1, f"🌐 远程可达性验证")
                    msg_lines.insert(-1, f"  ├ 远程有效: {total_verify_valid} 个（保留）")
                    msg_lines.insert(-1, f"  └ 远程不可达: {total_verify_dead} 个（已清理）")
                if failed_items:
                    msg_lines.append("")
                    msg_lines.append(f"⚠️ 检查失败: {len(failed_items)} 个")
                msg_lines.append("━━━━━━━━━━━━━━━━━━")
                msg = "\n".join(msg_lines)
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="🧹 STRM 失效清理 · 检查完成",
                    text=msg
                )

            logger.info(f"STRM源文件检查完成: "
                        f"扫描{total_checked}个, "
                        f"源文件缺失{total_dead}个(无记录{total_no_record}), "
                        f"远程有效{total_verify_valid}个, "
                        f"远程不可达{total_verify_dead}个, "
                        f"删除Emby{total_deleted_emby}个, "
                        f"删除STRM{total_deleted_strm}个, "
                        f"删除附属文件{total_deleted_sidecar}个")

        except Exception as e:
            logger.error(f"STRM源文件检查异常: {str(e)}")
        finally:
            self._running = False

    def __can_verify(self) -> bool:
        """
        检查当前是否满足验证条件：未达日限且在低峰时段内
        """
        if self._verify_count >= self._verify_max_daily:
            logger.debug(f"远程验证已达单次上限({self._verify_max_daily})，跳过")
            return False
        if not self._verify_peak_avoid:
            return True
        # 检查当前时间是否在低峰时段内
        now = datetime.now().time()
        try:
            start_parts = self._verify_peak_start.split(":")
            end_parts = self._verify_peak_end.split(":")
            peak_start = dtime(int(start_parts[0]), int(start_parts[1]))
            peak_end = dtime(int(end_parts[0]), int(end_parts[1]))
        except (IndexError, ValueError):
            logger.warning("低峰时段格式错误，使用默认 23:00-07:00")
            peak_start = dtime(23, 0)
            peak_end = dtime(7, 0)

        if peak_start <= peak_end:
            # 同一天内（如 01:00-06:00）
            return peak_start <= now <= peak_end
        else:
            # 跨天（如 23:00-07:00）
            return now >= peak_start or now <= peak_end

    def __verify_strm_url(self, strm_path: str) -> Tuple[bool, str]:
        """
        读取STRM文件中的URL，通过HEAD请求验证远程可达性。
        使用线程锁控制频率（冷却时间）。
        返回 (可达, URL字符串)
        """
        url = ""
        try:
            with open(strm_path, 'r', encoding='utf-8') as f:
                url = f.read().strip()
        except Exception as e:
            logger.warning(f"读取STRM文件失败 {strm_path}: {e}")
            return False, url

        if not url:
            logger.warning(f"STRM文件内容为空: {strm_path}")
            return False, url

        # 风控：冷却 + 计数
        with self._verify_lock:
            if self._verify_count > 0:
                time.sleep(self._verify_cooldown)
            self._verify_count += 1

        logger.info(f"验证STRM远程可达性: {url} (来自 {strm_path})")
        try:
            session = requests.Session()
            session.verify = False
            session.max_redirects = 5

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                              " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Range": "bytes=0-1",
            }
            resp = session.head(url, headers=headers, timeout=10, allow_redirects=True)
            if resp.status_code < 400:
                logger.info(f"STRM远程可达: {url} (HTTP {resp.status_code})")
                return True, url
            else:
                # 有些CDN不支持HEAD，尝试GET + Range
                resp2 = session.get(url, headers=headers, timeout=10, stream=True)
                if resp2.status_code < 400:
                    logger.info(f"STRM远程可达(GET): {url} (HTTP {resp2.status_code})")
                    return True, url
                logger.warning(f"STRM远程不可达: {url} (HEAD {resp.status_code}, GET {resp2.status_code})")
                return False, url
        except requests.Timeout:
            logger.warning(f"STRM验证超时: {url}")
            return False, url
        except requests.ConnectionError as e:
            logger.warning(f"STRM验证连接失败: {url} - {e}")
            return False, url
        except Exception as e:
            logger.warning(f"STRM验证异常: {url} - {e}")
            return False, url

    def __check_single_strm(self, strm_path: str,
                            emby_host: str, emby_apikey: str) -> dict:
        """
        检查单个STRM文件：通过转移记录查找源文件路径，检查源文件是否存在
        """
        result = {
            "strm_path": strm_path,
            "dead": False,
            "emby_deleted": False,
            "strm_deleted": False,
            "sidecar_deleted": False,
            "no_record": False,
            "failed": False,
            "verify_valid": False,
            "verify_dead": False,
        }

        try:
            # 通过转移记录查找源文件路径
            transfer_his = self._transferhis.get_by_dest(dest=strm_path)
            if not transfer_his:
                # 尝试用文件名模糊匹配
                strm_name = Path(strm_path).stem
                transfer_his_list = self._transferhis.get_by_title(strm_name)
                if transfer_his_list:
                    # 取dest路径最匹配的
                    for th in transfer_his_list:
                        if th.dest and th.dest == strm_path:
                            transfer_his = th
                            break
                    if not transfer_his:
                        transfer_his = transfer_his_list[0]

            if not transfer_his:
                logger.warning(f"未找到转移记录: {strm_path}")
                # 尝试远程可达性验证
                if self._verify_enabled and self.__can_verify():
                    reachable, strm_url = self.__verify_strm_url(strm_path)
                    if reachable:
                        logger.info(f"STRM远程有效，保留: {strm_url}")
                        result["verify_valid"] = True
                        self.__save_check_record(
                            strm_path, strm_url, "远程有效", "保留",
                            title=Path(strm_path).stem
                        )
                        return result
                    # 验证不可达 → 执行清理
                    result["no_record"] = True
                    result["dead"] = True
                    result["verify_dead"] = True
                    # 清理Emby条目
                    emby_ok = self.__delete_emby_item_by_path(
                        emby_host=emby_host, emby_apikey=emby_apikey, strm_path=strm_path
                    )
                    if emby_ok:
                        result["emby_deleted"] = True
                    # 清理附属文件
                    if self._delete_sidecar:
                        if self.__delete_sidecar_files(strm_path):
                            result["sidecar_deleted"] = True
                    # 清理STRM文件
                    if self._delete_strm:
                        try:
                            os.remove(strm_path)
                            logger.info(f"已删除远程不可达STRM文件: {strm_path}")
                            result["strm_deleted"] = True
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
                    self.__save_check_record(
                        strm_path, strm_url, "远程不可达",
                        ", ".join(action_parts) if action_parts else "已清理",
                        title=Path(strm_path).stem
                    )
                    return result
                # 未启用验证或不在低峰时段
                result["no_record"] = True
                result["dead"] = True
                self.__save_check_record(strm_path, "", "无转移记录", "跳过")
                return result

            # 获取源文件路径
            src_path = transfer_his.src
            if not src_path:
                logger.warning(f"转移记录中无源路径: {strm_path}")
                # 同上的验证逻辑
                if self._verify_enabled and self.__can_verify():
                    reachable, strm_url = self.__verify_strm_url(strm_path)
                    if reachable:
                        logger.info(f"STRM远程有效，保留: {strm_url}")
                        result["verify_valid"] = True
                        self.__save_check_record(
                            strm_path, strm_url, "远程有效", "保留",
                            title=Path(strm_path).stem
                        )
                        return result
                    result["no_record"] = True
                    result["dead"] = True
                    result["verify_dead"] = True
                    emby_ok = self.__delete_emby_item_by_path(
                        emby_host=emby_host, emby_apikey=emby_apikey, strm_path=strm_path
                    )
                    if emby_ok:
                        result["emby_deleted"] = True
                    if self._delete_sidecar:
                        if self.__delete_sidecar_files(strm_path):
                            result["sidecar_deleted"] = True
                    if self._delete_strm:
                        try:
                            os.remove(strm_path)
                            logger.info(f"已删除远程不可达STRM文件: {strm_path}")
                            result["strm_deleted"] = True
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
                    self.__save_check_record(
                        strm_path, strm_url, "远程不可达",
                        ", ".join(action_parts) if action_parts else "已清理",
                        title=Path(strm_path).stem
                    )
                    return result
                result["no_record"] = True
                result["dead"] = True
                self.__save_check_record(strm_path, "", "无源路径", "跳过")
                return result

            title = transfer_his.title or ""

            # 检查源文件是否存在
            if os.path.exists(src_path):
                logger.debug(f"源STRM文件存在: {src_path}")
                self.__save_check_record(strm_path, src_path, "有效", "无操作", title=title)
                return result

            # 源文件不存在
            logger.warning(f"源STRM文件已删除: {src_path} (来自 {strm_path})")
            result["dead"] = True

            # 通过Emby API删除该STRM文件对应的单个条目
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
                strm_path, src_path, "源文件缺失",
                ", ".join(action_parts),
                title=title
            )

        except Exception as e:
            logger.error(f"检查STRM文件异常 {strm_path}: {str(e)}")
            result["failed"] = True
            self.__save_check_record(strm_path, "", "检查失败", str(e))

        return result

    def __delete_emby_item_by_path(self, emby_host: str, emby_apikey: str,
                                   strm_path: str) -> bool:
        """
        通过STRM文件路径精确匹配并删除Emby中的单个条目。
        只删除该STRM文件对应的条目，不会删除整个视频项目（同一视频的其他版本不受影响）。
        """
        try:
            search_url = f"{emby_host}emby/Items"

            # 先尝试用文件名搜索
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

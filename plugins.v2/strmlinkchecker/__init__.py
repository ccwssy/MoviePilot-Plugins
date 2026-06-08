import os
import random
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
from app.core.event import eventmanager
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
    plugin_version = "1.0.12"
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
    # URL可用性检查（风险功能）
    _url_check_enabled = False
    _url_check_threads = 1
    _url_check_cooldown = 5
    _url_check_daily_limit = 50
    _url_check_today_count = 0
    _url_check_date = None
    _url_check_count_lock = threading.Lock()

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
            # URL可用性检查（风险功能）
            self._url_check_enabled = config.get("url_check_enabled", False)
            self._url_check_threads = int(config.get("url_check_threads", 1))
            self._url_check_cooldown = int(config.get("url_check_cooldown", 5))
            self._url_check_daily_limit = int(config.get("url_check_daily_limit", 50))

        if self._enabled:
            if self._onlyonce:
                self._onlyonce = False
                # 先启动检查线程，再保存配置（避免update_config触发重载导致线程被销毁）
                logger.info(f"STRM源链接检查服务启动，立即运行一次")
                self._running = False
                check_thread = threading.Thread(target=self.__check_all)
                check_thread.start()
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
                    "url_check_enabled": self._url_check_enabled,
                    "url_check_threads": self._url_check_threads,
                    "url_check_cooldown": self._url_check_cooldown,
                    "url_check_daily_limit": self._url_check_daily_limit,
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
        logger.info("api_run_check 被调用，准备启动检查线程")
        # 重置运行标志，确保检查可以执行
        self._running = False
        thread = threading.Thread(target=self.__check_all)
        thread.start()
        logger.info("api_run_check 检查线程已启动")
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
                    # URL可用性检查（风险功能）
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
                                            'text': '⚠️ 风险功能：URL可用性检查。启用后，对于没有转移记录的STRM文件，'
                                                    '插件会通过HTTP请求模拟访问源链接来测试文件是否可用。'
                                                    '此功能可能触发网盘或文件服务器的风控机制，请谨慎使用并严格控制频率。'
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
                                            'model': 'url_check_enabled',
                                            'label': '启用URL可用性检查',
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
                                            'model': 'url_check_threads',
                                            'label': '线程数',
                                            'type': 'number',
                                            'min': 1,
                                            'max': 3,
                                            'suffix': '个',
                                            'placeholder': '1'
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
                                            'model': 'url_check_cooldown',
                                            'label': '冷却时间',
                                            'type': 'number',
                                            'min': 3,
                                            'max': 60,
                                            'suffix': '秒',
                                            'placeholder': '5'
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
                                            'model': 'url_check_daily_limit',
                                            'label': '每日检查上限',
                                            'type': 'number',
                                            'min': 10,
                                            'max': 500,
                                            'suffix': '次',
                                            'placeholder': '50'
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
            "url_check_enabled": False,
            "url_check_threads": 1,
            "url_check_cooldown": 5,
            "url_check_daily_limit": 50,
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
        no_record_items = [h for h in history if h.get('status') == '无转移记录' or h.get('status') == '无源路径']
        dead_items = [h for h in history if h.get('status') == '源文件缺失']
        failed_items = [h for h in history if h.get('status') == '检查失败']
        other_items = [h for h in history if h not in valid_items and h not in no_record_items
                       and h not in dead_items and h not in failed_items]

        def render_section(title: str, items: list, color: str, max_count: int = 20):
            total = len(items)
            title_text = f'{title} ({total})'
            if total > max_count:
                title_text += '（只显示部分）'
            if not items:
                return [
                    {
                        'component': 'div',
                        'props': {'class': 'mt-3 mb-1 font-weight-bold'},
                        'text': title_text
                    }
                ]
            cards = []
            for item in items[:max_count]:
                strm_path = item.get("strm_path", "")
                status = item.get("status", "")
                title_text_item = item.get("title", "")
                action = item.get("action", "")
                check_time = item.get("check_time", "")
                # 简化显示：只显示文件名和action
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
                            'text': f'{title_text_item} | {action} | {check_time}'
                        }
                    ]
                })
            return [
                {
                    'component': 'div',
                    'props': {'class': 'mt-3 mb-1 font-weight-bold'},
                    'text': title_text
                },
                {
                    'component': 'div',
                    'props': {'class': 'grid gap-2 grid-info-card'},
                    'content': cards
                }
            ]

        sections = []
        sections.extend(render_section('✅ 有效', valid_items, 'success', 10))
        sections.extend(render_section('⚠️ 无转移记录', no_record_items, 'warning', 20))
        sections.extend(render_section('❌ 源文件缺失', dead_items, 'error', 50))
        sections.extend(render_section('💥 检查失败', failed_items, 'error', 20))
        sections.extend(render_section('📋 其他', other_items, 'grey', 20))

        return sections

    def __check_all(self):
        """
        检查所有STRM文件
        """
        logger.info("__check_all 被调用，开始执行STRM源链接检查")
        if self._running:
            logger.warning("STRM源链接检查任务正在运行中，跳过本次执行")
            return

        self._running = True
        logger.info("__check_all 开始执行，清空历史记录")
        # 每次执行开始时清空历史记录，只保留当次执行数据
        self.save_data('history', [])
        # 用于等待URL检查线程完成
        url_threads = []
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
            total_url_dead = 0
            total_url_alive = 0
            total_url_skipped = 0
            total_url_cached = 0
            failed_items = []

            # URL可用性检查 - 重置每日计数（如果日期变了）
            self.__reset_daily_url_check_count()

            # 收集所有STRM文件路径
            all_strm_files = []
            for lib_dir in strm_dirs:
                if not os.path.isdir(lib_dir):
                    logger.warning(f"Emby媒体库STRM目录不存在: {lib_dir}")
                    continue
                logger.info(f"开始扫描Emby媒体库STRM目录: {lib_dir}")
                for root, dirs, files in os.walk(lib_dir):
                    for filename in files:
                        if not filename.lower().endswith('.strm'):
                            continue
                        all_strm_files.append(os.path.join(root, filename))

            if not all_strm_files:
                logger.info("未找到任何STRM文件")
                return

            # 如果启用了URL检查，随机打乱顺序以降低风控特征
            if self._url_check_enabled:
                random.shuffle(all_strm_files)

            # 用于URL检查的线程控制
            url_check_lock = threading.Lock()
            url_check_sem = threading.Semaphore(self._url_check_threads if self._url_check_enabled else 1)

            def url_check_worker(strm_path: str):
                """URL检查工作线程"""
                nonlocal total_url_dead, total_url_alive, total_url_skipped, total_url_cached
                with url_check_sem:
                    with self._url_check_count_lock:
                        if self._url_check_enabled and self._url_check_today_count >= self._url_check_daily_limit:
                            total_url_skipped += 1
                            # 记录跳过原因
                            self.__save_check_record(strm_path, "", "无转移记录", "跳过(URL检查已达上限)")
                            return
                    try:
                        result = self.__check_single_strm_url(strm_path)
                        with url_check_lock:
                            if result.get("url_dead"):
                                total_url_dead += 1
                            elif result.get("url_alive"):
                                total_url_alive += 1
                            else:
                                total_url_skipped += 1
                    except Exception as e:
                        logger.error(f"URL检查失败 {strm_path}: {str(e)}")
                        with url_check_lock:
                            total_url_skipped += 1

            # 处理每个STRM文件
            for strm_path in all_strm_files:
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
                    # 如果启用了URL检查且无转移记录，启动URL检查
                    if (self._url_check_enabled
                            and result.get("no_record")
                            and not result.get("url_checked")):
                        # 先检查缓存，如果缓存命中则直接跳过，不启动线程
                        cache = self.get_data('url_check_cache') or {}
                        cached_entry = cache.get(strm_path)
                        if cached_entry:
                            try:
                                current_mtime = os.path.getmtime(strm_path)
                            except OSError:
                                current_mtime = 0
                            cached_mtime = cached_entry.get("mtime", 0)
                            if abs(cached_mtime - current_mtime) < 0.001:
                                cached_status = cached_entry.get("status", "未知")
                                total_url_cached += 1
                                # 覆盖之前保存的"待URL检查"记录
                                history = self.get_data('history') or []
                                history = [h for h in history
                                           if not (h.get("strm_path") == strm_path
                                                   and h.get("action") == "待URL检查")]
                                history.append({
                                    "strm_path": strm_path,
                                    "source_url": "",
                                    "status": "无转移记录",
                                    "action": f"跳过(缓存命中,上次状态:{cached_status})",
                                    "title": "",
                                    "check_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                                })
                                self.save_data('history', history)
                                continue
                        # 缓存未命中，启动URL检查线程
                        t = threading.Thread(target=url_check_worker, args=(strm_path,))
                        t.start()
                        url_threads.append(t)
                except Exception as e:
                    logger.error(f"检查STRM文件失败 {strm_path}: {str(e)}")
                    failed_items.append(strm_path)

            # 等待所有URL检查线程完成
            for t in url_threads:
                t.join(timeout=300)

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
                if self._url_check_enabled:
                    msg_lines.append("")
                    msg_lines.append(f"🌐 URL可用性检查")
                    msg_lines.append(f"  ├ 链接有效: {total_url_alive} 个")
                    msg_lines.append(f"  ├ 链接失效: {total_url_dead} 个")
                    msg_lines.append(f"  ├ 缓存跳过: {total_url_cached} 个")
                    msg_lines.append(f"  └ 跳过(已达上限): {total_url_skipped} 个")
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
                        f"删除Emby{total_deleted_emby}个, "
                        f"删除STRM{total_deleted_strm}个, "
                        f"删除附属文件{total_deleted_sidecar}个")

        except Exception as e:
            logger.error(f"STRM源文件检查异常: {str(e)}")
        finally:
            self._running = False

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
            "url_checked": False,
        }

        try:
            # 通过转移记录查找源文件路径
            # 注意：get_by_dest 返回最早匹配的记录（.first()），
            # 当同一 dest 被多次覆盖整理时，最早记录的 src 可能指向已被删除的旧文件。
            # 因此需要检查是否有同 dest 的更新记录，取最新的（ID最大的）。
            transfer_his = self._transferhis.get_by_dest(dest=strm_path)
            if transfer_his:
                # 查找同 dest 的最新记录（按 ID 降序）
                from app.db.models.transferhistory import TransferHistory
                from app.db import ScopedSession
                _db = ScopedSession()
                try:
                    latest = _db.query(TransferHistory)\
                        .filter(TransferHistory.dest == strm_path)\
                        .order_by(TransferHistory.id.desc())\
                        .first()
                    if latest and latest.id != transfer_his.id:
                        logger.debug(f"同dest存在更新记录(ID={latest.id})，替换旧记录(ID={transfer_his.id})")
                        transfer_his = latest
                finally:
                    _db.close()
            if not transfer_his:
                # 尝试用文件名模糊匹配
                strm_name = Path(strm_path).stem
                transfer_his_list = self._transferhis.get_by_title(strm_name)
                if transfer_his_list:
                    # 取dest路径最匹配的，且取最新的（ID最大的）
                    matched = None
                    for th in transfer_his_list:
                        if th.dest and th.dest == strm_path:
                            if matched is None or th.id > matched.id:
                                matched = th
                    if not matched:
                        # 没有精确匹配dest的，取ID最大的
                        matched = max(transfer_his_list, key=lambda x: x.id or 0)
                    transfer_his = matched

            if not transfer_his:
                logger.warning(f"未找到转移记录: {strm_path}")
                result["no_record"] = True
                # 不标记为dead，由URL检查决定
                if not self._url_check_enabled:
                    result["dead"] = True
                    self.__save_check_record(strm_path, "", "无转移记录", "跳过")
                else:
                    self.__save_check_record(strm_path, "", "无转移记录", "待URL检查")
                return result

            # 获取源文件路径
            src_path = transfer_his.src
            if not src_path:
                logger.warning(f"转移记录中无源路径: {strm_path}")
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

    def __reset_daily_url_check_count(self):
        """
        重置每日URL检查计数（如果日期变了）
        """
        today = datetime.now().strftime("%Y-%m-%d")
        if self._url_check_date != today:
            self._url_check_today_count = 0
            self._url_check_date = today

    def __check_single_strm_url(self, strm_path: str) -> dict:
        """
        通过HTTP请求测试STRM文件中的URL是否可访问（无转移记录时使用）
        严格风控：单线程、冷却时间、每日上限
        已检查过的文件会缓存结果，下次mtime未变则跳过
        """
        result = {
            "strm_path": strm_path,
            "url_dead": False,
            "url_alive": False,
            "cached": False,
        }

        # 检查缓存：如果该文件已检查过且mtime未变，直接跳过
        cache = self.get_data('url_check_cache') or {}
        try:
            current_mtime = os.path.getmtime(strm_path)
        except OSError:
            current_mtime = 0

        cached_entry = cache.get(strm_path)
        if cached_entry:
            cached_mtime = cached_entry.get("mtime", 0)
            if abs(cached_mtime - current_mtime) < 0.001:
                logger.debug(f"URL检查缓存命中，跳过: {strm_path}")
                result["cached"] = True
                cached_status = cached_entry.get("status", "未知")
                result["cached_status"] = cached_status
                if cached_status == "有效":
                    result["url_alive"] = True
                elif cached_status == "失效":
                    result["url_dead"] = True
                return result

        # 检查每日上限
        with self._url_check_count_lock:
            if self._url_check_today_count >= self._url_check_daily_limit:
                logger.debug(f"URL检查已达每日上限({self._url_check_daily_limit})，跳过: {strm_path}")
                self.__save_check_record(strm_path, "", "无转移记录", "跳过(URL检查已达上限)")
                return result

        try:
            # 读取STRM文件内容获取URL
            with open(strm_path, 'r', encoding='utf-8', errors='ignore') as f:
                url = f.read().strip()

            if not url:
                logger.warning(f"STRM文件内容为空: {strm_path}")
                return result

            # 执行HTTP HEAD请求（轻量级，不下载实际内容）
            logger.info(f"URL可用性检查: {url[:100]}...")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Range": "bytes=0-0",  # 只请求第一个字节，最小化数据传输
                "Connection": "close",
            }

            # 使用HEAD请求（如果服务器不支持则降级为GET + Range）
            try:
                resp = requests.head(
                    url,
                    headers=headers,
                    timeout=15,
                    verify=False,
                    allow_redirects=True,
                )
                status_code = resp.status_code
            except requests.exceptions.RequestException:
                # HEAD不支持时，用GET + Range只取第一个字节
                resp = requests.get(
                    url,
                    headers=headers,
                    timeout=15,
                    verify=False,
                    allow_redirects=True,
                    stream=True,
                )
                status_code = resp.status_code
                resp.close()

            # 2xx 或 3xx 或 416（Range请求不支持但文件存在）都视为有效
            url_ok = status_code in [200, 201, 204, 206, 301, 302, 303, 307, 308, 416]

            # 更新计数
            with self._url_check_count_lock:
                self._url_check_today_count += 1

            if url_ok:
                logger.info(f"URL有效 [{status_code}]: {url[:100]}...")
                result["url_alive"] = True
                self.__save_check_record(
                    strm_path, url, "无转移记录",
                    f"URL有效(HTTP {status_code})",
                    title=Path(strm_path).stem
                )
                # 更新缓存
                self.__update_url_check_cache(strm_path, current_mtime, "有效")
            else:
                logger.warning(f"URL不可用 [{status_code}]: {url[:100]}...")
                result["url_dead"] = True
                self.__save_check_record(
                    strm_path, url, "无转移记录",
                    f"URL不可用(HTTP {status_code})",
                    title=Path(strm_path).stem
                )
                # 更新缓存
                self.__update_url_check_cache(strm_path, current_mtime, "失效")

        except Exception as e:
            logger.error(f"URL检查异常 {strm_path}: {str(e)}")
            with self._url_check_count_lock:
                self._url_check_today_count += 1
            self.__save_check_record(strm_path, "", "无转移记录", f"URL检查失败: {str(e)}")

        finally:
            # 冷却时间 - 每次请求后等待
            if self._url_check_cooldown > 0:
                logger.debug(f"URL检查冷却 {self._url_check_cooldown}秒...")
                time.sleep(self._url_check_cooldown)

        return result

    def __update_url_check_cache(self, strm_path: str, mtime: float, status: str):
        """
        更新URL检查缓存，记录已检查过的STRM文件及其mtime
        """
        cache = self.get_data('url_check_cache') or {}
        cache[strm_path] = {
            "mtime": mtime,
            "status": status,
            "checked_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
        # 限制缓存大小，最多保留10000条
        if len(cache) > 10000:
            # 按检查时间排序，移除最旧的
            sorted_items = sorted(cache.items(), key=lambda x: x[1].get("checked_at", ""))
            cache = dict(sorted_items[-8000:])
        self.save_data('url_check_cache', cache)

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
        # 如果是URL检查的最终结果或缓存跳过，先移除同路径的"待URL检查"中间记录
        if (action.startswith("URL有效") or action.startswith("URL不可用") 
                or action.startswith("URL检查失败") or action.startswith("跳过(缓存命中)")
                or action.startswith("跳过(URL检查已达上限)")):
            history = [h for h in history
                       if not (h.get("strm_path") == strm_path and h.get("action") == "待URL检查")]
        history.append({
            "strm_path": strm_path,
            "source_url": source_url[:200] if source_url else "",
            "status": status,
            "action": action,
            "title": title,
            "check_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        })
        if len(history) > 500:
            # 截断时优先保留重要记录：源文件缺失 > 检查失败 > 无转移记录 > 有效
            # 按优先级排序后保留前500条
            priority = {"源文件缺失": 0, "检查失败": 1, "无转移记录": 2, "无源路径": 2, "有效": 3}
            history.sort(key=lambda h: (
                priority.get(h.get("status", ""), 9),
                h.get("check_time", "")
            ))
            history = history[:500]
            # 恢复按时间倒序（数据看板按时间倒序展示）
            history.sort(key=lambda h: h.get("check_time", ""), reverse=True)
        self.save_data('history', history)

    @eventmanager.register(EventType.PluginAction)
    def serve(self, event, **kwargs):
        """
        处理插件事件
        """
        if event and event.get("action") == "strm_check":
            logger.info("收到 /strm_check 命令，开始执行STRM源链接检查")
            self._running = False
            thread = threading.Thread(target=self.__check_all)
            thread.start()

    def stop_service(self):
        """
        停止插件
        """
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._scheduler.shutdown()
            self._scheduler = None

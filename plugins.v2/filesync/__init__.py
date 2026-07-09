import datetime
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.utils.system import SystemUtils


class FileSync(_PluginBase):
    """
    文件同步插件。
    监控源目录文件变化，按目录结构不变同步复制或创建软链接到目的目录。
    支持全量同步、增量监控、源文件删除时同步清理目的文件。
    """

    plugin_name = "文件同步"
    plugin_desc = "监控源目录文件变化，按目录结构不变同步复制或创建软链接到目的目录，支持附属文件清理。"
    plugin_icon = "filesync.png"
    plugin_version = "1.0.0"
    plugin_author = "local"
    author_url = ""
    plugin_config_prefix = "filesync_"
    plugin_order = 31
    auth_level = 1

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _observer: Optional[Observer] = None
    _enabled = False
    _onlyonce = False
    _sync_mode = "copy"  # copy 或 softlink
    _dir_pairs: Dict[str, str] = {}  # 源目录 -> 目的目录 映射
    _file_extensions = ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png"
    _delete_sidecar = False
    _sidecar_extensions = ".jpg, .nfo, .png, .srt, .ass, .sub"
    _notify = False
    _running = False
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._dir_pairs = {}

        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._sync_mode = config.get("sync_mode", "copy")
            self._file_extensions = config.get("file_extensions", ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png")
            self._delete_sidecar = config.get("delete_sidecar", False)
            self._sidecar_extensions = config.get("sidecar_extensions", ".jpg, .nfo, .png, .srt, .ass, .sub")
            self._notify = config.get("notify", False)

            # 解析多行目录映射：每行格式 "源目录:目的目录"
            dir_config = config.get("dir_config", "").strip()
            if dir_config:
                for line in dir_config.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    if ":" in line:
                        parts = line.split(":", 1)
                        src = parts[0].strip()
                        dst = parts[1].strip()
                        if src and dst:
                            self._dir_pairs[src] = dst

        if not self._enabled and not self._onlyonce:
            return

        if not self._dir_pairs:
            logger.error("未配置目录映射")
            return

        # 验证所有目录
        for src, dst in self._dir_pairs.items():
            src_path = Path(src)
            dst_path = Path(dst)
            if not src_path.exists():
                logger.error(f"源目录不存在: {src}")
                return
            if not dst_path.exists():
                logger.info(f"目的目录不存在，尝试创建: {dst}")
                try:
                    dst_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logger.error(f"创建目的目录失败: {e}")
                    return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._onlyonce:
            self._onlyonce = False
            self.update_config({
                "onlyonce": False,
                "enabled": self._enabled,
                "sync_mode": self._sync_mode,
                "dir_config": config.get("dir_config", ""),
                "file_extensions": self._file_extensions,
                "delete_sidecar": self._delete_sidecar,
                "sidecar_extensions": self._sidecar_extensions,
                "notify": self._notify,
            })
            logger.info("文件同步服务启动，立即运行一次全量同步")
            self._scheduler.add_job(
                func=self.__full_sync,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                name="文件同步全量同步"
            )

        if self._enabled:
            # 为每对目录启动实时监控
            for src, dst in self._dir_pairs.items():
                self.__start_watcher(Path(src), Path(dst))

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def __start_watcher(self, source_path: Path, target_path: Path):
        """启动文件系统监控。"""
        try:
            self._observer = Observer()
            event_handler = FileSyncHandler(
                source_path=source_path,
                target_path=target_path,
                sync_mode=self._sync_mode,
                file_extensions=[ext.strip() for ext in self._file_extensions.split(",") if ext.strip()],
                delete_sidecar=self._delete_sidecar,
                sidecar_extensions=[ext.strip() for ext in self._sidecar_extensions.split(",") if ext.strip()],
                plugin=self
            )
            self._observer.schedule(event_handler, str(source_path), recursive=True)
            self._observer.start()
            logger.info(f"文件实时监控已启动: {source_path}")
        except Exception as e:
            logger.error(f"启动文件监控失败: {e}")

    def __full_sync(self):
        """全量同步：遍历所有源目录，同步到对应的目的目录。"""
        if self._running:
            logger.warning("全量同步任务正在运行中，跳过本次执行")
            return

        self._running = True
        try:
            extensions = [ext.strip() for ext in self._file_extensions.split(",") if ext.strip()]
            total_all = 0
            sync_all = 0
            skip_all = 0
            fail_all = 0

            for source_dir, target_dir in self._dir_pairs.items():
                source_path = Path(source_dir)
                target_path = Path(target_dir)

                if not source_path.exists():
                    logger.warning(f"源目录不存在，跳过: {source_dir}")
                    continue

                logger.info(f"全量同步: {source_dir} -> {target_dir}")
                files = SystemUtils.list_files(source_path, extensions=extensions)
                total = len(files)
                sync_count = 0
                skip_count = 0
                fail_count = 0

                for file_path in files:
                    rel_path = file_path.relative_to(source_path)
                    dest_path = target_path / rel_path

                    if dest_path.exists():
                        skip_count += 1
                        continue

                    dest_path.parent.mkdir(parents=True, exist_ok=True)

                    success = self.__sync_file(file_path, dest_path)
                    if success:
                        sync_count += 1
                        self.__save_sync_record(str(file_path), str(dest_path), "已同步")
                    else:
                        fail_count += 1

                logger.info(f"  {source_dir}: 同步{sync_count}个, 跳过{skip_count}个, 失败{fail_count}个")
                total_all += total
                sync_all += sync_count
                skip_all += skip_count
                fail_all += fail_count

            logger.info(f"全量同步完成: 同步{sync_all}个, 跳过{skip_all}个, 失败{fail_all}个")

            if self._notify:
                mode_text = "复制" if self._sync_mode == "copy" else "创建软链接"
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="📁 文件同步 · 全量同步完成",
                    text=(
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📊 同步统计\n"
                        f"  ├ 模式: {mode_text}\n"
                        f"  ├ 目录对数: {len(self._dir_pairs)}\n"
                        f"  ├ 总文件: {total_all} 个\n"
                        f"  ├ 已同步: {sync_all} 个\n"
                        f"  ├ 已跳过: {skip_all} 个\n"
                        f"  └ 失败: {fail_all} 个\n"
                        f"━━━━━━━━━━━━━━━━━━"
                    )
                )

        except Exception as e:
            logger.error(f"全量同步异常: {e}")
        finally:
            self._running = False

    def __sync_file(self, src: Path, dest: Path) -> bool:
        """执行单个文件的同步（复制或创建软链接）。"""
        try:
            if self._sync_mode == "softlink":
                # 创建软链接
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                dest.symlink_to(src)
                logger.debug(f"软链接: {src} -> {dest}")
            else:
                # 复制文件
                shutil.copy2(src, dest)
                logger.debug(f"复制: {src} -> {dest}")
            return True
        except Exception as e:
            logger.error(f"同步失败 {src} -> {dest}: {e}")
            return False

    def __delete_dest_file(self, src_path: str):
        """根据源文件路径查找对应的目录对，删除目的文件及附属文件。"""
        try:
            src = Path(src_path)
            for source_dir, target_dir in self._dir_pairs.items():
                source_path = Path(source_dir)
                if not str(src).startswith(str(source_path)):
                    continue

                rel_path = src.relative_to(source_path)
                dest = Path(target_dir) / rel_path

                # 删除目的文件
                if dest.exists() or dest.is_symlink():
                    logger.info(f"删除目的文件: {dest}")
                    dest.unlink()
                    self.__save_sync_record(src_path, str(dest), "已删除(源文件删除)")

                # 删除附属文件
                if self._delete_sidecar:
                    stem = dest.stem
                    parent = dest.parent
                    sidecar_exts = [ext.strip() for ext in self._sidecar_extensions.split(",") if ext.strip()]
                    for ext in sidecar_exts:
                        sidecar_file = parent / f"{stem}{ext}"
                        if sidecar_file.exists() or sidecar_file.is_symlink():
                            logger.info(f"删除附属文件: {sidecar_file}")
                            sidecar_file.unlink()

                # 递归删除空目录
                self.__remove_empty_parent(dest.parent, Path(target_dir))
                return  # 找到匹配的目录对后退出

        except Exception as e:
            logger.error(f"删除目的文件失败 {src_path}: {e}")

    def __remove_empty_parent(self, directory: Path, target_root: Path):
        """递归删除空父目录。"""
        try:
            if not directory.exists():
                return
            if any(directory.iterdir()):
                return
            logger.info(f"删除空目录: {directory}")
            directory.rmdir()
            # 递归检查上一级
            if directory.parent != target_root:
                self.__remove_empty_parent(directory.parent, target_root)
        except Exception as e:
            logger.debug(f"删除空目录失败 {directory}: {e}")

    def __save_sync_record(self, src: str, dest: str, action: str):
        """保存同步记录。"""
        records = self.get_data("sync_records") or []
        records.append({
            "src": src,
            "dest": dest,
            "action": action,
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        })
        # 最多保留 5000 条
        if len(records) > 5000:
            records = records[-5000:]
        self.save_data("sync_records", records)

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/filesync",
                "event": EventType.PluginAction,
                "desc": "文件同步全量同步",
                "category": "媒体管理",
                "data": {"action": "full_sync"}
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/full_sync",
                "endpoint": self.api_full_sync,
                "methods": ["GET"],
                "summary": "手动触发全量同步",
            }
        ]

    def api_full_sync(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        thread = threading.Thread(target=self.__full_sync)
        thread.start()
        return {"success": True, "message": "全量同步任务已启动"}

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    # ===== 基础设置 =====
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VCard',
                                        'props': {'variant': 'outlined'},
                                        'content': [
                                            {
                                                'component': 'VCardTitle',
                                                'props': {'class': 'pa-4 pb-0'},
                                                'content': [
                                                    {
                                                        'component': 'VRow',
                                                        'props': {'dense': True},
                                                        'content': [
                                                            {
                                                                'component': 'VCol',
                                                                'props': {'cols': 12, 'md': 4, 'sm': 4},
                                                                'content': [
                                                                    {
                                                                        'component': 'VSwitch',
                                                                        'props': {
                                                                            'model': 'enabled',
                                                                            'label': '⚙️ 启用插件',
                                                                            'color': 'primary',
                                                                            'hideDetails': True,
                                                                            'density': 'compact'
                                                                        }
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                'component': 'VCol',
                                                                'props': {'cols': 12, 'md': 4, 'sm': 4},
                                                                'content': [
                                                                    {
                                                                        'component': 'VSwitch',
                                                                        'props': {
                                                                            'model': 'onlyonce',
                                                                            'label': '立即全量同步',
                                                                            'color': 'primary',
                                                                            'hideDetails': True,
                                                                            'density': 'compact'
                                                                        }
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                'component': 'VCol',
                                                                'props': {'cols': 12, 'md': 4, 'sm': 4},
                                                                'content': [
                                                                    {
                                                                        'component': 'VSwitch',
                                                                        'props': {
                                                                            'model': 'notify',
                                                                            'label': '发送通知',
                                                                            'color': 'primary',
                                                                            'hideDetails': True,
                                                                            'density': 'compact'
                                                                        }
                                                                    }
                                                                ]
                                                            }
                                                        ]
                                                    }
                                                ]
                                            },
                                            {
                                                'component': 'VCardText',
                                                'props': {'class': 'pa-4 pt-4'},
                                                'content': [
                                                    {
                                                        'component': 'VRow',
                                                        'content': [
                                                            {
                                                                'component': 'VCol',
                                                                'props': {'cols': 12, 'md': 8, 'offset-md': 2},
                                                                'content': [
                                                                    {
                                                                        'component': 'VSelect',
                                                                        'props': {
                                                                            'model': 'sync_mode',
                                                                            'label': '同步模式',
                                                                            'items': [
                                                                                {'title': '复制文件', 'value': 'copy'},
                                                                                {'title': '创建软链接', 'value': 'softlink'}
                                                                            ],
                                                                            'hideDetails': True,
                                                                            'density': 'compact'
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
                                                                'props': {'cols': 12, 'md': 8, 'offset-md': 2},
                                                                'content': [
                                                                    {
                                                                        'component': 'VTextarea',
                                                                        'props': {
                                                                            'model': 'dir_config',
                                                                            'label': '目录映射',
                                                                            'rows': 5,
                                                                            'placeholder': '/path/source1:/path/target1\n/path/source2:/path/target2\n说明：每行一对，源目录:目的目录，支持子路径。\n如 /path/source1 下有 /电影/国产电影/xx.mp4，\n/path/target1 下自动同步为 /电影/国产电影/xx.mp4',
                                                                            'hideDetails': True,
                                                                            'density': 'compact'
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
                                                                'props': {'cols': 12, 'md': 8, 'offset-md': 2},
                                                                'content': [
                                                                    {
                                                                        'component': 'VTextField',
                                                                        'props': {
                                                                            'model': 'file_extensions',
                                                                            'label': '同步文件后缀',
                                                                            'placeholder': '.strm, .mkv, .mp4',
                                                                            'hideDetails': True,
                                                                            'density': 'compact',
                                                                            'persistentHint': True,
                                                                            'hint': '逗号分隔，仅同步这些后缀的文件'
                                                                        }
                                                                    }
                                                                ]
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    # ===== 文件过滤 =====
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VCard',
                                        'props': {'variant': 'outlined'},
                                        'content': [
                                            {
                                                'component': 'VCardTitle',
                                                'props': {'class': 'pa-3 pb-0'},
                                                'content': [
                                                    {
                                                        'component': 'VAlert',
                                                        'props': {
                                                            'type': 'info',
                                                            'variant': 'tonal',
                                                            'density': 'compact',
                                                            'class': 'mb-0',
                                                            'text': '开启后，当源文件被删除时，除了删除目的目录中对应的文件外，还会查找并删除同名的附属文件（如封面、元数据、字幕等）。附属文件后缀在下方配置。'
                                                        }
                                                    }
                                                ]
                                            },
                                            {
                                                'component': 'VCardText',
                                                'props': {'class': 'pa-3'},
                                                'content': [
                                                    {
                                                        'component': 'VRow',
                                                        'props': {'dense': True},
                                                        'content': [
                                                            {
                                                                'component': 'VCol',
                                                                'props': {'cols': 12, 'md': 4, 'sm': 4},
                                                                'content': [
                                                                    {
                                                                        'component': 'VSwitch',
                                                                        'props': {
                                                                            'model': 'delete_sidecar',
                                                                            'label': '删除附属文件(jpg/nfo等)',
                                                                            'color': 'error',
                                                                            'hideDetails': True,
                                                                            'density': 'compact'
                                                                        }
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                'component': 'VCol',
                                                                'props': {'cols': 12, 'md': 8, 'sm': 8},
                                                                'content': [
                                                                    {
                                                                        'component': 'VTextField',
                                                                        'props': {
                                                                            'model': 'sidecar_extensions',
                                                                            'label': '附属文件后缀',
                                                                            'placeholder': '.jpg, .nfo, .png',
                                                                            'hideDetails': True,
                                                                            'density': 'compact',
                                                                            'persistentHint': True,
                                                                            'hint': '源文件删除时一并清理这些后缀的附属文件'
                                                                        }
                                                                    }
                                                                ]
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "sync_mode": "copy",
            "dir_config": "",
            "file_extensions": ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png",
            "delete_sidecar": False,
            "sidecar_extensions": ".jpg, .nfo, .png, .srt, .ass, .sub",
            "notify": False,
        }

    def get_page(self) -> Optional[List[dict]]:
        records = self.get_data("sync_records") or []
        if not records:
            return [
                {
                    'component': 'div',
                    'text': '暂无同步记录',
                    'props': {'class': 'text-center'}
                }
            ]
        records = sorted(records, key=lambda x: x.get('time', ''), reverse=True)[:100]
        cards = []
        for record in records:
            src = record.get("src", "")
            dest = record.get("dest", "")
            action = record.get("action", "")
            check_time = record.get("time", "")
            cards.append({
                'component': 'VCard',
                'props': {'class': 'mb-1'},
                'content': [
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2 py-1'},
                        'text': f'{Path(src).name}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {'class': 'pa-0 px-2 py-1 text-caption text-grey'},
                        'text': f'{action} | {dest} | {check_time}'
                    }
                ]
            })
        return [
            {
                'component': 'div',
                'props': {'class': 'mt-2 mb-1 font-weight-bold'},
                'text': f'同步记录（最近100条）'
            },
            {
                'component': 'div',
                'content': cards
            }
        ]

    def stop_service(self):
        """停止插件后台服务。"""
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join(timeout=5)
            except Exception as e:
                logger.debug(f"停止文件监控失败: {e}")
            self._observer = None

        if self._scheduler:
            try:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
            except Exception as e:
                logger.debug(f"停止调度器失败: {e}")
            self._scheduler = None


class FileSyncHandler(FileSystemEventHandler):
    """文件系统事件处理器，监控文件变化并同步。"""

    def __init__(self, source_path: Path, target_path: Path,
                 sync_mode: str, file_extensions: List[str],
                 delete_sidecar: bool, sidecar_extensions: List[str],
                 plugin: FileSync):
        self._source_path = source_path
        self._target_path = target_path
        self._sync_mode = sync_mode
        self._file_extensions = file_extensions
        self._delete_sidecar = delete_sidecar
        self._sidecar_extensions = sidecar_extensions
        self._plugin = plugin
        # 去重缓存，防止同一文件短时间内多次触发
        self._recent_events = {}
        self._lock = threading.Lock()

    def on_created(self, event):
        """文件创建时触发。"""
        if event.is_directory:
            return
        self.__process_event(event.src_path, "创建")

    def on_modified(self, event):
        """文件修改时触发。"""
        if event.is_directory:
            return
        self.__process_event(event.src_path, "修改")

    def on_deleted(self, event):
        """文件删除时触发。"""
        if event.is_directory:
            return
        self.__process_delete(event.src_path)

    def on_moved(self, event):
        """文件移动/重命名时触发。"""
        if event.is_directory:
            return
        # 源文件移动走视为删除
        self.__process_delete(event.src_path)
        # 目标位置视为创建
        self.__process_event(event.dest_path, "移动")

    def __should_process(self, src_path: str) -> bool:
        """检查文件后缀是否在同步列表中。"""
        ext = Path(src_path).suffix.lower()
        return any(ext == e.lower() for e in self._file_extensions)

    def __is_sidecar(self, src_path: str) -> bool:
        """检查文件是否是附属文件后缀。"""
        ext = Path(src_path).suffix.lower()
        return any(ext == e.lower() for e in self._sidecar_extensions)

    def __dedup(self, src_path: str) -> bool:
        """去重：同一文件 2 秒内不重复处理。"""
        now = time.time()
        with self._lock:
            last = self._recent_events.get(src_path, 0)
            if now - last < 2:
                return False
            self._recent_events[src_path] = now
        return True

    def __process_event(self, src_path: str, event_type: str):
        """处理文件创建/修改事件。"""
        if not self.__should_process(src_path):
            return
        if not self.__dedup(src_path):
            return

        try:
            src = Path(src_path)
            if not src.exists():
                return

            rel_path = src.relative_to(self._source_path)
            dest = self._target_path / rel_path

            # 确保目的子目录存在
            dest.parent.mkdir(parents=True, exist_ok=True)

            if self._sync_mode == "softlink":
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                dest.symlink_to(src)
                logger.info(f"实时同步(软链接): {src} -> {dest}")
            else:
                shutil.copy2(src, dest)
                logger.info(f"实时同步(复制): {src} -> {dest}")

            self._plugin._FileSync__save_sync_record(str(src), str(dest), f"已同步({event_type})")

        except Exception as e:
            logger.error(f"实时同步失败 {src_path}: {e}")

    def __process_delete(self, src_path: str):
        """处理文件删除事件。"""
        if not self.__should_process(src_path) and not self.__is_sidecar(src_path):
            return
        if not self.__dedup(src_path):
            return

        self._plugin._FileSync__delete_dest_file(src_path)

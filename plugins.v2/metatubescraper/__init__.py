from typing import Any, List, Dict, Optional, Tuple
from pathlib import Path
from xml.sax.saxutils import escape
import os
import re
import shutil
import threading
import time
import httpx

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, ChainEventType, NotificationType
from app.core.config import settings
from app.utils.system import SystemUtils


class MetaTubeScraper(_PluginBase):
    """
    整合文件同步与番号刮削。
    同步模式：监控源目录，按目录结构不变复制/软链接到目的目录。
    刮削模式：监控源目录，用 MetaTube 识别番号视频，自动整理入库并写 NFO/海报。
    """

    plugin_name = "MetaTube 刮削器"
    plugin_desc = "文件同步 + 番号刮削：监控源目录同步文件，或识别番号视频自动整理入库并写入 NFO 文件及海报。"
    plugin_icon = "metatube.png"
    plugin_version = "1.5.0"
    plugin_author = "ccwssy"
    author_url = "https://github.com/ccwssy"
    plugin_config_prefix = "metatubescraper_"
    plugin_order = 50
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _notify = True
    _history: List[dict] = []
    _observer: Optional[Observer] = None
    _event = threading.Event()

    _sync_enabled = False
    _sync_mode = "copy"
    _dir_pairs: Dict[str, str] = {}
    _sync_extensions = ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png"
    _delete_sidecar = False
    _sidecar_extensions = ".jpg, .nfo, .png, .srt, .ass, .sub"
    _sync_running = False

    _scrape_enabled = False
    _metatube_url = "http://192.168.2.4:8897"
    _metatube_token = ""
    _translate_enabled = False
    _translate_engine = "GoogleFree"
    _translate_params = ""
    _keyword_pattern = r"^[A-Za-z]{1,6}-\d{2,}(?:-[A-Z0-9]+)?$"
    _sync_extensions = ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png"
    _organize_mode = "copy"
    _download_images = True
    _cover_type = "primary"
    _skip_existing = True

    def init_plugin(self, config: dict = None):
        self.stop_service()
        config = config or {}
        self._enabled = config.get("enabled", False)
        self._onlyonce = config.get("onlyonce", False)
        self._notify = config.get("notify", True)
        self._history = self.get_data("history") or []

        self._sync_enabled = config.get("sync_enabled", False)
        self._sync_mode = config.get("sync_mode", "copy")
        self._sync_extensions = config.get("sync_extensions", ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png")
        self._delete_sidecar = config.get("delete_sidecar", False)
        self._sidecar_extensions = config.get("sidecar_extensions", ".jpg, .nfo, .png, .srt, .ass, .sub")
        self._dir_pairs = {}
        dir_config = config.get("dir_config", "").strip()
        if dir_config:
            for line in dir_config.split("\n"):
                line = line.strip()
                if not line or ":" not in line:
                    continue
                parts = line.split(":", 1)
                src, dst = parts[0].strip(), parts[1].strip()
                if src and dst:
                    self._dir_pairs[src] = dst

        self._scrape_enabled = config.get("scrape_enabled", False)
        self._metatube_url = config.get("metatube_url", "http://192.168.2.4:8897").rstrip("/")
        self._metatube_token = config.get("metatube_token", "")
        self._translate_enabled = config.get("translate_enabled", False)
        self._translate_engine = config.get("translate_engine", "GoogleFree")
        self._translate_params = config.get("translate_params", "")
        self._keyword_pattern = config.get("keyword_pattern", r"^[A-Za-z]{1,6}-\d{2,}(?:-[A-Z0-9]+)?$")
        self._sync_extensions = config.get("sync_extensions", ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png")
        self._organize_mode = config.get("organize_mode", "copy")
        self._download_images = config.get("download_images", True)
        self._cover_type = config.get("cover_type", "primary")
        self._skip_existing = config.get("skip_existing", True)

        if not self._enabled and not self._onlyonce:
            return

        if self._onlyonce:
            self._onlyonce = False
            self._save_config()
            if self._dir_pairs:
                logger.info("MetaTubeScraper: 立即运行一次全量扫描整理")
                threading.Thread(target=self._scan_and_organize).start()

        if self._enabled:
            self._start_watchers()

    def _save_config(self):
        self.update_config({
            "enabled": self._enabled, "onlyonce": False, "notify": self._notify,
            "sync_enabled": self._sync_enabled, "sync_mode": self._sync_mode,
            "dir_config": "\n".join(f"{k}:{v}" for k, v in self._dir_pairs.items()),
            "sync_extensions": self._sync_extensions,
            "delete_sidecar": self._delete_sidecar, "sidecar_extensions": self._sidecar_extensions,
            "scrape_enabled": self._scrape_enabled,
            "metatube_url": self._metatube_url, "metatube_token": self._metatube_token,
            "translate_enabled": self._translate_enabled, "translate_engine": self._translate_engine,
            "translate_params": self._translate_params,
            "keyword_pattern": self._keyword_pattern,
            "organize_mode": self._organize_mode,
            "download_images": self._download_images, "cover_type": self._cover_type,
            "skip_existing": self._skip_existing,
        })

    def _start_watchers(self):
        if not self._observer:
            self._observer = Observer()

        sync_exts = [e.strip().lower() for e in self._sync_extensions.split(",") if e.strip()]
        video_exts = sync_exts

        if self._sync_enabled and self._scrape_enabled:
            # 两模式同时启用：用统一 Handler，番号走刮削，非番号走同步
            for src, dst in self._dir_pairs.items():
                sp = Path(src)
                if not sp.exists():
                    continue
                self._observer.schedule(
                    HybridHandler(
                        source_path=sp, target_path=Path(dst),
                        sync_mode=self._sync_mode,
                        sync_extensions=sync_exts,
                        delete_sidecar=self._delete_sidecar,
                        sidecar_extensions=[e.strip() for e in self._sidecar_extensions.split(",") if e.strip()],
                        video_exts=video_exts,
                        keyword_pattern=self._keyword_pattern,
                        plugin=self,
                    ), str(sp), recursive=True)
                logger.info(f"混合监控: {src} -> {dst}（番号刮削，其他同步）")
        else:
            if self._sync_enabled:
                for src, dst in self._dir_pairs.items():
                    sp = Path(src)
                    if not sp.exists():
                        continue
                    self._observer.schedule(
                        SyncHandler(sp, Path(dst), self._sync_mode, sync_exts,
                                    self._delete_sidecar,
                                    [e.strip() for e in self._sidecar_extensions.split(",") if e.strip()],
                                    self),
                        str(sp), recursive=True)
                    logger.info(f"同步监控: {src} -> {dst}")
            if self._scrape_enabled:
                for src, dst in self._dir_pairs.items():
                    sp = Path(src)
                    if not sp.exists():
                        continue
                    self._observer.schedule(
                        ScrapeHandler(self, video_exts, Path(dst)),
                        str(sp), recursive=True)
                    logger.info(f"刮削监控: {src} -> {dst}")

        if self._observer._watches:
            self._observer.start()

    def _full_sync(self):
        if self._sync_running:
            return
        self._sync_running = True
        try:
            exts = [e.strip() for e in self._sync_extensions.split(",") if e.strip()]
            sync_all = skip_all = fail_all = 0
            for src_dir, dst_dir in self._dir_pairs.items():
                sp, dp = Path(src_dir), Path(dst_dir)
                if not sp.exists():
                    continue
                for f in SystemUtils.list_files(sp, extensions=exts):
                    dest = dp / f.relative_to(sp)
                    if dest.exists():
                        skip_all += 1
                        continue
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if self._sync_file(f, dest):
                        sync_all += 1
                        self._save_sync_record(str(f), str(dest), "已同步")
                    else:
                        fail_all += 1
            logger.info(f"全量同步完成: 同步{sync_all}, 跳过{skip_all}, 失败{fail_all}")
            if self._notify:
                self.post_message(mtype=NotificationType.Plugin, title="文件同步完成",
                                  text=f"同步{sync_all}个, 跳过{skip_all}个, 失败{fail_all}个")
        finally:
            self._sync_running = False

    def _sync_file(self, src: Path, dest: Path) -> bool:
        try:
            if self._sync_mode == "softlink":
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                dest.symlink_to(src)
            else:
                shutil.copy2(src, dest)
            return True
        except Exception as e:
            logger.error(f"同步失败 {src} -> {dest}: {e}")
            return False

    def _delete_dest_file(self, src_path: str):
        try:
            src = Path(src_path)
            for src_dir, dst_dir in self._dir_pairs.items():
                sp = Path(src_dir)
                if not str(src).startswith(str(sp)):
                    continue
                dest = Path(dst_dir) / src.relative_to(sp)
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                    self._save_sync_record(src_path, str(dest), "已删除(源文件删除)")
                if self._delete_sidecar:
                    stem, parent = dest.stem, dest.parent
                    for ext in [e.strip() for e in self._sidecar_extensions.split(",") if e.strip()]:
                        sf = parent / f"{stem}{ext}"
                        if sf.exists() or sf.is_symlink():
                            sf.unlink()
                self._remove_empty_parent(dest.parent, Path(dst_dir))
                return
        except Exception as e:
            logger.error(f"删除目的文件失败 {src_path}: {e}")

    def _remove_empty_parent(self, directory: Path, root: Path):
        try:
            if not directory.exists() or any(directory.iterdir()):
                return
            directory.rmdir()
            if directory.parent != root:
                self._remove_empty_parent(directory.parent, root)
        except Exception:
            pass

    def _save_sync_record(self, src: str, dest: str, action: str):
        records = self.get_data("sync_records") or []
        records.append({"src": src, "dest": dest, "action": action, "time": time.strftime("%Y-%m-%d %H:%M:%S")})
        if len(records) > 5000:
            records = records[-5000:]
        self.save_data("sync_records", records)

    def _scan_and_organize(self):
        if not self._dir_pairs:
            return
        video_exts = [e.strip().lower() for e in self._sync_extensions.split(",") if e.strip()]
        for src_dir, dst_dir in self._dir_pairs.items():
            for root, _dirs, files in os.walk(Path(src_dir)):
                for fname in files:
                    fp = Path(root) / fname
                    if fp.suffix.lower() in video_exts:
                        self._process_video(fp, Path(dst_dir))

    def _process_video(self, video_path: Path, target_base: Path = None):
        name, stem = video_path.name, video_path.stem
        try:
            if not re.search(self._keyword_pattern, stem):
                return
        except re.error:
            return
        if target_base is None:
            for src_dir, dst_dir in self._dir_pairs.items():
                if str(video_path).startswith(str(Path(src_dir))):
                    target_base = Path(dst_dir)
                    source_base = Path(src_dir)
                    break
            if target_base is None:
                return
        else:
            # 从 dir_pairs 查找对应的源目录
            source_base = None
            for src_dir, dst_dir in self._dir_pairs.items():
                if str(video_path).startswith(str(Path(src_dir))):
                    source_base = Path(src_dir)
                    break
        if self._skip_existing and self._find_existing(stem, target_base):
            return
        results = self._metatube_search(stem)
        if not results:
            self._add_history(name, "未识别", False)
            return
        movie = results[0]
        provider, movie_id = movie.get("provider", ""), movie.get("id", "")
        if not provider or not movie_id:
            self._add_history(name, "数据不完整", False)
            return
        full = self._metatube_get_movie(provider, movie_id) or movie
        title = full.get("title", movie.get("title", stem))
        # 翻译标题
        if self._translate_enabled and title:
            translated = self._translate_text(title)
            if translated:
                logger.info(f"MetaTubeScraper: 翻译标题 {title[:30]} -> {translated[:30]}")
                title = translated
        number = full.get("number", stem)
        num_clean = re.sub(r'[\\\\/:*?\"<>|]', "_", number)
        # 保持源目录结构，只把文件名改为番号
        if source_base:
            rel_dir = video_path.parent.relative_to(source_base)
            # 如果父目录名包含番号，不重复创建子目录
            if num_clean.lower() in video_path.parent.name.lower():
                dest_dir = target_base / rel_dir
            else:
                dest_dir = target_base / rel_dir / num_clean
        else:
            dest_dir = target_base / num_clean
        new_name = f"{num_clean}{video_path.suffix}"
        dest_path = dest_dir / new_name
        if dest_path.exists():
            return
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
            if self._organize_mode == "softlink":
                dest_path.symlink_to(video_path)
                logger.info(f"软链接: {video_path} -> {dest_path}")
            else:
                shutil.copy2(str(video_path), str(dest_path))
                logger.info(f"复制: {video_path} -> {dest_path}")
            (dest_dir / f"{num_clean}.nfo").write_text(self._build_nfo(full, num_clean), encoding="utf-8")
            if self._download_images:
                self._download_cover(provider, movie_id, dest_dir, num_clean)
            self._add_history(f"{name} -> {new_name}", title, True)
            if self._notify:
                self.post_message(mtype=NotificationType.Plugin, title="MetaTube 刮削整理完成",
                                  text=f"{name}\n-> {title}\n-> {dest_path}")
        except Exception as e:
            logger.error(f"整理失败 {name}: {e}")
            self._add_history(name, f"整理失败: {e}", False)

    def _find_existing(self, number: str, target_base: Path) -> bool:
        if not target_base.exists():
            return False
        video_exts = [e.strip().lower() for e in self._sync_extensions.split(",") if e.strip()]
        for item in target_base.rglob(f"*{number}*"):
            if item.is_dir() or (item.is_file() and item.suffix.lower() in video_exts):
                return True
        return False

    @eventmanager.register(ChainEventType.NameRecognize)
    def on_name_recognize(self, event: Event) -> None:
        if not self._enabled or not self._scrape_enabled:
            return
        event_data = getattr(event, "event_data", None) or {}
        title = event_data.get("title", "")
        if not title:
            return
        try:
            if not re.search(self._keyword_pattern, title):
                return
        except re.error:
            return
        results = self._metatube_search(title)
        if not results:
            return
        movie = results[0]
        mt_title, mt_number = movie.get("title", ""), movie.get("number", "")
        if not mt_title and not mt_number:
            return
        if isinstance(event_data, dict):
            event_data["name"] = mt_title or mt_number
            event_data["year"] = (movie.get("release_date", "")[:4] if movie.get("release_date") else "")
            event_data["source_plugin"] = "MetaTubeScraper"

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        if not self._enabled or not self._scrape_enabled:
            return
        ti = self._get_ti(event)
        if not ti:
            return
        target_path = self._get_path(ti, "target_item") or ""
        target_dir = self._get_path(ti, "target_diritem") or ""
        if not target_path and not target_dir:
            return
        video_exts = {e.strip().lower() for e in self._sync_extensions.split(",")}
        video_files = []
        p = Path(target_path)
        if p.is_file() and p.suffix.lower() in video_exts:
            video_files.append(p)
        elif p.is_dir():
            video_files.extend(f for f in p.iterdir() if f.is_file() and f.suffix.lower() in video_exts)
        if not video_files:
            for item in (ti.get("file_list_new") or []):
                fp = item.get("path", "") if isinstance(item, dict) else getattr(item, "path", "")
                if fp and Path(fp).suffix.lower() in video_exts:
                    video_files.append(Path(fp))
        if not video_files:
            return
        results = [r for vf in video_files if (r := self._scrape_video_nfo(vf))]
        if results and self._notify:
            ok = sum(1 for r in results if r.get("success"))
            text = f"总计 {len(results)} 个文件：{ok} 成功 {len(results)-ok} 失败"
            for r in results[:5]:
                text += f"\n- {r.get('file', '')} -> {r.get('title', '')}"
            self.post_message(title="MetaTube 刮削完成", text=text)

    def _scrape_video_nfo(self, video_path: Path) -> Optional[dict]:
        stem, name, parent = video_path.stem, video_path.name, video_path.parent
        nfo_path = parent / f"{stem}.nfo"
        if self._skip_existing and nfo_path.exists():
            return {"file": name, "title": "已跳过", "success": True}
        try:
            if not re.search(self._keyword_pattern, stem):
                return None
        except re.error:
            pass
        results = self._metatube_search(stem)
        if not results:
            self._add_history(name, "未找到", False)
            return {"file": name, "title": "未找到", "success": False}
        movie = results[0]
        provider, movie_id = movie.get("provider", ""), movie.get("id", "")
        if not provider or not movie_id:
            self._add_history(name, "数据不完整", False)
            return {"file": name, "title": "数据不完整", "success": False}
        full = self._metatube_get_movie(provider, movie_id) or movie
        title = full.get("title", movie.get("title", ""))
        # 翻译标题
        if self._translate_enabled and title:
            translated = self._translate_text(title)
            if translated:
                title = translated
        # 用翻译后的标题构建 NFO
        full["title"] = title
        nfo_path.write_text(self._build_nfo(full, stem), encoding="utf-8")
        if self._download_images:
            self._download_cover(provider, movie_id, parent, stem)
        title = full.get("title", movie.get("title", ""))
        self._add_history(name, title, True)
        return {"file": name, "title": title, "success": True}

    def _mt_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._metatube_token}"} if self._metatube_token else {}

    def _metatube_search(self, query: str) -> List[dict]:
        try:
            resp = httpx.get(f"{self._metatube_url}/v1/movies/search",
                             params={"q": query, "fallback": "true"},
                             headers=self._mt_headers(), timeout=30.0)
            if resp.status_code == 200:
                data = resp.json().get("data") or []
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"MetaTube 搜索异常: {e}")
        return []

    def _metatube_get_movie(self, provider: str, movie_id: str) -> Optional[dict]:
        try:
            resp = httpx.get(f"{self._metatube_url}/v1/movies/{provider}/{movie_id}",
                             headers=self._mt_headers(), timeout=30.0)
            if resp.status_code == 200:
                return resp.json().get("data")
        except Exception as e:
            logger.error(f"MetaTube 详情异常: {e}")
        return None

    def _download_cover(self, provider: str, movie_id: str, target_dir: Path, stem: str):
        hdrs = self._mt_headers()
        for cover_key, save_name in [
            (self._cover_type, "poster.jpg" if self._cover_type == "primary" else f"{stem}-thumb.jpg"),
            ("backdrop", "backdrop.jpg"),
        ]:
            try:
                resp = httpx.get(f"{self._metatube_url}/v1/images/{cover_key}/{provider}/{movie_id}",
                                 headers=hdrs, follow_redirects=True, timeout=30.0)
                if resp.status_code == 200:
                    (target_dir / save_name).write_bytes(resp.content)
            except Exception as e:
                logger.error(f"下载图片异常 {cover_key}: {e}")

    def _translate_text(self, text: str) -> Optional[str]:
        """调用 MetaTube 翻译 API 将日文标题翻译为中文。"""
        if not text:
            return None
        try:
            params = {"q": text, "from": "auto", "to": "zh", "engine": self._translate_engine}
            # 解析额外的翻译参数（如 baidu-app-id=xxx,baidu-app-key=xxx）
            if self._translate_params:
                for pair in self._translate_params.split(","):
                    pair = pair.strip()
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        params[k.strip()] = v.strip()
            resp = httpx.get(
                f"{self._metatube_url}/v1/translate",
                params=params,
                headers=self._mt_headers(),
                timeout=30.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                translated = data.get("data", {}).get("translatedText") or data.get("translatedText")
                if translated and translated != text:
                    return translated
        except Exception as e:
            logger.error(f"MetaTube 翻译异常: {e}")
        return None

    def _build_nfo(self, info: dict, default_number: str) -> str:
        title = escape(info.get("title") or default_number)
        number = escape(info.get("number") or default_number)
        summary = escape(info.get("summary") or "")
        director = escape(info.get("director") or "")
        maker = escape(info.get("maker") or "")
        label = escape(info.get("label") or "")
        series = escape(info.get("series") or "")
        score = info.get("score") or 0.0
        runtime = info.get("runtime") or 0
        release_date = info.get("release_date") or ""
        homepage = escape(info.get("homepage") or "")
        cover_url = escape(info.get("cover_url") or "")
        genres = info.get("genres") or []
        actors = info.get("actors") or []
        year = str(release_date)[:4] if release_date else ""
        xml = '<?xml version="1.0" encoding="utf-8" standalone="yes"?>\n<movie>\n'
        xml += f"  <title>{title}</title>\n"
        xml += f'  <originaltitle>{title} ({number})</originaltitle>\n'
        xml += f"  <sorttitle>{number}</sorttitle>\n"
        xml += f"  <plot>{summary}</plot>\n  <outline>{summary}</outline>\n"
        xml += f"  <mpaa>R18+</mpaa>\n  <country>Japan</country>\n"
        for g in genres:
            xml += f"  <genre>{escape(g)}</genre>\n"
        if director:
            xml += f"  <director>{director}</director>\n"
        if maker:
            xml += f"  <studio>{escape(maker)}</studio>\n"
        if series:
            xml += f"  <tag>{escape(series)}</tag>\n"
        if label:
            xml += f"  <label>{escape(label)}</label>\n"
        for a in actors:
            xml += f"  <actor>\n    <name>{escape(a)}</name>\n  </actor>\n"
        if score > 0:
            xml += f"  <rating>{score:.1f}</rating>\n  <userrating>{score:.1f}</userrating>\n"
        if runtime:
            xml += f"  <runtime>{runtime}</runtime>\n"
        if year:
            xml += f"  <year>{year}</year>\n"
        if release_date:
            xml += f"  <premiered>{release_date}</premiered>\n"
        xml += f"  <id>{number}</id>\n  <num>{number}</num>\n"
        xml += f'  <uniqueid type="metatube" default="true">{number}</uniqueid>\n'
        if homepage:
            xml += f"  <homepage>{homepage}</homepage>\n"
        if cover_url:
            xml += f"  <art>\n    <poster>{cover_url}</poster>\n  </art>\n"
        xml += "</movie>\n"
        return xml

    def _get_ti(self, event: Event) -> Optional[dict]:
        ed = event.event_data or {}
        ti = ed.get("transfer_info") or {}
        return ti if isinstance(ti, dict) else (ti.to_dict() if hasattr(ti, "to_dict") else vars(ti))

    def _get_path(self, ti: dict, key: str) -> str:
        item = ti.get(key) or {}
        return item.get("path", "") if isinstance(item, dict) else getattr(item, "path", "")

    def _add_history(self, file_name: str, title: str, success: bool):
        from datetime import datetime
        self._history.append({"file": file_name, "title": title, "success": success,
                              "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        if len(self._history) > 200:
            self._history = self._history[-200:]
        self.save_data("history", self._history)

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join(timeout=5)
            except Exception:
                pass
            self._observer = None

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {"cmd": "/metatube_scrape_all", "event": EventType.PluginAction,
             "desc": "扫描所有源目录重新整理", "category": "插件命令",
             "data": {"action": "metatube_scrape_all"}},
            {"cmd": "/filesync", "event": EventType.PluginAction,
             "desc": "文件同步全量同步", "category": "插件命令",
             "data": {"action": "full_sync"}},
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/history", "endpoint": self._get_history_api, "methods": ["GET"], "auth": "bear", "summary": "刮削历史"},
            {"path": "/scan", "endpoint": self._api_scan, "methods": ["GET"], "summary": "手动触发扫描整理"},
            {"path": "/full_sync", "endpoint": self._api_full_sync, "methods": ["GET"], "summary": "手动触发全量同步"},
        ]

    def _get_history_api(self) -> List[dict]:
        return self._history[-100:] if self._history else []

    def _api_scan(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        threading.Thread(target=self._scan_and_organize).start()
        return {"success": True, "message": "扫描整理任务已启动"}

    def _api_full_sync(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return {"success": False, "message": "API密钥错误"}
        threading.Thread(target=self._full_sync).start()
        return {"success": True, "message": "全量同步任务已启动"}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    # ===== 基础设置 =====
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol', 'props': {'cols': 12},
                            'content': [{
                                'component': 'VCard', 'props': {'variant': 'outlined'},
                                'content': [
                                    {
                                        'component': 'VCardTitle', 'props': {'class': 'pa-4 pb-0'},
                                        'content': [{
                                            'component': 'VRow', 'props': {'dense': True},
                                            'content': [
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4, 'sm': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]},
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4, 'sm': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即全量扫描', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]},
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4, 'sm': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]},
                                            ]
                                        }]
                                    },
                                ]
                            }]
                        }]
                    },
                    # ===== 文件同步模式 =====
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol', 'props': {'cols': 12},
                            'content': [{
                                'component': 'VCard', 'props': {'variant': 'outlined'},
                                'content': [
                                    {
                                        'component': 'VCardTitle', 'props': {'class': 'pa-2 pb-0'},
                                        'content': [{
                                            'component': 'VRow', 'props': {'dense': True},
                                            'content': [
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'sync_enabled', 'label': '文件同步模式', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]},
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect', 'props': {'model': 'sync_mode', 'label': '同步方式', 'items': [{'title': '复制文件', 'value': 'copy'}, {'title': '创建软链接', 'value': 'softlink'}], 'hideDetails': True, 'density': 'compact'}}]},
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'delete_sidecar', 'label': '删除附属文件', 'color': 'error', 'hideDetails': True, 'density': 'compact'}}]},
                                            ]
                                        }]
                                    },
                                    {
                                        'component': 'VCardText', 'props': {'class': 'pa-3'},
                                        'content': [
                                            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextarea', 'props': {'model': 'dir_config', 'label': '目录映射', 'rows': 4, 'placeholder': '/path/source1:/path/target1\n/path/source2:/path/target2', 'hideDetails': True, 'density': 'compact', 'persistentHint': True, 'hint': '每行一对，源目录:目的目录，支持子路径'}}]}]},
                                            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'sync_extensions', 'label': '同步文件后缀', 'placeholder': '.strm, .mkv, .mp4', 'hideDetails': True, 'density': 'compact', 'persistentHint': True, 'hint': '逗号分隔'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'sidecar_extensions', 'label': '附属文件后缀', 'placeholder': '.jpg, .nfo, .png', 'hideDetails': True, 'density': 'compact', 'persistentHint': True, 'hint': '逗号分隔'}}]}]},
                                        ]
                                    }
                                ]
                            }]
                        }]
                    },
                    # ===== 番号刮削模式 =====
                    {
                        'component': 'VRow',
                        'content': [{
                            'component': 'VCol', 'props': {'cols': 12},
                            'content': [{
                                'component': 'VCard', 'props': {'variant': 'outlined'},
                                'content': [
                                    {
                                        'component': 'VCardTitle', 'props': {'class': 'pa-2 pb-0'},
                                        'content': [{
                                            'component': 'VRow', 'props': {'dense': True},
                                            'content': [
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'scrape_enabled', 'label': '番号刮削模式', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]},
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect', 'props': {'model': 'organize_mode', 'label': '整理方式', 'items': [{'title': '复制文件', 'value': 'copy'}, {'title': '创建软链接', 'value': 'softlink'}], 'hideDetails': True, 'density': 'compact'}}]},
                                                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSelect', 'props': {'model': 'cover_type', 'label': '海报类型', 'items': [{'title': '主海报', 'value': 'primary'}, {'title': '缩略图', 'value': 'thumb'}], 'hideDetails': True, 'density': 'compact'}}]},
                                            ]
                                        }]
                                    },
                                    {
                                        'component': 'VCardText', 'props': {'class': 'pa-3'},
                                        'content': [
                                            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'metatube_url', 'label': 'MetaTube 地址', 'placeholder': 'http://192.168.2.4:8900', 'hideDetails': True, 'density': 'compact'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'metatube_token', 'label': 'Token（可选）', 'placeholder': '未设置则留空', 'hideDetails': True, 'density': 'compact'}}]}]},
                                            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'keyword_pattern', 'label': '番号正则', 'placeholder': r"^[A-Za-z]{1,6}-\d{2,}", 'hideDetails': True, 'density': 'compact', 'persistentHint': True, 'hint': '文件名匹配番号正则的视频走刮削，其他走同步'}}]}]},
                                            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'download_images', 'label': '下载海报', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'skip_existing', 'label': '跳过已有', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'translate_enabled', 'label': '标题翻译', 'color': 'primary', 'hideDetails': True, 'density': 'compact'}}]}]},
                                            {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSelect', 'props': {'model': 'translate_engine', 'label': '翻译引擎', 'items': [{'title': 'Google 免费', 'value': 'GoogleFree'}, {'title': 'Google 付费', 'value': 'Google'}, {'title': '百度', 'value': 'Baidu'}, {'title': 'DeepL', 'value': 'DeepL'}, {'title': 'OpenAI', 'value': 'OpenAi'}], 'hideDetails': True, 'density': 'compact'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'translate_params', 'label': '翻译参数', 'placeholder': 'baidu-app-id=xxx,baidu-app-key=xxx', 'hideDetails': True, 'density': 'compact', 'persistentHint': True, 'hint': 'API Key 等参数，逗号分隔'}}]}]},
                                        ]
                                    }
                                ]
                            }]
                        }]
                    },
                ]
            }
        ], {
            "enabled": False, "onlyonce": False, "notify": True,
            "sync_enabled": False, "sync_mode": "copy",
            "dir_config": "",
            "sync_extensions": ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png",
            "delete_sidecar": False,
            "sidecar_extensions": ".jpg, .nfo, .png, .srt, .ass, .sub",
            "scrape_enabled": False,
            "metatube_url": "http://192.168.2.4:8897", "metatube_token": "",
            "translate_enabled": False, "translate_engine": "GoogleFree", "translate_params": "",
            "keyword_pattern": r"^[A-Za-z]{1,6}-\d{2,}(?:-[A-Z0-9]+)?$",
            "sync_extensions": ".strm, .mkv, .mp4, .avi, .ts, .iso, .nfo, .jpg, .png",
            "organize_mode": "copy",
            "download_images": True, "cover_type": "primary", "skip_existing": True,
        }

    def get_page(self) -> List[dict]:
        sync_records = self.get_data("sync_records") or []
        if not self._history and not sync_records:
            return [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "暂无记录"}}]
        items = []
        if self._history:
            items.append({"component": "div", "props": {"class": "mt-2 mb-1 font-weight-bold"}, "text": "刮削记录（最近50条）"})
            for h in self._history[-50:]:
                items.append({
                    'component': 'VCard', 'props': {'class': 'mb-1'},
                    'content': [
                        {'component': 'VCardText', 'props': {'class': 'pa-0 px-2 py-1'}, 'text': h.get("file", "")},
                        {'component': 'VCardText', 'props': {'class': 'pa-0 px-2 py-1 text-caption text-grey'}, 'text': f'{h.get("title","")} | {"成功" if h.get("success") else "失败"} | {h.get("time","")}'},
                    ]
                })
        if sync_records:
            items.append({"component": "div", "props": {"class": "mt-2 mb-1 font-weight-bold"}, "text": "同步记录（最近100条）"})
            for r in sorted(sync_records, key=lambda x: x.get('time', ''), reverse=True)[:100]:
                items.append({
                    'component': 'VCard', 'props': {'class': 'mb-1'},
                    'content': [
                        {'component': 'VCardText', 'props': {'class': 'pa-0 px-2 py-1'}, 'text': Path(r.get("src", "")).name},
                        {'component': 'VCardText', 'props': {'class': 'pa-0 px-2 py-1 text-caption text-grey'}, 'text': f'{r.get("action","")} | {r.get("dest","")} | {r.get("time","")}'},
                    ]
                })
        return items


class SyncHandler(FileSystemEventHandler):
    def __init__(self, source_path: Path, target_path: Path, sync_mode: str,
                 file_extensions: List[str], delete_sidecar: bool,
                 sidecar_extensions: List[str], plugin: MetaTubeScraper):
        self._source_path = source_path
        self._target_path = target_path
        self._sync_mode = sync_mode
        self._file_extensions = file_extensions
        self._delete_sidecar = delete_sidecar
        self._sidecar_extensions = sidecar_extensions
        self._plugin = plugin
        self._recent = {}
        self._lock = threading.Lock()

    def on_created(self, event):
        if event.is_directory:
            return
        self._process(event.src_path, "创建")

    def on_modified(self, event):
        if event.is_directory:
            return
        self._process(event.src_path, "修改")

    def on_deleted(self, event):
        if event.is_directory:
            return
        self._delete(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self._delete(event.src_path)
        self._process(event.dest_path, "移动")

    def _should_process(self, path: str) -> bool:
        return Path(path).suffix.lower() in [e.lower() for e in self._file_extensions]

    def _is_sidecar(self, path: str) -> bool:
        return Path(path).suffix.lower() in [e.lower() for e in self._sidecar_extensions]

    def _dedup(self, path: str) -> bool:
        now = time.time()
        with self._lock:
            if now - self._recent.get(path, 0) < 2:
                return False
            self._recent[path] = now
        return True

    def _process(self, src_path: str, event_type: str):
        if not self._should_process(src_path) or not self._dedup(src_path):
            return
        try:
            src = Path(src_path)
            if not src.exists():
                return
            dest = self._target_path / src.relative_to(self._source_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if self._sync_mode == "softlink":
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                dest.symlink_to(src)
            else:
                shutil.copy2(src, dest)
            self._plugin._save_sync_record(str(src), str(dest), f"已同步({event_type})")
        except Exception as e:
            logger.error(f"实时同步失败 {src_path}: {e}")

    def _delete(self, src_path: str):
        if not self._should_process(src_path) and not self._is_sidecar(src_path):
            return
        if not self._dedup(src_path):
            return
        self._plugin._delete_dest_file(src_path)


class ScrapeHandler(FileSystemEventHandler):
    def __init__(self, plugin: MetaTubeScraper, video_exts: List[str], target_base: Path):
        self._plugin = plugin
        self._video_exts = video_exts
        self._target_base = target_base
        self._recent = {}
        self._lock = threading.Lock()

    def on_created(self, event):
        if event.is_directory:
            return
        if Path(event.src_path).suffix.lower() not in self._video_exts:
            return
        if not self._dedup(event.src_path):
            return
        self._plugin._process_video(Path(event.src_path), self._target_base)

    def _dedup(self, path: str) -> bool:
        now = time.time()
        with self._lock:
            if now - self._recent.get(path, 0) < 3:
                return False
            self._recent[path] = now
        return True


class HybridHandler(FileSystemEventHandler):
    """
    两模式同时启用时的统一处理器。
    匹配番号正则的视频文件 -> 刮削模式
    其他文件 -> 同步模式
    """

    def __init__(self, source_path: Path, target_path: Path,
                 sync_mode: str, sync_extensions: List[str],
                 delete_sidecar: bool, sidecar_extensions: List[str],
                 video_exts: List[str], keyword_pattern: str,
                 plugin: MetaTubeScraper):
        self._source_path = source_path
        self._target_path = target_path
        self._sync_mode = sync_mode
        self._sync_extensions = sync_extensions
        self._delete_sidecar = delete_sidecar
        self._sidecar_extensions = sidecar_extensions
        self._video_exts = video_exts
        self._keyword_pattern = keyword_pattern
        self._plugin = plugin
        self._recent = {}
        self._lock = threading.Lock()

    def on_created(self, event):
        if event.is_directory:
            return
        self._route(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._route(event.src_path)

    def on_deleted(self, event):
        if event.is_directory:
            return
        self._delete(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self._delete(event.src_path)
        self._route(event.dest_path)

    def _is_video(self, path: str) -> bool:
        return Path(path).suffix.lower() in self._video_exts

    def _is_sync_file(self, path: str) -> bool:
        return Path(path).suffix.lower() in [e.lower() for e in self._sync_extensions]

    def _is_sidecar(self, path: str) -> bool:
        return Path(path).suffix.lower() in [e.lower() for e in self._sidecar_extensions]

    def _match_keyword(self, path: str) -> bool:
        stem = Path(path).stem
        try:
            return bool(re.search(self._keyword_pattern, stem))
        except re.error:
            return False

    def _dedup(self, path: str) -> bool:
        now = time.time()
        with self._lock:
            if now - self._recent.get(path, 0) < 2:
                return False
            self._recent[path] = now
        return True

    def _route(self, src_path: str):
        if not self._dedup(src_path):
            return
        # 番号视频 -> 刮削
        if self._is_video(src_path) and self._match_keyword(src_path):
            self._plugin._process_video(Path(src_path))
            return
        # 其他文件 -> 同步
        if self._is_sync_file(src_path):
            self._sync(src_path)

    def _sync(self, src_path: str):
        try:
            src = Path(src_path)
            if not src.exists():
                return
            dest = self._target_path / src.relative_to(self._source_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if self._sync_mode == "softlink":
                if dest.exists() or dest.is_symlink():
                    dest.unlink()
                dest.symlink_to(src)
            else:
                shutil.copy2(src, dest)
            self._plugin._save_sync_record(str(src), str(dest), "已同步(实时)")
        except Exception as e:
            logger.error(f"混合模式同步失败 {src_path}: {e}")

    def _delete(self, src_path: str):
        if not self._is_sync_file(src_path) and not self._is_sidecar(src_path):
            return
        if not self._dedup(src_path):
            return
        self._plugin._delete_dest_file(src_path)

#!/usr/bin/env python3
"""
农村居民收入数据爬取脚本
目标数据库: 复旦大学图书馆 -> CNKI 中国经济社会大数据平台 -> 统计资料 -> 统计年鉴

当前版本先解决两件事：
1. 稳定进入统计年鉴检索页
2. 按城市/年份定位年鉴，尽量在页面内提取收入字段

后续真正稳定的方案应当是：
搜索目标表 -> 下载 Excel -> 用 pandas/openpyxl 解析。
"""

import argparse
from dataclasses import dataclass
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from selenium import webdriver
from selenium.common.exceptions import (
    ElementNotInteractableException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ══════════════════════════════════════════════════════════════════
# 配置
# ══════════════════════════════════════════════════════════════════

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

CITY_LIST_FILE = DATA_DIR / "city_list.xlsx"
FUDAN_DB_NAV_URL = "https://libdbnav.fudan.edu.cn/database/navigation"
CNKI_HOME_URL = "https://data.cnki.net/"
CNKI_YEARBOOK_URL = "https://data.cnki.net/yearBook?type=type&code=A"
CNKI_API_BASE = "https://data.cnki.net/api/csyd"

WAIT_TIMEOUT = 30
PAGE_PAUSE = 2.0
SEARCH_PAUSE = 4.0

YEARBOOK_SEARCH_PLACEHOLDER = "年鉴关键字"

TABLE_KEYWORDS = [
    "农村居民人均纯收入",
    "农村居民收入",
    "农村居民收支",
    "农村住户收支",
    "纯收入",
    "收支",
    "住户",
    "农民",
    "人民生活",
    "农村",
    "人均",
    "收入",
]
ENTRY_SEARCH_KEYWORDS = [
    "农村",
    "农村居民收入",
    "农村居民收支",
    "农村住户收支",
    "纯收入",
    "收支",
    "住户",
    "农民",
    "人民生活",
    "人均",
    "收入",
]

DOWNLOAD_DIR = OUTPUT_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
CHROME_PROFILE_CLONE_DIR = OUTPUT_DIR / "chrome_profile_clone"
EXCEL_APP_PATH = Path("/Applications/Microsoft Excel.app")

CHROME_PROFILE_ROOT_FILES = (
    "Cookies",
    "Preferences",
    "Secure Preferences",
)
CHROME_SQLITE_FILES = {
    "Cookies",
}
CHROME_PROFILE_ROOT_DIRS = (
)

ROW_VALUE_SPLIT_RE = re.compile(r"\s+([-−－]?\d+(?:[．\.]\d+)?)")
NUMERIC_TOKEN_RE = re.compile(r"[-−－]?\d+(?:[．\.]\d+)?")

CITY_NAME_CORRECTIONS = {
    "秦岛市": "秦皇岛市",
    "折州市": "忻州市",
    "抚顾市": "抚顺市",
    "掄林市": "榆林市",
    "新.维吾尔自治区": "新疆维吾尔自治区",
}

PROVINCE_TO_REGION = {
    "北京市": "华北",
    "天津市": "华北",
    "河北省": "华北",
    "山西省": "华北",
    "内蒙古自治区": "华北",
    "辽宁省": "东北",
    "吉林省": "东北",
    "黑龙江省": "东北",
    "上海市": "华东",
    "江苏省": "华东",
    "浙江省": "华东",
    "安徽省": "华东",
    "福建省": "华东",
    "江西省": "华东",
    "山东省": "华东",
    "河南省": "中南",
    "湖北省": "中南",
    "湖南省": "中南",
    "广东省": "中南",
    "广西壮族自治区": "中南",
    "海南省": "中南",
    "重庆市": "西南",
    "四川省": "西南",
    "贵州省": "西南",
    "云南省": "西南",
    "西藏自治区": "西南",
    "陕西省": "西北",
    "甘肃省": "西北",
    "青海省": "西北",
    "宁夏回族自治区": "西北",
    "新疆维吾尔自治区": "西北",
    "香港": "其他",
    "澳门": "其他",
    "台湾省": "其他",
}

INCOME_ALIASES: dict[str, list[str]] = {
    "农村人均收入": [
        "农村居民家庭人均纯收入",
        "农村居民人均纯收入",
        "农村人均纯收入",
        "人均纯收入",
        "全年纯收入",
        "农村人均总收入",
        "人均总收入",
        "农村居民人均收入",
        "农村居民收入",
    ],
    "工资性收入": [
        "工资性收入",
        "劳动报酬收入",
        "劳动者报酬收入",
        "报酬性收入",
        "工资收入",
        "工薪收入",
        "劳动报酬",
        "劳动者的报酬收入",
    ],
    "经营性收入": [
        "家庭经营性收入",
        "经营性收入",
        "家庭经营收入",
        "经营收入",
        "家庭经营纯收入",
        "经营纯收入",
    ],
    "财产收入": [
        "财产收入",
        "财产性收入",
    ],
    "转移收入": [
        "转移收入",
        "转移性收入",
        "转移净收入",
    ],
}

TOC_INCOME_KEYWORDS = [
    "农村居民家庭基本情况",
    "农村居民人均纯收入",
    "农村居民收入",
    "农村居民家庭",
    "人均纯收入",
    "农村居民",
    "人民生活",
    "居民收入",
]

SKIP_NAMES = {"城市", "城 市", "城市合计", "nan", ""}
PROVINCE_LEVEL_NAMES = set(PROVINCE_TO_REGION)
YEARBOOK_REQUIRED_KEYWORDS = ("统计年鉴",)
YEARBOOK_SPECIAL_TOPIC_KEYWORDS = (
    "人口普查",
    "税务",
    "工业",
    "房地产",
    "科技",
    "教育",
    "卫生",
    "财政",
    "金融",
    "交通",
    "旅游",
    "文化",
    "体育",
    "环境",
    "水利",
    "气象",
    "法院",
    "检察",
    "审计",
)
ENTRY_SUFFIX_RE = re.compile(r"[（(][一二三四五六七八九十]+[)）]\s*$")
DOWNLOAD_FILE_NAME_RE = re.compile(r"([^\s；;]+\.xls[x]?)", re.IGNORECASE)


@dataclass(frozen=True)
class CityContext:
    original_name: str
    normalized_name: str
    province_name: str | None
    region_group: str
    is_province_level: bool
    query_variants: tuple[str, ...]


# ══════════════════════════════════════════════════════════════════
# 日志
# ══════════════════════════════════════════════════════════════════

OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# 通用工具
# ══════════════════════════════════════════════════════════════════

def load_cities() -> list[str]:
    df = pd.read_excel(CITY_LIST_FILE, header=None)
    cities = []
    for val in df.iloc[:, 0]:
        name = str(val).strip()
        if name not in SKIP_NAMES:
            cities.append(name)
    log.info(f"共读取 {len(cities)} 个城市/省份条目")
    return cities


def canonicalize_name(name: str) -> str:
    cleaned = name.strip().replace(" ", "")
    cleaned = CITY_NAME_CORRECTIONS.get(cleaned, cleaned)
    return cleaned.replace(".", "")


def append_note(note: str | None, text: str) -> str:
    text = text.strip()
    if not text:
        return note or ""
    if not note:
        return text
    if text in note:
        return note
    return f"{note}；{text}"


def strip_admin_suffix(name: str) -> str:
    for suffix in ("维吾尔自治区", "回族自治区", "壮族自治区", "自治区", "自治州", "地区", "盟", "省", "市", "县", "区"):
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def name_tokens(name: str) -> tuple[str, ...]:
    tokens = []
    canonical = canonicalize_name(name)
    stripped = strip_admin_suffix(canonical)
    for value in (canonical, stripped):
        if value and value not in tokens:
            tokens.append(value)
    return tuple(tokens)


def build_query_variants(name: str) -> tuple[str, ...]:
    variants = []
    for token in name_tokens(name):
        for value in (f"{token}统计年鉴", token):
            if value not in variants:
                variants.append(value)
    return tuple(variants)


def build_city_contexts(cities: list[str]) -> dict[str, CityContext]:
    contexts: dict[str, CityContext] = {}
    current_province: str | None = None
    current_region = "其他"

    for raw_name in cities:
        normalized_name = canonicalize_name(raw_name)
        is_province_level = normalized_name in PROVINCE_LEVEL_NAMES
        if is_province_level:
            current_province = normalized_name
            current_region = PROVINCE_TO_REGION.get(normalized_name, "其他")

        province_name = normalized_name if is_province_level else current_province
        region_group = PROVINCE_TO_REGION.get(province_name or "", current_region)

        contexts[raw_name] = CityContext(
            original_name=raw_name,
            normalized_name=normalized_name,
            province_name=province_name,
            region_group=region_group,
            is_province_level=is_province_level,
            query_variants=build_query_variants(normalized_name),
        )

    return contexts


def resolve_city_context(city: str, contexts: dict[str, CityContext]) -> CityContext:
    if city in contexts:
        return contexts[city]

    normalized = canonicalize_name(city)
    for ctx in contexts.values():
        if normalized == ctx.normalized_name:
            return CityContext(
                original_name=city,
                normalized_name=ctx.normalized_name,
                province_name=ctx.province_name,
                region_group=ctx.region_group,
                is_province_level=ctx.is_province_level,
                query_variants=ctx.query_variants,
            )
        if normalized in name_tokens(ctx.normalized_name):
            return CityContext(
                original_name=city,
                normalized_name=ctx.normalized_name,
                province_name=ctx.province_name,
                region_group=ctx.region_group,
                is_province_level=ctx.is_province_level,
                query_variants=ctx.query_variants,
            )

    province_name = normalized if normalized in PROVINCE_LEVEL_NAMES else None
    region_group = PROVINCE_TO_REGION.get(province_name or "", "其他")
    return CityContext(
        original_name=city,
        normalized_name=normalized,
        province_name=province_name,
        region_group=region_group,
        is_province_level=province_name is not None,
        query_variants=build_query_variants(normalized),
    )


def env_truthy(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def manual_login_enabled(driver: webdriver.Chrome) -> bool:
    if not env_truthy("CNKI_ENABLE_MANUAL_LOGIN", default=False):
        return False
    return not bool(getattr(driver, "_cnki_headless", False))


def prompt_for_manual_login() -> bool:
    print(
        "\n检测到 CNKI 下载被重定向到登录页。"
        "请在打开的浏览器窗口中完成登录/机构认证，完成后回到终端按回车继续。"
        "如果要放弃当前下载，请输入 skip 后回车。\n",
        flush=True,
    )
    answer = input("继续等待手动登录: ").strip().lower()
    return answer not in {"skip", "s", "n", "no"}


def detect_local_chrome_user_data_dir() -> Path | None:
    explicit = os.getenv("CNKI_CHROME_USER_DATA_DIR")
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())

    home = Path.home()
    candidates.extend(
        [
            home / "Library/Application Support/Google/Chrome",
            home / ".config/google-chrome",
            home / ".config/chromium",
        ]
    )

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.expanduser()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved
    return None


def copy_profile_artifact(src: Path, dst: Path) -> None:
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
    elif src.is_file():
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.name in CHROME_SQLITE_FILES:
            try:
                if dst.exists():
                    dst.unlink()
                with sqlite3.connect(f"file:{src}?mode=ro", uri=True) as source_db:
                    with sqlite3.connect(dst) as target_db:
                        source_db.backup(target_db)
            except sqlite3.Error:
                shutil.copy2(src, dst)
        else:
            shutil.copy2(src, dst)
        for suffix in ("-journal", "-wal", "-shm"):
            sidecar = src.with_name(f"{src.name}{suffix}")
            if sidecar.exists():
                shutil.copy2(sidecar, dst.with_name(f"{dst.name}{suffix}"))


def profile_clone_needs_refresh(
    source_profile_dir: Path,
    clone_profile_dir: Path,
) -> bool:
    if not clone_profile_dir.exists():
        return True

    source_cookies = source_profile_dir / "Cookies"
    clone_cookies = clone_profile_dir / "Cookies"
    if source_cookies.exists() and not clone_cookies.exists():
        return True
    if source_cookies.exists() and clone_cookies.exists():
        return source_cookies.stat().st_mtime > clone_cookies.stat().st_mtime
    return False


def prepare_chrome_profile() -> tuple[Path | None, str | None]:
    if not env_truthy("CNKI_USE_LOCAL_CHROME_PROFILE", default=True):
        return None, None

    mode = os.getenv("CNKI_CHROME_PROFILE_MODE", "clone").strip().lower()
    profile_name = os.getenv("CNKI_CHROME_PROFILE_DIRECTORY", "Default").strip() or "Default"
    user_data_dir = detect_local_chrome_user_data_dir()
    if user_data_dir is None:
        log.info("未找到本机 Chrome 用户数据目录，将使用临时匿名浏览器会话")
        return None, None

    source_profile_dir = user_data_dir / profile_name
    if not source_profile_dir.exists():
        log.warning(f"Chrome 配置文件目录不存在: {source_profile_dir}")
        return None, None

    if mode == "off":
        return None, None

    if mode == "direct":
        log.info(f"复用本机 Chrome 配置文件: {source_profile_dir}")
        return user_data_dir, profile_name

    clone_root = Path(
        os.getenv("CNKI_CHROME_PROFILE_CLONE_DIR", str(CHROME_PROFILE_CLONE_DIR))
    ).expanduser()
    refresh = env_truthy("CNKI_REFRESH_CHROME_PROFILE", default=False)
    clone_user_data_dir = clone_root / "user-data"
    clone_profile_dir = clone_user_data_dir / profile_name

    if not refresh:
        refresh = profile_clone_needs_refresh(source_profile_dir, clone_profile_dir)

    if refresh and clone_user_data_dir.exists():
        shutil.rmtree(clone_user_data_dir, ignore_errors=True)

    if not clone_profile_dir.exists():
        clone_profile_dir.mkdir(parents=True, exist_ok=True)
        local_state = user_data_dir / "Local State"
        if local_state.exists():
            copy_profile_artifact(local_state, clone_user_data_dir / "Local State")

        for name in CHROME_PROFILE_ROOT_FILES:
            artifact = source_profile_dir / name
            if artifact.exists():
                copy_profile_artifact(artifact, clone_profile_dir / name)

        for name in CHROME_PROFILE_ROOT_DIRS:
            artifact = source_profile_dir / name
            if artifact.exists():
                copy_profile_artifact(artifact, clone_profile_dir / name)

        for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
            lock_path = clone_user_data_dir / lock_name
            if lock_path.exists() or lock_path.is_symlink():
                try:
                    lock_path.unlink()
                except OSError:
                    pass

        first_run = clone_user_data_dir / "First Run"
        if first_run.exists():
            try:
                first_run.unlink()
            except OSError:
                pass

    log.info(f"使用克隆的 Chrome 配置文件: {clone_profile_dir}")
    return clone_user_data_dir, profile_name


def create_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )
    profile_user_data_dir, profile_name = prepare_chrome_profile()
    if profile_user_data_dir is not None:
        opts.add_argument(f"--user-data-dir={profile_user_data_dir}")
        opts.add_argument(f"--profile-directory={profile_name}")
    opts.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(DOWNLOAD_DIR.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    driver = webdriver.Chrome(options=opts)
    setattr(driver, "_cnki_headless", headless)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(DOWNLOAD_DIR.resolve()),
            },
        )
    except Exception as exc:
        log.warning(f"配置 Chrome 下载目录失败: {exc}")
    return driver


def wait_click(wait: WebDriverWait, locator: tuple, msg: str = "") -> None:
    elem = wait.until(EC.element_to_be_clickable(locator), msg)
    elem.click()


def switch_to_newest_window(driver: webdriver.Chrome, old_handles: set[str]) -> bool:
    for _ in range(30):
        new = set(driver.window_handles) - old_handles
        if new:
            driver.switch_to.window(new.pop())
            return True
        time.sleep(0.5)
    return False


def take_screenshot(driver: webdriver.Chrome, name: str) -> None:
    path = OUTPUT_DIR / f"debug_{name}.png"
    try:
        driver.save_screenshot(str(path))
    except Exception:
        pass


def match_field(text: str) -> str | None:
    text = text.strip()
    if not text:
        return None

    normalized_text = (
        text.replace(" ", "")
        .replace("\u3000", "")
        .replace("（", "(")
        .replace("）", ")")
        .replace("收人", "收入")
        .replace("报酬性", "报酬")
    )
    normalized_text = re.sub(r"^[一二三四五六七八九十]+[、.．\s]*", "", normalized_text)
    normalized_text = re.sub(r"^[（(][一二三四五六七八九十]+[)）]", "", normalized_text)

    for canonical, aliases in INCOME_ALIASES.items():
        for alias in aliases:
            normalized_alias = (
                alias.replace(" ", "")
                .replace("\u3000", "")
                .replace("（", "(")
                .replace("）", ")")
                .replace("收人", "收入")
                .replace("报酬性", "报酬")
            )
            if (
                alias in text
                or text in alias
                or normalized_alias in normalized_text
                or normalized_text in normalized_alias
            ):
                return canonical
    return None


def normalize_label_for_exact_match(text: str) -> str:
    normalized = (
        str(text or "")
        .strip()
        .replace(" ", "")
        .replace("\u3000", "")
        .replace("（", "(")
        .replace("）", ")")
        .replace("收人", "收入")
        .replace("报酬性", "报酬")
    )
    normalized = re.sub(r"[A-Za-z][A-Za-z\s\-\.,:;()]*$", "", normalized)
    normalized = re.sub(r"^\d+[、.．]\s*", "", normalized)
    normalized = re.sub(r"^[一二三四五六七八九十]+[、.．\s]*", "", normalized)
    normalized = re.sub(r"^[（(][一二三四五六七八九十\d]+[)）]", "", normalized)
    return normalized.strip()


def match_field_exactish(text: str) -> str | None:
    normalized_text = normalize_label_for_exact_match(text)
    if not normalized_text:
        return None
    for canonical, aliases in INCOME_ALIASES.items():
        for alias in aliases:
            if normalized_text == normalize_label_for_exact_match(alias):
                return canonical
    return None


def best_numeric(cells: list) -> str | None:
    skip_tokens = {"-", "—", "－", "…", "...", "/", ""}
    for cell in reversed(cells):
        try:
            val = cell.text.strip()
        except StaleElementReferenceException:
            continue
        if val in skip_tokens:
            continue
        if any(ch.isdigit() for ch in val):
            return val
    return None


def visible_clickable(driver: webdriver.Chrome, xpath: str):
    for elem in driver.find_elements(By.XPATH, xpath):
        try:
            if elem.is_displayed() and elem.is_enabled():
                return elem
        except StaleElementReferenceException:
            continue
    return None


def wait_visible_clickable(
    driver: webdriver.Chrome, xpath: str, timeout: int = WAIT_TIMEOUT
):
    end = time.time() + timeout
    while time.time() < end:
        elem = visible_clickable(driver, xpath)
        if elem is not None:
            return elem
        time.sleep(0.5)
    raise TimeoutException(f"找不到可见元素: {xpath}")


def click_js(driver: webdriver.Chrome, elem) -> None:
    try:
        driver.execute_script("arguments[0].click();", elem)
    except Exception:
        driver.execute_script(
            "arguments[0].dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true}));",
            elem,
        )


def extract_book_code_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    book_code = query.get("id", [None])[0]
    return book_code or None


def build_requests_session_from_driver(driver: webdriver.Chrome) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://data.cnki.net",
        }
    )
    for cookie in driver.get_cookies():
        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )
    return session


def api_post(
    session: requests.Session,
    endpoint: str,
    payload,
    referer: str,
):
    url = f"{CNKI_API_BASE}{endpoint}"
    headers = {
        "Referer": referer,
        "Content-Type": "application/json",
    }
    if isinstance(payload, str):
        data = payload
    else:
        data = json.dumps(payload, ensure_ascii=False)
    response = session.post(url, data=data.encode("utf-8"), headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def search_entries_in_book(
    session: requests.Session,
    book_code: str,
    keyword: str,
    referer: str,
    page_size: int = 100,
) -> list[dict]:
    payload = {
        "serchModel": 0,
        "searchNameOne": keyword,
        "searchKeyOne": 0,
        "operator": 0,
        "searchNameTwo": "",
        "searchKeyTwo": 1,
        "code": book_code,
        "currentPage": 1,
        "pageSize": page_size,
    }
    response = api_post(
        session,
        "/StatisticalData/GetSearchThisBook",
        payload,
        referer=referer,
    )
    data = response.get("data", {}).get("data", {})
    return data.get("list", []) or []


def preview_entry(
    session: requests.Session,
    file_code: str,
    referer: str,
) -> dict | None:
    response = api_post(
        session,
        "/StatisticalData/GetEntryPreview",
        file_code,
        referer=referer,
    )
    data = response.get("data", {}).get("data", [])
    if data:
        return data[0]
    return None


def request_download_url(
    session: requests.Session,
    file_code: str,
    referer: str,
    flag: str = "2",
) -> str | None:
    response = api_post(
        session,
        "/StatisticalData/PdfAndCajDownload",
        {"fileName": file_code, "downLoadFlag": flag},
        referer=referer,
    )
    if response.get("isSuccess"):
        return response.get("data")
    return None


def fetch_modal_detail(
    session: requests.Session,
    file_code: str,
    referer: str,
) -> dict | list | None:
    try:
        response = api_post(
            session,
            "/StatisticalTable/Get_ModalDetail",
            file_code,
            referer=referer,
        )
    except requests.RequestException:
        return None
    return response.get("data")


def list_completed_downloads() -> set[str]:
    return {
        path.name
        for path in DOWNLOAD_DIR.iterdir()
        if path.is_file()
        and not path.name.endswith(".crdownload")
        and not path.name.startswith("~$")
        and not path.name.startswith(".")
    }


def find_existing_downloads(file_code: str) -> list[Path]:
    if not file_code:
        return []
    candidates = []
    for path in DOWNLOAD_DIR.iterdir():
        if not path.is_file():
            continue
        if path.name.startswith("~$") or path.name.startswith("."):
            continue
        if file_code in path.name and path.suffix.lower() in {".xls", ".xlsx"}:
            candidates.append(path)
    return sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)


def wait_for_download(before: set[str], timeout: int = 25) -> Path | None:
    end = time.time() + timeout
    while time.time() < end:
        current = list_completed_downloads()
        new_files = sorted(current - before)
        if new_files:
            return DOWNLOAD_DIR / new_files[-1]
        time.sleep(0.5)
    return None


def search_current_book_entries(driver: webdriver.Chrome, keyword: str) -> bool:
    try:
        search_box = wait_visible_clickable(
            driver,
            "//input[contains(@class,'single_input-cont')]",
            timeout=15,
        )
        search_box.clear()
        search_box.send_keys(keyword)
        spans = driver.find_elements(
            By.XPATH,
            (
                "//span[contains(@class,'single_retrieve')"
                " and (contains(normalize-space(.), '本册检索')"
                " or contains(normalize-space(.), '本种检索'))]"
            ),
        )
        for button in spans:
            try:
                if button.is_displayed() and button.is_enabled():
                    click_js(driver, button)
                    time.sleep(PAGE_PAUSE)
                    return True
            except StaleElementReferenceException:
                continue
        buttons = driver.find_elements(
            By.XPATH, "//button[contains(@class,'single_retrieve')]"
        )
        for button in buttons:
            try:
                if button.is_displayed() and button.is_enabled():
                    click_js(driver, button)
                    time.sleep(PAGE_PAUSE)
                    return True
            except StaleElementReferenceException:
                continue
        search_box.send_keys(Keys.RETURN)
        time.sleep(PAGE_PAUSE)
        return True
    except TimeoutException:
        return False


def click_entry_row(driver: webdriver.Chrome, entry_title: str) -> bool:
    row = find_entry_row(driver, entry_title)
    if row is None:
        return False
    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});",
            row,
        )
        time.sleep(0.5)
        try:
            row.click()
        except Exception:
            click_js(driver, row)
        time.sleep(PAGE_PAUSE)
        return True
    except StaleElementReferenceException:
        return False


def find_entry_row(driver: webdriver.Chrome, entry_title: str):
    title_variants = [entry_title]
    if "(" in entry_title:
        title_variants.append(entry_title.split("(")[0].strip())
    if "（" in entry_title:
        title_variants.append(entry_title.split("（")[0].strip())

    for variant in title_variants:
        xpath = (
            "//tr[contains(@class,'single_s-tab-tbody')]"
            f"[.//*[contains(normalize-space(.), '{variant}')]]"
        )
        rows = driver.find_elements(By.XPATH, xpath)
        for row in rows:
            try:
                if not row.is_displayed():
                    continue
                return row
            except StaleElementReferenceException:
                continue
    return None


def wait_for_download_or_new_window(
    driver: webdriver.Chrome,
    before_files: set[str],
    before_handles: set[str],
    origin_handle: str,
    timeout: int = 25,
) -> tuple[Path | None, str | None, bool]:
    end = time.time() + timeout
    while time.time() < end:
        downloaded = wait_for_download(before_files, timeout=1)
        if downloaded is not None:
            return downloaded, None, False

        new_handles = [h for h in driver.window_handles if h not in before_handles]
        if new_handles:
            notes = []
            saw_login_page = False
            for handle in new_handles:
                driver.switch_to.window(handle)
                time.sleep(PAGE_PAUSE)
                current_url = driver.current_url
                page_title = driver.title
                if "login.cnki.net" in current_url:
                    saw_login_page = True
                    notes.append("页面 Excel 图标触发后被重定向到 CNKI 登录页")
                elif "ErrorMsg.html" in current_url:
                    notes.append(f"页面 Excel 图标返回错误页: {page_title or current_url}")
                elif current_url and current_url != "about:blank":
                    notes.append(f"页面 Excel 图标打开新页: {current_url}")

            if saw_login_page and manual_login_enabled(driver):
                if prompt_for_manual_login():
                    download_after_login = wait_for_download(before_files, timeout=5)
                    for handle in list(driver.window_handles):
                        if handle != origin_handle:
                            driver.switch_to.window(handle)
                            if "login.cnki.net" in driver.current_url:
                                try:
                                    driver.close()
                                except Exception:
                                    pass
                    driver.switch_to.window(origin_handle)
                    if download_after_login is not None:
                        notes.append("手动登录后下载成功")
                        return download_after_login, "；".join(dict.fromkeys(notes)), False
                    notes.append("已完成手动登录，准备重试当前下载")
                    return None, "；".join(dict.fromkeys(notes)), True
                notes.append("用户跳过手动登录")

            for handle in list(driver.window_handles):
                if handle == origin_handle:
                    continue
                driver.switch_to.window(handle)
                try:
                    driver.close()
                except Exception:
                    pass
            driver.switch_to.window(origin_handle)
            return None, "；".join(dict.fromkeys(notes)) if notes else None, False

        time.sleep(0.5)

    return None, None, False


def attempt_row_excel_download(
    driver: webdriver.Chrome,
    keyword: str,
    entry_title: str,
) -> tuple[Path | None, str | None]:
    if not search_current_book_entries(driver, keyword):
        return None, None

    row = find_entry_row(driver, entry_title)
    if row is None:
        return None, None

    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});",
            row,
        )
        time.sleep(0.5)
        excel_icon = row.find_element(
            By.XPATH,
            ".//*[name()='svg' and contains(@class,'single_excel')]",
        )
        attempts = 2 if manual_login_enabled(driver) else 1
        notes: list[str] = []
        for _ in range(attempts):
            before_files = list_completed_downloads()
            before_handles = set(driver.window_handles)
            origin_handle = driver.current_window_handle
            click_js(driver, excel_icon)
            downloaded, note, should_retry = wait_for_download_or_new_window(
                driver,
                before_files=before_files,
                before_handles=before_handles,
                origin_handle=origin_handle,
                timeout=20,
            )
            if note:
                notes.append(note)
            if downloaded is not None:
                return downloaded, "；".join(dict.fromkeys(notes)) if notes else None
            if not should_retry:
                break
        return None, "；".join(dict.fromkeys(notes)) if notes else None
    except NoSuchElementException:
        return None, None
    except StaleElementReferenceException:
        return None, None


def attempt_modal_excel_download(
    driver: webdriver.Chrome,
    keyword: str,
    entry_title: str,
) -> Path | None:
    if not search_current_book_entries(driver, keyword):
        return None
    if not click_entry_row(driver, entry_title):
        return None

    try:
        modal = driver.find_element(
            By.XPATH,
            "//div[contains(@class,'modal_single_modal') and contains(@class,'in')]",
        )
        excel = modal.find_element(
            By.XPATH,
            ".//span[contains(., 'Excel格式文件下载')]",
        )
        if not excel.is_displayed():
            return None
        before = list_completed_downloads()
        click_js(driver, excel)
        downloaded = wait_for_download(before, timeout=25)
        close_icons = modal.find_elements(
            By.XPATH,
            ".//*[contains(@class,'icon-close')]",
        )
        for icon in close_icons:
            try:
                if icon.is_displayed():
                    click_js(driver, icon)
                    break
            except StaleElementReferenceException:
                continue
        return downloaded
    except NoSuchElementException:
        return None


def attempt_browser_download(
    driver: webdriver.Chrome,
    download_url: str,
) -> tuple[Path | None, str | None]:
    before = list_completed_downloads()
    current_handle = driver.current_window_handle
    old_handles = set(driver.window_handles)
    try:
        driver.execute_script("window.open(arguments[0], '_blank');", download_url)
        if switch_to_newest_window(driver, old_handles):
            time.sleep(PAGE_PAUSE)
            downloaded = wait_for_download(before)
            failure_context = None
            if downloaded is None:
                current_url = driver.current_url
                page_title = driver.title
                if "login.cnki.net" in current_url:
                    failure_context = "下载被重定向到 CNKI 登录页"
                elif "ErrorMsg.html" in current_url:
                    failure_context = f"下载服务返回错误页: {page_title or current_url}"
                elif current_url and current_url != "about:blank":
                    failure_context = f"下载未落地，当前页: {current_url}"
            driver.close()
            driver.switch_to.window(current_handle)
            return downloaded, failure_context
    except Exception as exc:
        log.warning(f"浏览器下载尝试失败: {exc}")
        try:
            driver.switch_to.window(current_handle)
        except Exception:
            pass
        return None, f"浏览器下载尝试失败: {exc}"

    fallback = wait_for_download(before, timeout=3)
    return fallback, None


# ══════════════════════════════════════════════════════════════════
# 导航
# ══════════════════════════════════════════════════════════════════

def open_cnki_platform(driver: webdriver.Chrome, wait: WebDriverWait) -> bool:
    log.info(f"▶ 打开数据库导航页: {FUDAN_DB_NAV_URL}")
    driver.get(FUDAN_DB_NAV_URL)
    time.sleep(PAGE_PAUSE * 2)

    try:
        search_box = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//input[contains(@placeholder,'数据库')"
                    " or contains(@placeholder,'搜索')"
                    " or contains(@placeholder,'出版社')]",
                )
            )
        )
        search_box.clear()
        search_box.send_keys("中国经济社会大数据")
        search_box.send_keys(Keys.RETURN)
        time.sleep(SEARCH_PAUSE)

        old_handles = set(driver.window_handles)
        target = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[contains(text(),'经济社会大数据')]")
            )
        )
        click_js(driver, target)

        if switch_to_newest_window(driver, old_handles):
            log.info(f"  切换到新窗口: {driver.current_url}")
        time.sleep(PAGE_PAUSE * 2)

        if "data.cnki.net" not in driver.current_url:
            log.warning(f"到达的 URL 不符合预期: {driver.current_url}")

        log.info(f"▶ 已到达 CNKI 平台: {driver.current_url}")
        return True

    except TimeoutException as exc:
        log.error(f"导航到 CNKI 平台失败: {exc}")
        take_screenshot(driver, "open_cnki_fail")
        return False


def open_yearbook_search_page(driver: webdriver.Chrome, wait: WebDriverWait) -> bool:
    try:
        stat = wait_visible_clickable(driver, "//a[normalize-space()='统计资料']")
        click_js(driver, stat)
        time.sleep(PAGE_PAUSE)

        yearbook = wait_visible_clickable(driver, "//a[normalize-space()='统计年鉴']")
        click_js(driver, yearbook)
        time.sleep(SEARCH_PAUSE)

        wait_visible_clickable(
            driver,
            f"//input[contains(@placeholder,'{YEARBOOK_SEARCH_PLACEHOLDER}')]",
        )
        log.info(f"▶ 已进入统计年鉴检索页: {driver.current_url}")
        return True

    except TimeoutException as exc:
        log.error(f"进入统计年鉴检索页失败: {exc}")
        take_screenshot(driver, "open_yearbook_fail")
        return False


# ══════════════════════════════════════════════════════════════════
# yearbook_locator
# ══════════════════════════════════════════════════════════════════

def ensure_yearbook_search_page(driver: webdriver.Chrome) -> bool:
    driver.get(CNKI_YEARBOOK_URL)
    time.sleep(PAGE_PAUSE * 2)
    try:
        wait_visible_clickable(
            driver,
            f"//input[contains(@placeholder,'{YEARBOOK_SEARCH_PLACEHOLDER}')]",
            timeout=15,
        )
        return True
    except TimeoutException:
        take_screenshot(driver, "yearbook_search_page_fail")
        return False


def search_yearbook(driver: webdriver.Chrome, keyword: str) -> bool:
    try:
        search_box = wait_visible_clickable(
            driver,
            f"//input[contains(@placeholder,'{YEARBOOK_SEARCH_PLACEHOLDER}')]",
        )
        search_box.clear()
        search_box.send_keys(keyword)

        icon = visible_clickable(
            driver,
            "//*[name()='svg'][contains(@class,'yearBook_icons-search')]",
        )
        if icon is not None:
            click_js(driver, icon)
        else:
            search_box.send_keys(Keys.RETURN)

        time.sleep(SEARCH_PAUSE)
        return True

    except (TimeoutException, ElementNotInteractableException) as exc:
        log.warning(f"  关键词“{keyword}”年鉴检索失败: {exc}")
        take_screenshot(driver, f"yearbook_search_fail_{keyword}")
        return False


def expand_region_panel(driver: webdriver.Chrome) -> bool:
    try:
        header = wait_visible_clickable(
            driver,
            "//div[contains(@class,'yearBook_panel-header')][.//span[normalize-space()='地区']]",
            timeout=15,
        )
        body = header.find_element(By.XPATH, "following-sibling::div[1]")
        if "hide" in (body.get_attribute("class") or ""):
            click_js(driver, header)
            time.sleep(PAGE_PAUSE)
        return True
    except (NoSuchElementException, TimeoutException):
        return False


def apply_region_group_filter(driver: webdriver.Chrome, region_group: str) -> bool:
    try:
        item = visible_clickable(
            driver,
            f"//strong[normalize-space()='地区分组：']/following-sibling::i[contains(normalize-space(.), '{region_group}')]",
        )
        if item is None:
            return False
        click_js(driver, item)
        time.sleep(PAGE_PAUSE)
        return True
    except Exception:
        return False


def apply_province_filter(driver: webdriver.Chrome, province_name: str | None) -> bool:
    if not province_name:
        return False
    if not expand_region_panel(driver):
        return False

    xpath = (
        "//div[contains(@class,'yearBook_panel-region')]"
        f"//li[contains(normalize-space(.), '{province_name}')]"
    )
    try:
        province_item = wait_visible_clickable(driver, xpath, timeout=15)
        click_js(driver, province_item)
        time.sleep(SEARCH_PAUSE)
        return True
    except TimeoutException:
        return False


def collect_yearbook_cards(driver: webdriver.Chrome) -> list[dict]:
    cards: list[dict] = []
    containers = driver.find_elements(
        By.XPATH, "//div[contains(@class,'yearBook_list-content')]"
    )
    for container in containers:
        try:
            if not container.is_displayed():
                continue
            title_elem = container.find_element(By.XPATH, ".//h3/a")
            cards.append(
                {
                    "title_elem": title_elem,
                    "title": title_elem.text.strip(),
                    "href": title_elem.get_attribute("href") or "",
                    "text": container.text.strip(),
                }
            )
        except (NoSuchElementException, StaleElementReferenceException):
            continue
    return cards


def choose_yearbook_card(
    cards: list[dict], city_ctx: CityContext
) -> tuple[dict | None, str | None]:
    city_best = None
    city_best_score = -1
    provincial_best = None
    provincial_best_score = -1

    city_tokens = name_tokens(city_ctx.normalized_name)
    province_tokens = name_tokens(city_ctx.province_name or "")

    for card in cards:
        title = card["title"]
        text = card["text"]
        full_text = canonicalize_name(f"{title} {text}")
        canonical_title = canonicalize_name(title)

        if not any(keyword in title for keyword in YEARBOOK_REQUIRED_KEYWORDS):
            continue

        score = 0

        if "中国统计年鉴" in title:
            score -= 100
        if "统计年鉴" in title:
            score += 40
        if "农村统计年鉴" in title:
            score += 20
        if "农村" in title:
            score += 15
        if any(keyword in title for keyword in YEARBOOK_SPECIAL_TOPIC_KEYWORDS):
            score -= 60

        city_hit = any(token and token in full_text for token in city_tokens)
        province_hit = any(token and token in full_text for token in province_tokens)

        if city_hit:
            if any(token in canonical_title for token in city_tokens):
                score += 100
            else:
                score += 40
            if score > city_best_score:
                city_best = card
                city_best_score = score

        if province_hit:
            provincial_score = score
            if any(token in canonical_title for token in province_tokens):
                provincial_score += 40
            if provincial_score > provincial_best_score:
                provincial_best = card
                provincial_best_score = provincial_score

    if city_best is not None:
        return city_best, "CITY"
    if provincial_best is not None:
        return provincial_best, "PROVINCIAL"
    return None, None


def open_city_yearbook(
    driver: webdriver.Chrome, city_ctx: CityContext
) -> tuple[str | None, str | None, str | None]:
    if not ensure_yearbook_search_page(driver):
        return None, None, None

    apply_region_group_filter(driver, city_ctx.region_group)
    apply_province_filter(driver, city_ctx.province_name)

    initial_cards = collect_yearbook_cards(driver)
    log.info(
        f"  [{city_ctx.original_name}] 省级筛选后命中 {len(initial_cards)} 条候选"
    )
    if initial_cards:
        log.info(
            "  候选标题: " + " | ".join(card["title"] for card in initial_cards[:8])
        )
    card, source_scope = choose_yearbook_card(initial_cards, city_ctx)

    if card is None:
        for query in city_ctx.query_variants:
            if not ensure_yearbook_search_page(driver):
                break
            apply_region_group_filter(driver, city_ctx.region_group)
            apply_province_filter(driver, city_ctx.province_name)
            if not search_yearbook(driver, query):
                continue

            cards = collect_yearbook_cards(driver)
            log.info(
                f"  [{city_ctx.original_name}] 关键词“{query}”命中 {len(cards)} 条候选"
            )
            if cards:
                log.info(
                    "  候选标题: " + " | ".join(card["title"] for card in cards[:8])
                )
            card, source_scope = choose_yearbook_card(cards, city_ctx)
            if card is not None:
                break

    if card is None:
        return None, None, None

    title = card["title"]
    href = card["href"]
    log.info(f"  [{city_ctx.original_name}] 选择年鉴: {title} ({source_scope})")
    click_js(driver, card["title_elem"])
    time.sleep(SEARCH_PAUSE)
    return title, href, source_scope


def select_year(driver: webdriver.Chrome, wait: WebDriverWait, year: int) -> bool:
    year_str = str(year)
    xpaths = [
        f"//a[normalize-space(text())='{year_str}']",
        f"//span[normalize-space(text())='{year_str}']",
    ]
    for xpath in xpaths:
        try:
            elem = wait_visible_clickable(driver, xpath, timeout=8)
            click_js(driver, elem)
            time.sleep(PAGE_PAUSE * 2)
            log.info(f"  找到年份 {year_str}，点击...")
            return True
        except TimeoutException:
            continue

    log.warning(f"  找不到年份 {year_str}")
    take_screenshot(driver, f"no_year_{year}")
    return False


# ══════════════════════════════════════════════════════════════════
# year_selector / table_locator / parser
# ══════════════════════════════════════════════════════════════════


def score_income_entry(entry: dict) -> int:
    title = entry.get("title", "") or ""
    keywords = entry.get("tmgjc", "") or ""
    full_text = f"{title} {keywords}"
    score = 0

    if "农村住户人均总收入总支出和纯收入" in title:
        score += 220
    if "农村居民人均收支情况" in title:
        score += 180
    if "农村居民人均纯收入" in title:
        score += 160
    if "农村住户" in title:
        score += 45
    if "农村居民" in title:
        score += 45
    if "人均" in title:
        score += 35
    if "收入" in title:
        score += 35
    if "纯收入" in title:
        score += 25
    if "工资" in full_text or "报酬收入" in full_text:
        score += 20
    if "家庭经营收入" in full_text or "经营收入" in full_text:
        score += 20
    if "财产收入" in full_text:
        score += 18
    if "转移收入" in full_text or "转移性收入" in full_text:
        score += 18

    if "(一)" in title:
        score += 12
    if "(二)" in title:
        score += 8
    if "(三)" in title:
        score += 4
    if str(entry.get("czexcel")) == "1":
        score += 10

    for keyword in (
        "生活消费支出",
        "农产品生产和出售",
        "百户耐用消费品",
        "农业生产条件",
        "固定资产",
        "卫生组织",
        "排序",
        "各县",
        "县(市)",
        "县市",
        "各市县",
        "分县",
        "排名",
    ):
        if keyword in title:
            score -= 120

    return score


def choose_income_entry(entries: list[dict]) -> dict | None:
    if not entries:
        return None

    best_entry = None
    best_score = -10**9
    for entry in entries:
        score = score_income_entry(entry)
        if score > best_score:
            best_entry = entry
            best_score = score
    if best_score < 40:
        return None
    return best_entry


def entry_family_title(title: str) -> str:
    return ENTRY_SUFFIX_RE.sub("", (title or "").strip()).strip()


def find_related_income_entries(
    session: requests.Session,
    book_code: str,
    best_entry: dict,
    referer: str,
) -> list[dict]:
    family_title = entry_family_title(best_entry.get("title", ""))
    if not family_title:
        return [best_entry]

    try:
        entries = search_entries_in_book(
            session,
            book_code,
            family_title,
            referer=referer,
            page_size=100,
        )
    except requests.RequestException:
        return [best_entry]

    family: dict[str, dict] = {}
    best_file_code = best_entry.get("fileCode") or ""
    if best_file_code:
        family[best_file_code] = best_entry

    for entry in entries:
        file_code = entry.get("fileCode") or ""
        if not file_code:
            continue
        if entry_family_title(entry.get("title", "")) != family_title:
            continue
        family[file_code] = entry

    return sorted(family.values(), key=lambda item: item.get("title") or "")


def find_income_entry_via_api(
    session: requests.Session,
    book_code: str,
    referer: str,
) -> tuple[dict | None, list[str]]:
    by_file_code: dict[str, dict] = {}
    notes: list[str] = []

    for keyword in ENTRY_SEARCH_KEYWORDS:
        try:
            entries = search_entries_in_book(
                session,
                book_code,
                keyword,
                referer=referer,
            )
        except requests.RequestException as exc:
            notes.append(f"接口检索“{keyword}”失败: {exc}")
            continue

        if entries:
            notes.append(f"接口检索“{keyword}”命中 {len(entries)} 条")
        for entry in entries:
            file_code = entry.get("fileCode")
            if not file_code:
                continue
            cached = by_file_code.setdefault(file_code, dict(entry))
            hits = set(cached.get("_hit_keywords", []))
            hits.add(keyword)
            cached["_hit_keywords"] = sorted(hits)

    return choose_income_entry(list(by_file_code.values())), notes


def parse_excel_numeric(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if any(ch.isdigit() for ch in text):
        return text
    return None


def normalize_numeric_token(token: str) -> str:
    return (
        token.strip()
        .replace("．", ".")
        .replace("－", "-")
        .replace("−", "-")
        .replace("—", "-")
    )


def extract_label_and_numeric_tokens(text: str) -> tuple[str, list[str]]:
    raw = str(text or "").strip()
    if not raw:
        return "", []
    match = ROW_VALUE_SPLIT_RE.search(raw)
    if not match:
        return raw, []
    label = raw[: match.start()].strip()
    tail = raw[match.start() :].strip()
    numbers = []
    for chunk in re.split(r"(?:\t+|\s{2,})", tail):
        chunk = chunk.strip()
        if not chunk:
            continue
        if not any(ch.isdigit() for ch in chunk):
            continue
        numbers.append(normalize_numeric_token(chunk).replace(" ", ""))
    return label, numbers


def dump_encrypted_xls_rows_via_excel(path: Path) -> list[str]:
    if sys.platform == "darwin":
        return dump_encrypted_xls_rows_via_excel_macos(path)
    if sys.platform == "win32":
        return dump_encrypted_xls_rows_via_excel_windows(path)
    raise RuntimeError("当前环境不可用 Microsoft Excel 回退解析")


def dump_encrypted_xls_rows_via_excel_macos(path: Path) -> list[str]:
    if not EXCEL_APP_PATH.exists():
        raise RuntimeError("当前环境未安装 Microsoft Excel，无法回退解析")

    escaped_path = str(path.resolve()).replace("\\", "\\\\").replace('"', '\\"')
    script = f"""
on join_list(xs, delim)
  set oldTIDs to AppleScript's text item delimiters
  set AppleScript's text item delimiters to delim
  set outText to xs as text
  set AppleScript's text item delimiters to oldTIDs
  return outText
end join_list

tell application "Microsoft Excel"
  set oldVisible to visible
  set visible to false
  try
    open POSIX file "{escaped_path}"
    delay 2
    set wb to active workbook
    set ws to worksheet 1 of wb
    set ur to used range of ws
    set rCount to count of rows of ur
    set cCount to count of columns of ur
    set linesOut to {{}}
    repeat with r from 1 to rCount
      set rowVals to {{}}
      repeat with c from 1 to cCount
        set v to value of cell c of row r of ws
        if v is missing value then
          set end of rowVals to ""
        else
          set end of rowVals to (v as text)
        end if
      end repeat
      set end of linesOut to my join_list(rowVals, tab)
    end repeat
    close wb saving no
    set visible to oldVisible
    return my join_list(linesOut, linefeed)
  on error errMsg number errNum
    try
      close active workbook saving no
    end try
    set visible to oldVisible
    error errMsg number errNum
  end try
end tell
"""
    result = subprocess.run(
        ["osascript"],
        input=script,
        text=True,
        capture_output=True,
        check=True,
        timeout=90,
    )
    return result.stdout.splitlines()


def dump_encrypted_xls_rows_via_excel_windows(path: Path) -> list[str]:
    escaped_path = str(path.resolve()).replace("`", "``").replace('"', '`"')
    script = rf"""
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$path = "{escaped_path}"
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
$workbook = $null
$worksheet = $null
$usedRange = $null
try {{
  $workbook = $excel.Workbooks.Open($path, 0, $true)
  $worksheet = $workbook.Worksheets.Item(1)
  $usedRange = $worksheet.UsedRange
  $rowCount = $usedRange.Rows.Count
  $colCount = $usedRange.Columns.Count
  $lines = New-Object System.Collections.Generic.List[string]
  for ($r = 1; $r -le $rowCount; $r++) {{
    $rowVals = New-Object System.Collections.Generic.List[string]
    for ($c = 1; $c -le $colCount; $c++) {{
      $cell = $worksheet.Cells.Item($r, $c)
      $value = $cell.Text
      if ($null -eq $value) {{
        $rowVals.Add("")
      }} else {{
        $rowVals.Add([string]$value)
      }}
      [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($cell)
    }}
    $lines.Add(($rowVals -join "`t"))
  }}
  $lines -join "`n"
}} finally {{
  if ($workbook -ne $null) {{
    $workbook.Close($false)
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($workbook)
  }}
  if ($usedRange -ne $null) {{
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($usedRange)
  }}
  if ($worksheet -ne $null) {{
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($worksheet)
  }}
  if ($excel -ne $null) {{
    $excel.Quit()
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel)
  }}
  [GC]::Collect()
  [GC]::WaitForPendingFinalizers()
}}
"""
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True,
        capture_output=True,
        check=True,
        timeout=120,
    )
    return result.stdout.splitlines()


def extract_income_from_excel_rows(
    rows: list[str],
    result: dict,
    sheet_name: str,
) -> dict:
    for row_text in rows:
        label, numbers = extract_label_and_numeric_tokens(row_text)
        if not label or not numbers:
            continue
        if label.lstrip().startswith("#"):
            continue
        canonical = match_field_exactish(label)
        if canonical and result[canonical] is None:
            result[canonical] = numbers[0]
            log.info(
                f'    ✓ [{canonical}] ← "{label}" = {numbers[0]} (Excel:{sheet_name})'
            )
    return result


def extract_income_from_excel(path: Path, result: dict) -> dict:
    try:
        workbook = pd.read_excel(path, sheet_name=None, header=None)
    except Exception as exc:
        log.info(f"  pandas 直接读取 Excel 失败，尝试本机 Excel 回退解析: {exc}")
        rows = dump_encrypted_xls_rows_via_excel(path)
        return extract_income_from_excel_rows(rows, result, sheet_name="ExcelApp")

    for sheet_name, df in workbook.items():
        if df.empty:
            continue
        for row in df.fillna("").itertuples(index=False):
            cells = [str(cell).strip() for cell in row if str(cell).strip()]
            if len(cells) < 2:
                continue
            for idx, cell in enumerate(cells[:-1]):
                canonical = match_field_exactish(cell)
                if canonical and result[canonical] is None:
                    value = None
                    for candidate in reversed(cells[idx + 1 :]):
                        value = parse_excel_numeric(candidate)
                        if value is not None:
                            break
                    if value is not None:
                        result[canonical] = value
                        log.info(
                            f'    ✓ [{canonical}] ← "{cell}" = {value} (Excel:{sheet_name})'
                        )
    return result


def extract_income_from_modal_detail(modal_detail, result: dict) -> dict:
    if not modal_detail:
        return result

    candidates = []
    if isinstance(modal_detail, dict):
        candidates.append(modal_detail)
        nested = modal_detail.get("data")
        if isinstance(nested, list):
            candidates.extend(item for item in nested if isinstance(item, dict))
    elif isinstance(modal_detail, list):
        candidates.extend(item for item in modal_detail if isinstance(item, dict))

    for item in candidates:
        for raw_key, raw_value in item.items():
            canonical = match_field(str(raw_key))
            value = parse_excel_numeric(raw_value)
            if canonical and value and result[canonical] is None:
                result[canonical] = value
    return result


def entry_scope_matches_city(
    preview: dict | None,
    entry: dict,
    city_ctx: CityContext,
    source_scope: str | None,
) -> bool:
    if city_ctx.is_province_level or source_scope == "CITY":
        return True

    combined = " ".join(
        str(value or "")
        for value in (
            entry.get("title"),
            entry.get("parentNode"),
            preview.get("title") if preview else "",
            preview.get("wzlm") if preview else "",
            preview.get("shdy") if preview else "",
            preview.get("city") if preview else "",
            preview.get("county") if preview else "",
        )
    )
    combined = canonicalize_name(combined)
    return any(token and token in combined for token in name_tokens(city_ctx.normalized_name))


def split_scope_items(scope_text: str | None) -> list[str]:
    if not scope_text:
        return []
    items = []
    for raw in re.split(r"[;；]", str(scope_text)):
        cleaned = canonicalize_name(raw.strip())
        if cleaned and cleaned not in items:
            items.append(cleaned)
    return items


def should_skip_multi_region_parse(
    preview: dict | None,
    city_ctx: CityContext,
    source_scope: str | None,
) -> bool:
    if city_ctx.is_province_level or source_scope != "PROVINCIAL" or not preview:
        return False

    scope_items = split_scope_items(preview.get("shdy"))
    if len(scope_items) < 2:
        return False

    city_tokens = name_tokens(city_ctx.normalized_name)
    if not any(token and any(token in item for item in scope_items) for token in city_tokens):
        return False

    return True

def navigate_to_income_section(driver: webdriver.Chrome) -> None:
    for kw in TOC_INCOME_KEYWORDS:
        try:
            xpath = f"//*[contains(text(),'{kw}')][not(self::script)][not(self::style)]"
            candidates = driver.find_elements(By.XPATH, xpath)
            clickable = [
                elem
                for elem in candidates
                if elem.is_displayed() and len(elem.text.strip()) <= 30
            ]
            if clickable:
                log.info(f"  目录中找到收入相关节点: {clickable[0].text.strip()}")
                click_js(driver, clickable[0])
                time.sleep(PAGE_PAUSE)
                return
        except Exception:
            continue
    log.info("  未找到收入目录节点，继续扫描当前页表格")


def search_income_keywords(driver: webdriver.Chrome) -> bool:
    search_box = visible_clickable(
        driver,
        "//input["
        "contains(@placeholder,'搜索')"
        " or contains(@placeholder,'检索')"
        " or contains(@placeholder,'请输入')"
        " or contains(@class,'single_input-cont')"
        "]",
    )
    if search_box is None:
        return False

    for keyword in TABLE_KEYWORDS:
        try:
            search_box.clear()
            search_box.send_keys(keyword)
            retrieve_button = visible_clickable(
                driver,
                "//button[contains(@class,'single_retrieve')][contains(.,'本册检索')]",
            )
            if retrieve_button is not None:
                click_js(driver, retrieve_button)
            else:
                search_box.send_keys(Keys.RETURN)
            time.sleep(PAGE_PAUSE)
            if driver.find_elements(By.TAG_NAME, "table") or driver.find_elements(
                By.XPATH, "//tr[contains(@class,'single_s-tab-tbody')]"
            ):
                log.info(f"  页面内关键词检索命中: {keyword}")
                return True
        except Exception:
            continue

    return False


def detect_download_entry(driver: webdriver.Chrome) -> str | None:
    xpaths = [
        "//a[contains(., 'Excel')]",
        "//button[contains(., 'Excel')]",
        "//a[contains(., '导出')]",
        "//button[contains(., '导出')]",
        "//a[contains(., '下载')]",
        "//button[contains(., '下载')]",
    ]
    for xpath in xpaths:
        elem = visible_clickable(driver, xpath)
        if elem is not None:
            return elem.text.strip() or elem.get_attribute("title") or "download-entry"
    return None


def _parse_table_row_format(table, result: dict, tbl_idx: int) -> None:
    try:
        rows = table.find_elements(By.TAG_NAME, "tr")
    except StaleElementReferenceException:
        return

    for row in rows:
        try:
            cells = row.find_elements(By.TAG_NAME, "td") or row.find_elements(
                By.TAG_NAME, "th"
            )
            if len(cells) < 2:
                continue

            header_text = cells[0].text.strip()
            canonical = match_field(header_text)
            if canonical and result[canonical] is None:
                val = best_numeric(cells[1:])
                if val:
                    result[canonical] = val
                    log.info(
                        f'    ✓ [{canonical}] ← "{header_text}" = {val} (表格{tbl_idx + 1})'
                    )
        except StaleElementReferenceException:
            continue


def _parse_table_col_format(table, result: dict, year: int, tbl_idx: int) -> None:
    try:
        rows = table.find_elements(By.TAG_NAME, "tr")
        if len(rows) < 2:
            return

        header_row = rows[0]
        header_cells = header_row.find_elements(By.TAG_NAME, "th") or header_row.find_elements(
            By.TAG_NAME, "td"
        )
        col_map: dict[int, str] = {}
        for idx, cell in enumerate(header_cells):
            canonical = match_field(cell.text)
            if canonical:
                col_map[idx] = canonical

        if not col_map:
            return

        year_str = str(year)
        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue
            if year_str not in cells[0].text.strip():
                continue
            for col_idx, canonical in col_map.items():
                if col_idx < len(cells) and result[canonical] is None:
                    val = cells[col_idx].text.strip()
                    if val and any(ch.isdigit() for ch in val):
                        result[canonical] = val
                        log.info(
                            f"    ✓ [{canonical}] = {val} (表格{tbl_idx + 1}, 列格式)"
                        )
    except StaleElementReferenceException:
        return


def extract_income_from_tables(driver: webdriver.Chrome, result: dict, year: int) -> dict:
    tables = driver.find_elements(By.TAG_NAME, "table")
    log.info(f"  当前页共 {len(tables)} 个表格")
    for tbl_idx, table in enumerate(tables):
        try:
            table_text = table.text
        except StaleElementReferenceException:
            continue
        if all(
            keyword in table_text
            for keyword in ("条目题名", "年鉴年份", "页码", "下载")
        ):
            log.info(f"    跳过目录检索结果表（表格{tbl_idx + 1}）")
            continue
        _parse_table_row_format(table, result, tbl_idx)
        _parse_table_col_format(table, result, year, tbl_idx)
    found = sum(1 for key in INCOME_ALIASES if result[key] is not None)
    log.info(f"  提取完毕，命中 {found}/{len(INCOME_ALIASES)} 个字段")
    return result


def found_field_count(result: dict) -> int:
    return sum(1 for key in INCOME_ALIASES if result[key] is not None)


# ══════════════════════════════════════════════════════════════════
# 业务流程
# ══════════════════════════════════════════════════════════════════

def empty_result(city: str, year: int) -> dict:
    return {
        "城市": city,
        "年份": year,
        "状态": "INIT",
        "来源标题": None,
        "来源链接": None,
        "来源范围": None,
        "备注": None,
        **{field: None for field in INCOME_ALIASES},
    }


def scrape_one_city(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    city_ctx: CityContext,
    year: int,
) -> dict:
    log.info(f"━━ [{city_ctx.original_name}] ━━")
    result = empty_result(city_ctx.original_name, year)

    title, href, source_scope = open_city_yearbook(driver, city_ctx)
    if title is None:
        result["状态"] = "NO_CITY_RESULT"
        result["备注"] = "未在统计年鉴检索页找到匹配城市的年鉴条目"
        return result

    result["来源标题"] = title
    result["来源链接"] = href
    result["来源范围"] = source_scope

    if not select_year(driver, wait, year):
        result["状态"] = "NO_YEAR"
        result["备注"] = f"年鉴页面未找到年份 {year}"
        return result

    api_session = build_requests_session_from_driver(driver)
    book_code = extract_book_code_from_url(driver.current_url)
    if book_code:
        entry, api_notes = find_income_entry_via_api(
            api_session,
            book_code,
            referer=driver.current_url,
        )
        for note in api_notes:
            result["备注"] = append_note(result["备注"], note)

        if entry is not None:
            entry_title = entry.get("title") or ""
            file_code = entry.get("fileCode") or ""
            result["备注"] = append_note(result["备注"], f"接口锁定条目: {entry_title}")
            if file_code:
                result["备注"] = append_note(result["备注"], f"fileCode={file_code}")

            preview = None
            if file_code:
                try:
                    preview = preview_entry(
                        api_session,
                        file_code,
                        referer=driver.current_url,
                    )
                except requests.RequestException as exc:
                    result["备注"] = append_note(
                        result["备注"], f"条目预览接口失败: {exc}"
                    )

            if preview:
                if preview.get("wzlm"):
                    result["备注"] = append_note(
                        result["备注"], f"目录路径: {preview['wzlm']}"
                    )
                if preview.get("shdy"):
                    result["备注"] = append_note(
                        result["备注"], f"适用范围: {preview['shdy']}"
                    )
                if preview.get("bhzb"):
                    metric_count = len(
                        [item for item in str(preview["bhzb"]).split(";") if item.strip()]
                    )
                    result["备注"] = append_note(
                        result["备注"], f"条目指标数: {metric_count}"
                    )

                if not entry_scope_matches_city(preview, entry, city_ctx, source_scope):
                    result["状态"] = "NO_INCOME_TABLE"
                    result["备注"] = append_note(
                        result["备注"], "仅找到省级汇总或非目标地区条目，按无相关城市数据跳过"
                    )
                    return result

                if should_skip_multi_region_parse(
                    preview,
                    city_ctx,
                    source_scope,
                ):
                    result["状态"] = "NO_INCOME_TABLE"
                    result["备注"] = append_note(
                        result["备注"],
                        "命中省级多地区汇总表，当前版本尚不解析其中的城市维度，按无相关城市数据跳过",
                    )
                    return result

            if file_code:
                related_entries = find_related_income_entries(
                    api_session,
                    book_code,
                    entry,
                    referer=driver.current_url,
                )
                if len(related_entries) > 1:
                    result["备注"] = append_note(
                        result["备注"],
                        "同组条目: " + " | ".join(
                            item.get("title", "") for item in related_entries
                        ),
                    )

                parsed_existing_count = 0
                for related_entry in related_entries:
                    related_file_code = related_entry.get("fileCode") or ""
                    related_downloads = find_existing_downloads(related_file_code)
                    if not related_downloads:
                        continue
                    existing_path = related_downloads[0]
                    parsed_existing_count += 1
                    result["备注"] = append_note(
                        result["备注"],
                        f"复用已有下载文件: {existing_path.name}",
                    )
                    try:
                        result = extract_income_from_excel(existing_path, result)
                    except Exception as exc:
                        result["备注"] = append_note(
                            result["备注"], f"已有 Excel 解析失败: {exc}"
                        )

                if parsed_existing_count:
                    result["备注"] = append_note(
                        result["备注"],
                        f"已复用同组下载文件 {parsed_existing_count}/{len(related_entries)} 份",
                    )
                    found = found_field_count(result)
                    if found == len(INCOME_ALIASES):
                        result["状态"] = "EXCEL_PARSED"
                        return result

                download_keywords = (
                    "农村",
                    "纯收入",
                    "收支",
                    "住户",
                    "农民",
                    "人民生活",
                )
                newly_downloaded = 0
                for related_entry in related_entries:
                    related_title = related_entry.get("title") or ""
                    related_file_code = related_entry.get("fileCode") or ""
                    if find_existing_downloads(related_file_code):
                        continue

                    downloaded_path = None
                    download_note = None
                    for keyword in download_keywords:
                        downloaded_path, download_note = attempt_row_excel_download(
                            driver,
                            keyword=keyword,
                            entry_title=related_title,
                        )
                        if downloaded_path is not None or download_note:
                            break

                        downloaded_path = attempt_modal_excel_download(
                            driver,
                            keyword=keyword,
                            entry_title=related_title,
                        )
                        if downloaded_path is not None:
                            break

                    if download_note:
                        result["备注"] = append_note(result["备注"], download_note)

                    if downloaded_path is None:
                        continue

                    newly_downloaded += 1
                    result["备注"] = append_note(
                        result["备注"],
                        f"页面点击下载成功: {downloaded_path.name}",
                    )
                    try:
                        result = extract_income_from_excel(downloaded_path, result)
                    except Exception as exc:
                        result["备注"] = append_note(
                            result["备注"], f"Excel 解析失败: {exc}"
                        )

                    found = found_field_count(result)
                    if found == len(INCOME_ALIASES):
                        result["状态"] = "EXCEL_PARSED"
                        return result

                if newly_downloaded:
                    result["备注"] = append_note(
                        result["备注"],
                        f"本次自动下载同组文件 {newly_downloaded} 份",
                    )
                    found = found_field_count(result)
                    if found:
                        result["状态"] = "EXCEL_PARSED"
                        return result

            if file_code:
                try:
                    download_url = request_download_url(
                        api_session,
                        file_code,
                        referer=driver.current_url,
                    )
                except requests.RequestException as exc:
                    download_url = None
                    result["备注"] = append_note(
                        result["备注"], f"下载链接接口失败: {exc}"
                    )

                if download_url:
                    result["备注"] = append_note(
                        result["备注"], "已拿到 Excel 下载链接"
                    )
                    downloaded_path, download_note = attempt_browser_download(
                        driver, download_url
                    )
                    if downloaded_path is not None:
                        result["备注"] = append_note(
                            result["备注"], f"下载成功: {downloaded_path.name}"
                        )
                        try:
                            result = extract_income_from_excel(downloaded_path, result)
                        except Exception as exc:
                            result["备注"] = append_note(
                                result["备注"], f"Excel 解析失败: {exc}"
                            )
                        else:
                            found = found_field_count(result)
                            if found:
                                result["状态"] = "EXCEL_PARSED"
                                return result
                            result["状态"] = "EXCEL_DOWNLOADED"
                            return result
                    else:
                        result["备注"] = append_note(
                            result["备注"], "浏览器侧未捕获到实际下载文件"
                        )
                        if download_note:
                            result["备注"] = append_note(result["备注"], download_note)
    else:
        result["备注"] = append_note(result["备注"], "当前页面 URL 中未提取到 book code")

    navigate_to_income_section(driver)
    search_income_keywords(driver)

    download_entry = detect_download_entry(driver)
    if download_entry:
        result["备注"] = append_note(
            result["备注"], f"检测到页面下载入口: {download_entry}"
        )

    result = extract_income_from_tables(driver, result, year)

    found = found_field_count(result)
    if found:
        result["状态"] = "PARTIAL_TABLE_PARSE"
        if not result["备注"]:
            result["备注"] = "已通过页面表格兜底提取部分字段；下一阶段应切到下载 Excel 解析"
    else:
        result["状态"] = "NO_INCOME_TABLE"
        if not result["备注"]:
            result["备注"] = "已成功定位城市与年份，但当前页未直接解析出收入字段"

    return result


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 CNKI 中国经济社会大数据平台抓取城市统计年鉴农村居民收入数据"
    )
    parser.add_argument("--year", type=int, default=2000, help="目标年份（默认 2000）")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="断点续爬：跳过输出文件中已有记录的城市",
    )
    parser.add_argument(
        "--city",
        type=str,
        default=None,
        help="只爬单个城市（调试用），例如 --city 北京市",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="无头模式（不显示浏览器窗口）",
    )
    parser.add_argument(
        "--sanitize-only",
        action="store_true",
        help="只离线清洗已有结果，不启动浏览器抓取",
    )
    parser.add_argument(
        "--retry-incomplete",
        action="store_true",
        help="只重跑当前年份中状态不是 EXCEL_PARSED 的城市",
    )
    parser.add_argument(
        "--retry-statuses",
        type=str,
        default=None,
        help="只重跑指定状态的城市，多个状态用逗号分隔，例如 NO_YEAR,ERROR",
    )
    return parser.parse_args()


def save_excel(rows: list[dict], path: Path) -> None:
    cols = [
        "城市",
        "年份",
        "状态",
        "来源标题",
        "来源链接",
        "来源范围",
        "备注",
        *INCOME_ALIASES.keys(),
    ]
    df = pd.DataFrame(rows, columns=cols)
    df.to_excel(path, index=False)


def merge_result_rows(rows: list[dict], new_row: dict) -> list[dict]:
    city = str(new_row.get("城市", ""))
    year = str(new_row.get("年份", ""))
    kept = [
        row
        for row in rows
        if not (
            str(row.get("城市", "")) == city
            and str(row.get("年份", "")) == year
        )
    ]
    kept.append(new_row)
    return kept


def sanitize_existing_results(
    rows: list[dict],
    city_contexts: dict[str, CityContext],
) -> tuple[list[dict], int]:
    sanitized: list[dict] = []
    changed = 0
    income_fields = list(INCOME_ALIASES.keys())

    for row in rows:
        updated = dict(row)
        city = str(updated.get("城市", "") or "")
        source_scope = str(updated.get("来源范围", "") or "")
        status = str(updated.get("状态", "") or "")
        note = str(updated.get("备注", "") or "")

        should_clear = (
            status == "PARTIAL_TABLE_PARSE"
            and source_scope == "PROVINCIAL"
            and "适用范围:" in note
            and (";" in note or "；" in note)
        )
        if should_clear:
            ctx = resolve_city_context(city, city_contexts)
            if not ctx.is_province_level and "命中省级多地区汇总表" not in note:
                updated["状态"] = "NO_INCOME_TABLE"
                updated["备注"] = append_note(
                    updated.get("备注"),
                    "离线清洗：省级多地区汇总表不作为城市结果，已清空误提取字段",
                )
                for field in income_fields:
                    updated[field] = None
                changed += 1

        sanitized.append(updated)

    return sanitized, changed


def extract_download_file_names_from_note(note: str | None) -> list[str]:
    if not note:
        return []
    names = []
    for match in DOWNLOAD_FILE_NAME_RE.findall(str(note)):
        name = Path(match).name
        if name not in names:
            names.append(name)
    return names


def refresh_existing_results_from_downloads(
    rows: list[dict],
    city_contexts: dict[str, CityContext],
) -> tuple[list[dict], int]:
    refreshed_rows: list[dict] = []
    changed = 0

    for row in rows:
        updated = dict(row)
        city = str(updated.get("城市", "") or "")
        note = str(updated.get("备注", "") or "")
        source_scope = str(updated.get("来源范围", "") or "")
        ctx = resolve_city_context(city, city_contexts)

        if (
            not ctx.is_province_level
            and source_scope == "PROVINCIAL"
            and "多地区汇总表" in note
        ):
            refreshed_rows.append(updated)
            continue

        file_names = extract_download_file_names_from_note(note)
        if not file_names:
            refreshed_rows.append(updated)
            continue

        parsed_result = empty_result(city, int(str(updated.get("年份", "0") or "0")))
        parsed_any = False
        for file_name in file_names:
            file_path = DOWNLOAD_DIR / file_name
            if not file_path.exists():
                continue
            try:
                parsed_result = extract_income_from_excel(file_path, parsed_result)
                parsed_any = True
            except Exception as exc:
                updated["备注"] = append_note(updated.get("备注"), f"离线刷新失败: {exc}")

        if parsed_any:
            before = {field: updated.get(field) for field in INCOME_ALIASES}
            after = {field: parsed_result.get(field) for field in INCOME_ALIASES}
            if before != after:
                changed += 1
                for field, value in after.items():
                    updated[field] = value
                updated["备注"] = append_note(
                    updated.get("备注"),
                    f"离线刷新：已重新解析 {len(file_names)} 份下载文件",
                )

        refreshed_rows.append(updated)

    return refreshed_rows, changed


def latest_results_by_city(rows: list[dict], year: int) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    year_str = str(year)
    for row in rows:
        if str(row.get("年份", "")) != year_str:
            continue
        city = str(row.get("城市", "") or "")
        if city:
            latest[city] = row
    return latest


def parse_retry_statuses(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {
        token.strip()
        for token in str(raw).split(",")
        if token.strip()
    }


def main() -> None:
    args = parse_args()
    year = args.year
    output_file = OUTPUT_DIR / f"rural_income_{year}.xlsx"

    all_cities = load_cities()
    city_contexts = build_city_contexts(all_cities)
    cities = all_cities
    if args.city:
        cities = [args.city]
        log.info(f"单城市调试模式: {args.city}")

    already_done: set[str] = set()
    all_results: list[dict] = []
    if output_file.exists():
        df_old = pd.read_excel(output_file, dtype=str)
        all_results = df_old.to_dict("records")
        all_results, sanitized_count = sanitize_existing_results(
            all_results,
            city_contexts,
        )
        refreshed_count = 0
        all_results, refreshed_count = refresh_existing_results_from_downloads(
            all_results,
            city_contexts,
        )
        if sanitized_count or refreshed_count:
            save_excel(all_results, output_file)
            if sanitized_count:
                log.info(f"已离线清洗 {sanitized_count} 条历史结果")
            if refreshed_count:
                log.info(f"已离线刷新 {refreshed_count} 条历史结果")
        if args.resume:
            already_done = {str(row.get("城市", "")) for row in all_results}
            log.info(f"断点续爬：已有 {len(already_done)} 条记录")
        else:
            log.info(f"已加载历史结果 {len(all_results)} 条；本次将按城市覆盖更新")

    if args.sanitize_only:
        log.info("仅执行离线清洗，不启动浏览器")
        return

    latest_by_city = latest_results_by_city(all_results, year)
    retry_statuses = parse_retry_statuses(args.retry_statuses)
    if args.retry_incomplete:
        retry_statuses.update(
            {
                "INIT",
                "NO_CITY_RESULT",
                "NO_YEAR",
                "NO_INCOME_TABLE",
                "PARTIAL_TABLE_PARSE",
                "EXCEL_DOWNLOADED",
                "ERROR",
            }
        )

    if retry_statuses:
        pending_cities = []
        for city in cities:
            latest_row = latest_by_city.get(city)
            if latest_row is None:
                pending_cities.append(city)
                continue
            if str(latest_row.get("状态", "") or "") in retry_statuses:
                pending_cities.append(city)
        log.info(
            "按状态重跑：%s；待处理 %s 个城市",
            ",".join(sorted(retry_statuses)),
            len(pending_cities),
        )
    else:
        pending_cities = [city for city in cities if city not in already_done]
    if not pending_cities:
        log.info("没有待处理城市，退出")
        return

    driver = create_driver(headless=args.headless)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        if not open_cnki_platform(driver, wait):
            log.error("无法打开 CNKI 平台，退出")
            return

        if not open_yearbook_search_page(driver, wait):
            log.error("无法进入统计年鉴检索页，退出")
            return

        total = len(pending_cities)
        for idx, city in enumerate(pending_cities, 1):
            log.info(f"[{idx}/{total}]")
            try:
                city_ctx = resolve_city_context(city, city_contexts)
                log.info(
                    f"  目标映射: 规范名={city_ctx.normalized_name}, 省份={city_ctx.province_name}, 大区={city_ctx.region_group}"
                )
                result = scrape_one_city(driver, wait, city_ctx, year)
            except Exception as exc:
                log.error(f"  [{city}] 意外错误: {exc}", exc_info=True)
                take_screenshot(driver, f"error_{city}")
                result = empty_result(city, year)
                result["状态"] = "ERROR"
                result["备注"] = str(exc)

            all_results = merge_result_rows(all_results, result)
            save_excel(all_results, output_file)
            log.info(f"  → 进度已保存（共 {len(all_results)} 条）")

        log.info(
            f"\n✓ 全部完成！共 {len(all_results)} 个城市，结果保存在 {output_file}"
        )

    finally:
        driver.quit()


if __name__ == "__main__":
    main()

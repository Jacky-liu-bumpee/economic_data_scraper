#!/usr/bin/env python3
"""
农村居民收入数据爬取脚本
目标数据库: 复旦大学图书馆 → CNKI 中国经济大数据平台 → 统计资料

用法:
    python income_scraper.py                   # 抓2000年，全部城市
    python income_scraper.py --year 2005       # 指定年份
    python income_scraper.py --resume          # 跳过已有记录，断点续爬
    python income_scraper.py --city 北京市     # 只跑单个城市（调试用）
    python income_scraper.py --headless        # 无头模式（不弹出浏览器窗口）
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
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
# 配置区（如平台改版，先在这里调整）
# ══════════════════════════════════════════════════════════════════

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

CITY_LIST_FILE = DATA_DIR / "city_list.xlsx"
FUDAN_LIBRARY_URL = "https://library.fudan.edu.cn/"
FUDAN_DB_NAV_URL  = "https://libdbnav.fudan.edu.cn/database/navigation"

WAIT_TIMEOUT = 20   # 元素等待超时（秒）
PAGE_PAUSE = 2.0    # 页面跳转后基础等待（秒）

# ──────────────────────────────────────────────────────────────────
# 收入字段别名映射
#   key   = 输出 Excel 的列名（规范化字段名）
#   value = 年鉴表格中可能出现的行/列标题列表（含模糊匹配项）
# 如果遇到新城市用了其他叫法，只需在对应列表里追加即可
# ──────────────────────────────────────────────────────────────────
INCOME_ALIASES: dict[str, list[str]] = {
    "农村人均收入": [
        "农村居民家庭人均纯收入",
        "农村居民人均纯收入",
        "农村人均纯收入",
        "人均纯收入",
        "农村人均总收入",
        "人均总收入",
        "农村居民人均收入",
        "农村居民收入",
    ],
    "工资性收入": [
        "工资性收入",
        "劳动报酬收入",
        "报酬性收入",
        "工资收入",
        "工薪收入",
        "劳动报酬",
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

# 在目录(TOC)中定位「农村收入」章节时使用的关键词（优先级从前到后）
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

# 跳过城市列表中的无效行
SKIP_NAMES = {"城市", "城 市", "城市合计", "nan", ""}

# ══════════════════════════════════════════════════════════════════
# 日志初始化
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
# 城市列表读取
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


# ══════════════════════════════════════════════════════════════════
# 浏览器
# ══════════════════════════════════════════════════════════════════

def create_driver(headless: bool = False) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,900")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)


# ══════════════════════════════════════════════════════════════════
# 通用工具
# ══════════════════════════════════════════════════════════════════

def wait_click(wait: WebDriverWait, locator: tuple, msg: str = "") -> None:
    """等待元素可点击后点击"""
    elem = wait.until(EC.element_to_be_clickable(locator), msg)
    elem.click()


def switch_to_newest_window(driver: webdriver.Chrome, old_handles: set) -> bool:
    """等待并切换到新打开的标签页/窗口"""
    for _ in range(30):
        new = set(driver.window_handles) - old_handles
        if new:
            driver.switch_to.window(new.pop())
            return True
        time.sleep(0.5)
    return False


def match_field(text: str) -> str | None:
    """
    将文本与 INCOME_ALIASES 匹配，返回规范字段名。
    使用「包含」而非精确匹配，应对各种表述差异。
    """
    text = text.strip()
    if not text:
        return None
    for canonical, aliases in INCOME_ALIASES.items():
        for alias in aliases:
            if alias in text or text in alias:
                return canonical
    return None


def best_numeric(cells: list) -> str | None:
    """
    从单元格列表中取最后一个含数字的文本值。
    跳过空值、横杠、省略号等占位符。
    """
    SKIP = {"-", "—", "－", "…", "...", "/", ""}
    for cell in reversed(cells):
        try:
            val = cell.text.strip()
        except StaleElementReferenceException:
            continue
        if val in SKIP:
            continue
        if any(ch.isdigit() for ch in val):
            return val
    return None


def take_screenshot(driver: webdriver.Chrome, name: str) -> None:
    path = OUTPUT_DIR / f"debug_{name}.png"
    try:
        driver.save_screenshot(str(path))
        log.debug(f"截图: {path}")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
# 第一阶段：从复旦图书馆导航到 CNKI 平台
# ══════════════════════════════════════════════════════════════════

def open_cnki_platform(driver: webdriver.Chrome, wait: WebDriverWait) -> bool:
    """
    直接打开复旦数据库导航页 → 搜索「中国经济大数据」→ 点击结果进入 CNKI 平台。
    成功后返回 True，driver 已位于 CNKI 平台页面。
    """
    log.info(f"▶ 打开数据库导航页: {FUDAN_DB_NAV_URL}")
    driver.get(FUDAN_DB_NAV_URL)
    time.sleep(PAGE_PAUSE * 2)   # Vue/React 页面需要等待渲染

    try:
        # ── Step 1: 在页面搜索框中输入「中国经济大数据」──────────────
        search_box = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 "//input[contains(@placeholder,'数据库')"
                 "        or contains(@placeholder,'搜索')"
                 "        or contains(@placeholder,'出版社')]")
            )
        )
        search_box.clear()
        search_box.send_keys("中国经济社会大数据")
        time.sleep(0.4)
        search_box.send_keys(Keys.RETURN)
        time.sleep(PAGE_PAUSE)

        # ── Step 2: 点击结果中的数据库链接 ──────────────────────────
        old_handles = set(driver.window_handles)
        span = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[contains(text(),'经济社会大数据')]")
            ),
            "搜索后仍找不到「中国经济社会大数据」链接",
        )
        log.info(f"  找到平台链接: {span.text.strip()[:60]}")
        span.click()   # span.click() 触发 Vue 冒泡，开新标签页

        if switch_to_newest_window(driver, old_handles):
            log.info(f"  切换到新窗口: {driver.current_url}")
        time.sleep(PAGE_PAUSE * 2)

        log.info(f"▶ 已到达 CNKI 平台: {driver.current_url}")
        return True

    except TimeoutException as exc:
        log.error(f"导航到 CNKI 平台失败: {exc}")
        take_screenshot(driver, "open_cnki_fail")
        return False


def click_stat_section(driver: webdriver.Chrome, wait: WebDriverWait) -> bool:
    """点击「统计资料」Tab，进入统计年鉴搜索区域"""
    try:
        wait_click(
            wait,
            (By.XPATH, "//*[normalize-space(text())='统计资料'"
                       " or contains(@class,'stat') and contains(text(),'统计资料')]"),
            "找不到「统计资料」",
        )
        time.sleep(PAGE_PAUSE)
        return True
    except TimeoutException:
        log.error("找不到「统计资料」Tab，请确认已在 CNKI 平台主页")
        take_screenshot(driver, "stat_section_fail")
        return False


# ══════════════════════════════════════════════════════════════════
# 第二阶段：搜索城市年鉴 → 选年份
# ══════════════════════════════════════════════════════════════════

# XPath 候选集：搜索输入框（多种平台可能用不同属性）
_SEARCH_BOX_XPATHS = [
    "//input[contains(@placeholder,'搜索') or contains(@placeholder,'请输入')]",
    "//input[@type='search']",
    "//input[@type='text'][not(ancestor::form[contains(@class,'login')])]",
]

# XPath 候选集：搜索结果第一条链接
_FIRST_RESULT_XPATHS = [
    "(//ul[contains(@class,'result')]//a)[1]",
    "(//ul[contains(@class,'list')]//li//a)[1]",
    "(//div[contains(@class,'result-list')]//a)[1]",
    "(//div[contains(@class,'item')]//a)[1]",
    "(//li[contains(@class,'item')]//a)[1]",
    # 兜底：页面上第一个含「年鉴」字样的链接
    "//a[contains(text(),'年鉴')][1]",
]


def search_city(driver: webdriver.Chrome, wait: WebDriverWait, city: str) -> bool:
    """在统计资料搜索框输入城市名并提交"""
    box = None
    for xpath in _SEARCH_BOX_XPATHS:
        try:
            box = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            break
        except TimeoutException:
            continue

    if box is None:
        log.warning(f"  [{city}] 找不到搜索框")
        take_screenshot(driver, f"no_search_box_{city}")
        return False

    try:
        box.clear()
        box.send_keys(city)
        time.sleep(0.4)

        # 先尝试点搜索按钮，找不到就按回车
        try:
            btn = driver.find_element(
                By.XPATH,
                "//button[contains(text(),'搜索') or contains(text(),'查询')"
                " or contains(@class,'search-btn') or contains(@aria-label,'搜索')]",
            )
            btn.click()
        except NoSuchElementException:
            box.send_keys(Keys.RETURN)

        time.sleep(PAGE_PAUSE)
        return True

    except Exception as exc:
        log.warning(f"  [{city}] 搜索失败: {exc}")
        return False


def click_first_yearbook(
    driver: webdriver.Chrome, wait: WebDriverWait, city: str
) -> bool:
    """点击搜索结果中第一条年鉴链接"""
    first = None
    for xpath in _FIRST_RESULT_XPATHS:
        try:
            first = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            break
        except TimeoutException:
            continue

    if first is None:
        log.warning(f"  [{city}] 找不到年鉴搜索结果")
        take_screenshot(driver, f"no_result_{city}")
        return False

    old_handles = set(driver.window_handles)
    log.info(f"  [{city}] 点击: {first.text[:60]}")
    first.click()

    # 若在新标签页打开，切换过去
    if len(driver.window_handles) > len(old_handles):
        switch_to_newest_window(driver, old_handles)

    time.sleep(PAGE_PAUSE * 2)
    return True


def select_year(driver: webdriver.Chrome, wait: WebDriverWait, year: int) -> bool:
    """
    在年鉴页面点击指定年份。
    年份通常出现在左侧年份列表或顶部导航条中，为纯数字文本的可点击元素。
    """
    year_str = str(year)
    # 优先在年份导航区域查找（避免误点表格内的数字）
    xpaths = [
        f"//ul[contains(@class,'year')]//a[normalize-space(text())='{year_str}']",
        f"//ul[contains(@class,'nav')]//a[normalize-space(text())='{year_str}']",
        f"//div[contains(@class,'year')]//a[normalize-space(text())='{year_str}']",
        f"//li[contains(@class,'year')]//a[normalize-space(text())='{year_str}']",
        # 宽松兜底：任意 <a> 或 <span> 文本精确等于年份
        f"//a[normalize-space(text())='{year_str}']",
        f"//span[normalize-space(text())='{year_str}']",
    ]
    for xpath in xpaths:
        try:
            elem = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            log.info(f"  找到年份 {year_str}，点击...")
            elem.click()
            time.sleep(PAGE_PAUSE * 2)
            return True
        except TimeoutException:
            continue

    log.warning(f"  找不到年份 {year_str}")
    take_screenshot(driver, f"no_year_{year}")
    return False


# ══════════════════════════════════════════════════════════════════
# 第三阶段：在年鉴中定位并提取收入数据
# ══════════════════════════════════════════════════════════════════

def navigate_to_income_section(driver: webdriver.Chrome) -> None:
    """
    在年鉴目录（左侧树/列表）中寻找农村收入相关章节并点击。
    按 TOC_INCOME_KEYWORDS 的顺序尝试，找到即停。
    找不到时保持当前页，后续直接扫全表。
    """
    for kw in TOC_INCOME_KEYWORDS:
        try:
            xpath = (
                f"//*[contains(text(),'{kw}')]"
                "[not(self::script)][not(self::style)]"
            )
            candidates = driver.find_elements(By.XPATH, xpath)
            # 过滤掉不可见或文字过长（可能是正文段落而非目录项）的节点
            clickable = [
                e for e in candidates
                if e.is_displayed() and len(e.text.strip()) <= 30
            ]
            if clickable:
                log.info(f"  目录中找到「{kw}」相关节点: {[e.text.strip() for e in clickable[:2]]}")
                clickable[0].click()
                time.sleep(PAGE_PAUSE)
                return
        except Exception:
            continue
    log.info("  目录中未找到收入相关节点，直接扫描当前页所有表格")


def _parse_table_row_format(table, result: dict, tbl_idx: int) -> None:
    """
    处理「行=指标，列=年份/数值」格式：
    遍历每行，若第一格文字命中收入字段，则取该行最后一个数字单元格。
    """
    try:
        rows = table.find_elements(By.TAG_NAME, "tr")
    except StaleElementReferenceException:
        return

    for row in rows:
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                cells = row.find_elements(By.TAG_NAME, "th")
            if len(cells) < 2:
                continue

            header_text = cells[0].text.strip()
            canonical = match_field(header_text)
            if canonical and result[canonical] is None:
                val = best_numeric(cells[1:])
                if val:
                    result[canonical] = val
                    log.info(
                        f"    ✓ [{canonical}] ← \"{header_text}\" = {val}"
                        f"  (表格{tbl_idx + 1})"
                    )
        except StaleElementReferenceException:
            continue


def _parse_table_col_format(table, result: dict, year: int, tbl_idx: int) -> None:
    """
    处理「列=指标，行=年份」格式：
    识别表头行中命中收入字段的列，再找含目标年份的数据行取值。
    """
    try:
        rows = table.find_elements(By.TAG_NAME, "tr")
        if len(rows) < 2:
            return

        # 解析表头（可能是 <th> 或第一行 <td>）
        header_row = rows[0]
        header_cells = header_row.find_elements(By.TAG_NAME, "th") or \
                       header_row.find_elements(By.TAG_NAME, "td")
        col_map: dict[int, str] = {}
        for i, hc in enumerate(header_cells):
            canonical = match_field(hc.text)
            if canonical:
                col_map[i] = canonical

        if not col_map:
            return

        year_str = str(year)
        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue
            first_cell_text = cells[0].text.strip()
            if year_str not in first_cell_text:
                continue
            for col_idx, canonical in col_map.items():
                if col_idx < len(cells) and result[canonical] is None:
                    val = cells[col_idx].text.strip()
                    if val and any(ch.isdigit() for ch in val):
                        result[canonical] = val
                        log.info(
                            f"    ✓ [{canonical}] = {val}"
                            f"  (表格{tbl_idx + 1}, 列格式)"
                        )
    except StaleElementReferenceException:
        return


def extract_income_from_tables(
    driver: webdriver.Chrome, city: str, year: int
) -> dict:
    """
    扫描当前页面所有 <table>，提取收入数据。
    同时支持「行=指标」和「列=指标」两种表格格式。
    """
    result: dict = {"城市": city, "年份": year}
    for field in INCOME_ALIASES:
        result[field] = None

    tables = driver.find_elements(By.TAG_NAME, "table")
    log.info(f"  当前页共 {len(tables)} 个表格")

    for tbl_idx, table in enumerate(tables):
        _parse_table_row_format(table, result, tbl_idx)
        _parse_table_col_format(table, result, year, tbl_idx)

    found = sum(1 for f in INCOME_ALIASES if result[f] is not None)
    log.info(f"  [{city}] 提取完毕，命中 {found}/{len(INCOME_ALIASES)} 个字段")
    return result


# ══════════════════════════════════════════════════════════════════
# 单城市完整流程
# ══════════════════════════════════════════════════════════════════

def scrape_one_city(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    city: str,
    year: int,
    stat_url: str,
) -> dict:
    """
    爬取单个城市的收入数据。
    stat_url 是「统计资料」搜索页的 URL，每个城市开始前回到这里。
    """
    log.info(f"━━ [{city}] ━━")
    empty = {"城市": city, "年份": year, **{f: None for f in INCOME_ALIASES}}

    # 回到统计资料搜索页
    try:
        driver.get(stat_url)
        time.sleep(PAGE_PAUSE)
    except Exception as exc:
        log.warning(f"  返回搜索页失败: {exc}")

    # 搜索城市名
    if not search_city(driver, wait, city):
        return empty

    # 点击第一条年鉴结果
    if not click_first_yearbook(driver, wait, city):
        return empty

    # 点击目标年份
    if not select_year(driver, wait, year):
        return empty

    # 导航到收入相关目录章节
    navigate_to_income_section(driver)

    # 提取表格数据
    result = extract_income_from_tables(driver, city, year)

    # 若主窗口以外还有新标签页，关闭它们回到主窗口
    handles = driver.window_handles
    if len(handles) > 1:
        for h in handles[1:]:
            driver.switch_to.window(h)
            driver.close()
        driver.switch_to.window(handles[0])

    return result


# ══════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="从 CNKI 中国经济大数据平台抓取城市统计年鉴农村居民收入数据"
    )
    p.add_argument("--year", type=int, default=2000, help="目标年份（默认 2000）")
    p.add_argument(
        "--resume",
        action="store_true",
        help="断点续爬：跳过输出文件中已有记录的城市",
    )
    p.add_argument(
        "--city",
        type=str,
        default=None,
        help="只爬单个城市（调试用），例如 --city 北京市",
    )
    p.add_argument(
        "--headless",
        action="store_true",
        help="无头模式（不显示浏览器窗口）",
    )
    return p.parse_args()


def save_excel(rows: list[dict], path: Path) -> None:
    cols = ["城市", "年份"] + list(INCOME_ALIASES.keys())
    df = pd.DataFrame(rows, columns=cols)
    df.to_excel(path, index=False)


def main() -> None:
    args = parse_args()
    year: int = args.year
    output_file = OUTPUT_DIR / f"rural_income_{year}.xlsx"

    # ── 读城市列表 ──────────────────────────────────────────────
    cities = load_cities()
    if args.city:
        cities = [args.city]
        log.info(f"单城市调试模式: {args.city}")

    # ── 断点续爬 ─────────────────────────────────────────────────
    already_done: set[str] = set()
    all_results: list[dict] = []
    if args.resume and output_file.exists():
        df_old = pd.read_excel(output_file, dtype=str)
        all_results = df_old.to_dict("records")
        already_done = {str(r.get("城市", "")) for r in all_results}
        log.info(f"断点续爬：已有 {len(already_done)} 条记录")

    # ── 启动浏览器 ───────────────────────────────────────────────
    driver = create_driver(headless=args.headless)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        # 一次性导航到 CNKI 平台
        if not open_cnki_platform(driver, wait):
            log.error("无法打开 CNKI 平台，退出")
            return

        if not click_stat_section(driver, wait):
            log.error("无法进入「统计资料」，退出")
            return

        # 记录「统计资料」搜索页 URL，用于每城市前复位
        stat_url = driver.current_url
        log.info(f"统计资料页 URL: {stat_url}")

        total = len(cities)
        for idx, city in enumerate(cities, 1):
            if city in already_done:
                log.info(f"[{idx}/{total}] 跳过（已有数据）: {city}")
                continue

            log.info(f"[{idx}/{total}]")
            try:
                result = scrape_one_city(driver, wait, city, year, stat_url)
            except Exception as exc:
                log.error(f"  [{city}] 意外错误: {exc}", exc_info=True)
                take_screenshot(driver, f"error_{city}")
                result = {
                    "城市": city,
                    "年份": year,
                    **{f: None for f in INCOME_ALIASES},
                }

            all_results.append(result)

            # 每城市完成后立即存盘，防崩溃丢失
            save_excel(all_results, output_file)
            log.info(f"  → 进度已保存（共 {len(all_results)} 条）")

        log.info(
            f"\n✓ 全部完成！共 {len(all_results)} 个城市，"
            f"结果保存在 {output_file}"
        )

    finally:
        driver.quit()


if __name__ == "__main__":
    main()

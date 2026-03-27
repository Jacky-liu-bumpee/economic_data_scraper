"""
从国家统计局数据库抓取GDP数据
目标：https://data.stats.gov.cn/easyquery.htm?cn=C01
"""

import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=opts)
    return driver


def scrape_gdp():
    url = "https://data.stats.gov.cn/easyquery.htm?cn=C01"
    print(f"打开页面: {url}")

    driver = create_driver()
    wait = WebDriverWait(driver, 20)

    try:
        driver.get(url)
        time.sleep(3)

        # 等待左侧指标树加载
        wait.until(EC.presence_of_element_located((By.ID, "myNaviTree")))
        print("页面加载完成，查找GDP指标...")

        # 在指标树中找"国内生产总值"节点
        gdp_node = None
        tree_items = driver.find_elements(By.CSS_SELECTOR, "#myNaviTree span.curSelectedNode, #myNaviTree a")
        for item in tree_items:
            if "国内生产总值" in item.text or "GDP" in item.text.upper():
                gdp_node = item
                print(f"找到节点: {item.text}")
                break

        if not gdp_node:
            # 尝试搜索输入框
            search_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text']")
            for inp in search_inputs:
                try:
                    inp.clear()
                    inp.send_keys("国内生产总值")
                    time.sleep(1)
                    break
                except Exception:
                    continue

        time.sleep(2)

        # 抓取数据表格
        print("尝试抓取数据表格...")
        rows_data = []

        # 等待数据表格出现
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tableClass, #dataView table, .dtable")))
        except Exception:
            print("等待表格超时，尝试直接抓取...")

        tables = driver.find_elements(By.TAG_NAME, "table")
        print(f"找到 {len(tables)} 个表格")

        for i, table in enumerate(tables):
            rows = table.find_elements(By.TAG_NAME, "tr")
            if len(rows) > 3:
                print(f"  表格 {i+1}: {len(rows)} 行")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td") or row.find_elements(By.TAG_NAME, "th")
                    if cells:
                        row_text = [c.text.strip() for c in cells]
                        if any(row_text):
                            rows_data.append(row_text)

        if rows_data:
            # 将数据保存为CSV
            max_cols = max(len(r) for r in rows_data)
            df = pd.DataFrame(rows_data, columns=[f"col_{i}" for i in range(max_cols)])
            output_path = DATA_DIR / "gdp_data.csv"
            df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"\n数据已保存到: {output_path}")
            print(f"共 {len(df)} 行，{len(df.columns)} 列")
            print("\n前5行预览:")
            print(df.head().to_string())
        else:
            print("未能从表格获取数据，保存页面源码供分析...")
            page_source_path = OUTPUT_DIR / "page_source.html"
            with open(page_source_path, "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print(f"页面源码已保存到: {page_source_path}")

        # 截图
        screenshot_path = OUTPUT_DIR / "gdp_screenshot.png"
        driver.save_screenshot(str(screenshot_path))
        print(f"截图已保存到: {screenshot_path}")

    finally:
        driver.quit()


if __name__ == "__main__":
    scrape_gdp()

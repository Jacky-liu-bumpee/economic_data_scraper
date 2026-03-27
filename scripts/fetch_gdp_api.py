"""
国家统计局 GDP 数据获取脚本
数据库：hgnd（宏观年度）
指标：A0201 国内生产总值相关
"""

import json
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://data.stats.gov.cn/easyquery.htm?cn=C01",
}
BASE_URL = "https://data.stats.gov.cn/easyquery.htm"


def query(dbcode, valuecode):
    params = {
        "m": "QueryData",
        "dbcode": dbcode,
        "rowcode": "zb",
        "colcode": "sj",
        "wds": "[]",
        "dfwds": json.dumps([{"wdcode": "zb", "valuecode": valuecode}]),
        "k1": "1",
    }
    resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_to_df(data):
    nodes = data["returndata"]["datanodes"]
    wdnodes = data["returndata"]["wdnodes"]

    # 建立指标 code -> 名称/单位 映射
    zb_map = {}
    for w in wdnodes:
        if w["wdcode"] == "zb":
            for n in w["nodes"]:
                zb_map[n["code"]] = {"name": n["cname"], "unit": n.get("unit", "")}

    records = []
    for node in nodes:
        if not node["data"]["hasdata"]:
            continue
        wds = {w["wdcode"]: w["valuecode"] for w in node["wds"]}
        zb_code = wds.get("zb", "")
        year = wds.get("sj", "")
        info = zb_map.get(zb_code, {})
        records.append({
            "年份": year,
            "指标": info.get("name", zb_code),
            "单位": info.get("unit", ""),
            "数值": node["data"]["data"],
        })

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values(["指标", "年份"], ascending=[True, False])
    return df


def main():
    print("正在从国家统计局获取年度GDP数据...")
    data = query("hgnd", "A0201")

    df = parse_to_df(data)
    print(f"获取到 {len(df)} 条记录，涵盖以下指标：")
    for name in df["指标"].unique():
        print(f"  - {name}")

    # 透视为宽表
    df_wide = df.pivot_table(index="年份", columns="指标", values="数值", aggfunc="first")
    df_wide = df_wide.sort_index(ascending=False)

    # 保存
    out_csv = DATA_DIR / "gdp_data.csv"
    df_wide.to_csv(out_csv, encoding="utf-8-sig")
    print(f"\n数据已保存至: {out_csv}")

    print("\n=== 最近10年GDP数据（亿元） ===")
    cols = [c for c in df_wide.columns if "国内生产总值" in c or "第一产业" in c or "第二产业" in c or "第三产业" in c]
    print(df_wide[cols].head(10).to_string())


if __name__ == "__main__":
    main()

# economic_data_scraper

用于抓取中国宏观经济与城市统计年鉴数据的 Python 脚本仓库。当前项目包含两类脚本：

- 国家统计局 GDP 数据抓取
- 通过复旦图书馆入口访问 CNKI 中国经济社会大数据平台，抓取城市统计年鉴中的农村居民收入数据

这是一个脚本型项目，不是安装型 Python 包。

## Repository Layout

```text
economic_data_scraper/
├── data/
│   ├── city_list.xlsx
│   ├── gdp_data.csv
│   └── examples/
│       └── rural_income_2000.xlsx
├── docs/
├── output/
├── scripts/
│   ├── fetch_gdp_api.py
│   ├── income_scraper.py
│   └── scrape_gdp.py
├── .gitignore
├── README.md
└── requirements.txt
```

## Data Sources

- 国家统计局数据查询平台: `https://data.stats.gov.cn/easyquery.htm?cn=C01`
- 复旦大学图书馆数据库导航: `https://libdbnav.fudan.edu.cn/database/navigation`
- CNKI 中国经济社会大数据研究平台: 通过复旦图书馆入口访问

## Environment

建议使用 Python 3.11+。项目依赖见 `requirements.txt`。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

运行 Selenium 脚本前需要：

- 本机已安装 Google Chrome 或 Chromium
- ChromeDriver 与浏览器版本匹配，且可被 Selenium 正常调用
- 可以访问复旦图书馆和目标数据平台

## Usage

优先推荐接口抓取 GDP：

```bash
python scripts/fetch_gdp_api.py
```

如果需要页面级抓取 GDP：

```bash
python scripts/scrape_gdp.py
```

抓取农村居民收入：

```bash
python scripts/income_scraper.py
python scripts/income_scraper.py --year 2005
python scripts/income_scraper.py --resume
python scripts/income_scraper.py --city 北京市
python scripts/income_scraper.py --headless
```

## Inputs and Outputs

- `data/city_list.xlsx`: 城市或地区输入列表
- `data/gdp_data.csv`: GDP 示例输出
- `output/`: 脚本运行时生成的输出目录

`income_scraper.py` 默认输出到 `output/rural_income_<year>.xlsx`。

## Known Limitations

- `income_scraper.py` 高度依赖复旦图书馆与 CNKI 页面结构；页面改版、认证流程变化或搜索控件变化都可能导致脚本失效。
- `data/examples/rural_income_2000.xlsx` 是一次调试运行的示例输出，目前字段为空，说明链路曾跑通到保存阶段，但数据提取尚不稳定。
- `scrape_gdp.py` 依赖网页 DOM，稳定性低于 `fetch_gdp_api.py`；如无特殊需要，优先使用接口脚本。

## Notes

- 仓库不包含虚拟环境、浏览器调试截图、日志和缓存。
- 如需复现实验，建议单独保留本地 `output/` 目录中的调试产物，不要提交到版本库。

# economic_data_scraper

用于通过复旦图书馆入口访问 CNKI 中国经济社会大数据平台，批量抓取城市/地区统计年鉴中的农村居民收入数据。

这是一个脚本型项目，不是安装型 Python 包。当前仓库已经删除 GDP 示例链路，后续工作全部围绕 CNKI 统计年鉴自动化展开。

## Repository Layout

```text
economic_data_scraper/
├── data/
│   ├── city_list.xlsx
│   └── examples/
│       └── rural_income_2000.xlsx
├── docs/
├── output/
├── scripts/
│   ├── income_scraper.py
├── .gitignore
├── README.md
└── requirements.txt
```

## Data Sources

- 复旦大学图书馆数据库导航: `https://libdbnav.fudan.edu.cn/database/navigation`
- CNKI 中国经济社会大数据研究平台: 通过复旦图书馆入口访问

## Environment

建议使用 Python 3.11+。项目依赖见 `requirements.txt`。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

运行脚本前需要：

- 本机已安装 Google Chrome 或 Chromium
- ChromeDriver 与浏览器版本匹配，且可被 Selenium 正常调用
- 可以访问复旦图书馆和目标数据平台
- 如需尽量复用你本机浏览器中的 CNKI 会话，脚本默认会克隆本机 Chrome `Default` 配置文件中的关键数据；可用环境变量覆盖：
  - `CNKI_USE_LOCAL_CHROME_PROFILE=0` 关闭
  - `CNKI_CHROME_PROFILE_MODE=clone|direct|off`
  - `CNKI_CHROME_USER_DATA_DIR=/path/to/Chrome`
  - `CNKI_CHROME_PROFILE_DIRECTORY=Default`
  - `CNKI_REFRESH_CHROME_PROFILE=1` 强制刷新克隆
  - `CNKI_ENABLE_MANUAL_LOGIN=1` 在非 `--headless` 模式下，若下载被打回 `login.cnki.net`，允许你手动登录后让脚本自动重试当前下载

## Usage

抓取农村居民收入：

```bash
python scripts/income_scraper.py
python scripts/income_scraper.py --year 2005
python scripts/income_scraper.py --resume
python scripts/income_scraper.py --city 北京市
python scripts/income_scraper.py --headless
python scripts/income_scraper.py --retry-incomplete
python scripts/income_scraper.py --retry-statuses NO_YEAR,ERROR

# 推荐用于当前阶段的半自动排障模式：
CNKI_ENABLE_MANUAL_LOGIN=1 python scripts/income_scraper.py --city 廊坊市

# 推荐用于后台批量跑：
python scripts/income_scraper.py --year 2000 --headless --resume
python scripts/income_scraper.py --year 2000 --headless --retry-incomplete

# 只做离线清洗与已有下载文件回灌：
python scripts/income_scraper.py --year 2000 --sanitize-only
```

当前 CLI 输出列固定为：

- `城市`
- `年份`
- `状态`
- `来源标题`
- `来源链接`
- `来源范围`
- `备注`
- `农村人均收入`
- `工资性收入`
- `经营性收入`
- `财产收入`
- `转移收入`

## Inputs and Outputs

- `data/city_list.xlsx`: 城市或地区输入列表
- `output/`: 脚本运行时生成的输出目录

`income_scraper.py` 默认输出到 `output/rural_income_<year>.xlsx`。

## Known Limitations

- 当前版本已经稳定确认的页面链路是：
  `数据库导航 -> 中国经济社会大数据平台 -> 统计资料 -> 统计年鉴 -> 年鉴检索页`
- 当前城市定位策略是：
  `地区分组/省级筛选优先 -> 年鉴候选排序 -> 城市关键字兜底`
- 当前年鉴单页已经接入 CNKI 内部接口辅助检索：
  `GetSearchThisBook -> GetEntryPreview -> PdfAndCajDownload`
  代码会先用接口锁定收入相关条目，再回退到页面内表格解析。
- 当前年鉴单页内的真实“书内检索”控件不是 `button`，而是输入框旁边的 `本册检索` / `本种检索` 文本按钮；脚本已按这个真实控件触发检索。
- 当前锁定到目标条目后，页面结果表最后一列会直接出现 Excel 图标；脚本现已优先点击这个图标，而不是继续假设必须先弹 modal。
- 当前候选排序只接受综合/统计年鉴类标题，显式避开 `人口普查年鉴`、`税务年鉴` 这类专题年鉴，避免错误进入无关页面。
- 当前条目排序会显式压低 `排序`、`各县`、`县(市)` 这类排行表，避免在省级年鉴里误把县级排序表当成城市结果。
- 当前版本会在识别到“仅有省级汇总或非目标地区条目”时直接跳过，避免像北京/邯郸这类无城市级结果的条目继续误抓。
- 当前脚本已经开始区分“可见年鉴搜索框”和“隐藏登录表单输入框”，这是过去大量定位失败的根因。
- 当前版本已经会申请 Excel 下载链接，并尝试两条下载路径：
  - 点击结果表最后一列的 Excel 图标
  - 兜底使用 `PdfAndCajDownload` 返回的下载 URL
  但这两条路径目前都会在 `bar.cnki.net` 上遇到授权问题，常见结果是被重定向到 `https://login.cnki.net/` 或只停留在 `bar.cnki.net` 下载页而不落地文件，因此 `EXCEL_DOWNLOADED` / `EXCEL_PARSED` 还没有稳定达成。
- 当前脚本已支持“手动登录接管”：
  - 在非 `--headless` 模式下设置 `CNKI_ENABLE_MANUAL_LOGIN=1`
  - 一旦下载跳到 `login.cnki.net`，脚本会暂停，等待你在浏览器窗口中完成登录/机构认证
  - 登录完成后回到终端按回车，脚本会自动重试当前 Excel 下载
- 当前已确认 CNKI 下发的文件可能是加密 `.xls`：
  - pandas/xlrd 直接读取会失败
  - 在 macOS 且本机装有 Microsoft Excel 时，脚本会自动调用 Excel 读取工作表内容并回退解析
  - 在 Windows 且服务器安装了 Microsoft Excel 时，脚本会通过 PowerShell + Excel COM 在后台隐藏读取工作表内容并回退解析
  - 该回退解析当前已改为后台隐藏运行，不会再主动弹出 Excel 窗口打断用户操作
  - 若 `output/downloads/` 中已存在对应 `fileCode` 的下载文件，脚本会优先复用，不要求本次运行重新下载
- 当前在已登录或已认证的本机 Chrome 会话下，`--headless --resume` 已可用于后台持续批量跑；浏览器窗口不会弹出，Excel 回退解析也会在后台完成。
- 未来部署到腾讯云 Windows 服务器时，推荐安装桌面版 Chrome、ChromeDriver 与 Microsoft Excel，并始终使用 `--headless --resume` 在后台会话中运行；当前代码已经为 Windows Excel 回退解析预留了后台 COM 路径。
- 当前仍不建议在用户正在使用的桌面会话里直接启动抓取任务；更稳妥的方式是放到独立服务器会话、计划任务或专用后台用户下运行。
- 当前 CLI 已支持：
  - `--sanitize-only`: 只做离线清洗，不启动浏览器
  - `--retry-incomplete`: 只重跑当前年份中未完成的城市
  - `--retry-statuses`: 只重跑指定状态的城市
- 当前版本仍保留“页面内表格兜底解析”，但已经显式跳过“条目题名/页码/下载”这种目录检索结果表，避免把页码误判成收入值。真实稳定数据源仍应以下载后的 Excel 为准。
- 当前 `12-4(二)` 这类单张表只能解析出部分字段；要拿齐 `农村人均收入 / 工资性收入 / 经营性收入 / 财产收入 / 转移收入`，下一步仍需要把同组的 `12-4(一)/(二)/(三)` 合并解析。
- `data/examples/rural_income_2000.xlsx` 是一次旧调试运行的示例输出，目前字段为空，说明旧流程曾跑到保存阶段，但没拿到有效数据。

当前状态枚举：

- `NO_CITY_RESULT`
- `NO_YEAR`
- `NO_INCOME_TABLE`
- `PARTIAL_TABLE_PARSE`
- `EXCEL_DOWNLOADED`
- `EXCEL_PARSED`
- `ERROR`

## Current Direction

当前开发方向分为三段：

1. 稳定进入统计年鉴检索页并按城市定位年鉴
2. 在年鉴页面选择年份，并通过接口锁定农村收入相关条目
3. 打通 Excel 实际落地与解析，生成最终总表

与这三段无关的旧示范代码已经删除。

## Notes

- 仓库不包含虚拟环境、浏览器调试截图、日志和缓存。
- 如需复现实验，建议单独保留本地 `output/` 目录中的调试产物，不要提交到版本库。
- 页面结构调研记录见 [docs/cnki_yearbook_workflow.md](docs/cnki_yearbook_workflow.md)。
- Windows 服务器部署说明见 [docs/windows_server_runbook.md](docs/windows_server_runbook.md)。

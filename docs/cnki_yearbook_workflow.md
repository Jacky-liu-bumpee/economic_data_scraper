# CNKI 统计年鉴工作流笔记

## 已确认的页面链路

1. 复旦数据库导航页：
   - `https://libdbnav.fudan.edu.cn/database/navigation#/home`
2. 搜索“中国经济社会大数据”并点击结果
3. 新窗口进入 CNKI 首页：
   - `https://data.cnki.net/`
4. 顶栏点击 `统计资料`
5. 下拉或二级导航点击 `统计年鉴`
6. 进入年鉴检索页：
   - `https://data.cnki.net/yearBook?type=type&code=A`

## 已观察到的关键 DOM 线索

- 顶栏入口：
  - `//a[normalize-space()='统计资料']`
  - `//a[normalize-space()='统计年鉴']`
- 年鉴检索输入框：
  - 占位符包含 `年鉴关键字`
- 左侧地区筛选：
  - `//div[contains(@class,'yearBook_panel-header')][.//span[normalize-space()='地区']]`
  - 省级条目位于 `//div[contains(@class,'yearBook_panel-region')]//li[...]`
- 年鉴搜索按钮：
  - SVG 类名包含 `yearBook_icons-search`
- 结果卡片：
  - 容器类名包含 `yearBook_list-content`
  - 标题链接位于 `.//h3/a`
- 年份链接：
  - 在年鉴单页中可直接按年份文本点击，例如 `//a[normalize-space()='2000']`
- 年鉴单页书内检索：
  - 输入框类名包含 `single_input-cont`
  - 真正触发检索的不是 `button`，而是旁边两个 `span.single_retrieve...`
  - 文本分别为 `本册检索` 和 `本种检索`
- 检索结果表：
  - 行类名包含 `single_s-tab-tbody`
  - 最后一列直接包含 Excel SVG 图标，类名包含 `single_excel`

## 当前主要问题

1. 页面 DOM 中同时存在隐藏登录表单输入框和真实业务输入框
2. 旧脚本把 CNKI 首页和年鉴检索页混成一步，导致后续状态全错
3. 旧脚本尝试直接扫描页面表格，而真实稳定数据源应当是下载后的 Excel

## 当前实现策略

短期：
- 先稳定做到 `城市 -> 年鉴 -> 年份`
- 年鉴单页优先走 CNKI 内部接口：
  - `POST /api/csyd/StatisticalData/GetSearchThisBook`
  - `POST /api/csyd/StatisticalData/GetEntryPreview`
  - `POST /api/csyd/StatisticalData/PdfAndCajDownload`
- 保留页面表格兜底解析，方便快速验证页面是否正确进入
- 页面下载优先级已调整为：
  - 先在书内检索结果表中找到目标条目
  - 优先点击该条目所在行末列的 Excel 图标
  - 若页面下载失败，再退回 `PdfAndCajDownload` 返回的 URL
- 当前脚本还支持手动登录接管：
  - 设置 `CNKI_ENABLE_MANUAL_LOGIN=1`
  - 且必须使用非 `--headless` 模式
  - 当页面下载被重定向到 `login.cnki.net` 时，脚本会暂停并等待用户在浏览器窗口中完成登录，再自动重试当前下载
- 城市定位先用 `地区分组 + 省级筛选` 缩小候选，再按标题/摘要挑选目标年鉴
- 候选排序只接受 `统计年鉴` 类标题，避免误选 `人口普查年鉴`、`科技年鉴` 这类专题年鉴
- 条目排序优先：
  - `农村住户人均总收入总支出和纯收入`
  - `农村居民人均收支情况`
  - `农村居民人均纯收入`
- 条目排序显式压低：
  - `排序`
  - `各县`
  - `县(市)`
  - `各市县`
  这样可以避免在省级年鉴中误选县级排行表
- 如果条目预览显示它只是省级汇总、且范围中不包含目标城市，则直接按“无相关城市数据”跳过，不再继续追下载链路

中期：
- 打通浏览器侧真实下载，让页面 Excel 图标或 `PdfAndCajDownload` 返回的链接能够稳定落地为本地 Excel
- 用 `pandas/openpyxl` 解析本地文件并产出统一总表

## 当前接口探测结果

- `GetSearchThisBook` 已确认可返回条目列表、`fileCode` 和 `czexcel`
- `GetEntryPreview` 已确认可返回：
  - 条目标题
  - 目录路径 `wzlm`
  - 适用范围 `shdy`
  - 指标列表 `bhzb`
  - 是否可导出 Excel `czexcel`
- `PdfAndCajDownload` 已确认可返回下载 URL
- 当前页面结果表已确认存在直接 Excel 图标下载入口，但在自动化会话下点击后通常会新开 `login.cnki.net` 页面，说明 `bar.cnki.net` 仍有额外授权校验
- `PdfAndCajDownload` 返回的链接也会遇到同类问题：有时被重定向到 `login.cnki.net`，有时停留在 `bar.cnki.net/download/order...` 而不落地文件
- 当前兜底表格解析已显式跳过“条目题名 / 年鉴年份 / 页码 / 下载”这种目录检索结果表，避免把页码误判成收入值
- 当前已确认下载文件可能是加密 `.xls`；在 macOS + Microsoft Excel 环境下，可以通过 Excel 对象模型读取 `used range`，再回退给 Python 解析
- 在 Windows 服务器上，当前代码会改走 PowerShell + Excel COM 的隐藏后台读取路径，避免把 Excel 窗口直接抛给前台用户
- 当前 Excel 回退解析已切为后台隐藏模式，不再主动把 Excel 窗口切到前台
- 当前脚本已支持复用 `output/downloads/` 中已有的 `fileCode` 对应下载文件，因此手动下载一次后可直接重跑脚本完成解析
- 当前 `12-4 农村住户人均总收入总支出和纯收入(二)` 可稳定提取：
  - `转移收入`
  - `财产收入`
  但完整字段仍需配套下载并合并 `12-4(一)` 与 `12-4(三)`
- 当前在本机已登录 Chrome 会话下，可以直接用 `--headless --resume` 做后台批量跑；后续迁移到腾讯云 Windows 服务器时，优先沿用这条后台运行模式，并使用 Windows Excel COM 完成加密 `.xls` 回退解析

## 当前状态定义

- `NO_CITY_RESULT`: 地区筛选和关键字兜底后，仍未找到可接受的统计年鉴候选
- `NO_YEAR`: 已进入目标年鉴，但未找到目标年份入口
- `NO_INCOME_TABLE`: 已进入目标年份页面，但页面内未直接解析出收入字段
- `PARTIAL_TABLE_PARSE`: 页面表格中解析出了部分收入字段
- `EXCEL_DOWNLOADED`: 已发现并触发下载入口，后续待补本地解析
- `EXCEL_PARSED`: 已完成 Excel 下载和结构化解析
- `ERROR`: Selenium 导航或解析过程中出现未兜底异常

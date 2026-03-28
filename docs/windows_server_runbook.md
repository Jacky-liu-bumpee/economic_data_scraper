# Windows Server Runbook

适用于后续把项目部署到腾讯云 Windows 服务器，并在后台持续跑 CNKI 年鉴抓取。

## 建议环境

- Windows Server 2019/2022
- Python 3.11+
- Google Chrome
- 与 Chrome 对应版本的 ChromeDriver
- Microsoft Excel
- 一个专门用于跑抓取任务的 Windows 用户

## 首次准备

1. 把仓库放到服务器，例如：
   - `D:\economic_data_scraper`
2. 安装依赖：

```powershell
cd D:\economic_data_scraper
py -3 -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

3. 用该服务器用户手动打开一次 Chrome，确认：
   - 能访问复旦图书馆入口
   - 已具备 CNKI 可用登录态/机构访问态

## 后台运行方式

### 方式一：先做纯离线清洗

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_batch_windows.ps1 -Year 2000 -SanitizeOnly
```

用途：
- 清理旧结果中的误提取字段
- 用 `output/downloads/` 中已有文件回灌更完整的解析结果

### 方式二：只重跑未完成城市

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_batch_windows.ps1 -Year 2000 -RetryIncomplete
```

这会调用：
- `scripts/income_scraper.py --year 2000 --headless --retry-incomplete`

适合：
- 已有部分结果
- 只想继续补 `NO_YEAR / NO_INCOME_TABLE / ERROR / PARTIAL_TABLE_PARSE` 之类未完成城市

### 方式三：只重跑指定状态

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_batch_windows.ps1 -Year 2000 -RetryStatuses "NO_YEAR,ERROR"
```

适合：
- 只想重试某一类失败

## 结果文件

- 主结果：
  - `output\rural_income_2000.xlsx`
- Windows 批跑日志：
  - `output\windows_batch_2000.log`
- 下载缓存：
  - `output\downloads\`

## 计划任务建议

建议用 Windows Task Scheduler 以专用后台用户运行：

- Program/script:
  - `powershell.exe`
- Add arguments:
  - `-ExecutionPolicy Bypass -File D:\economic_data_scraper\scripts\run_batch_windows.ps1 -Year 2000 -RetryIncomplete`
- Start in:
  - `D:\economic_data_scraper`

建议勾选：
- `Run whether user is logged on or not`
- `Run with highest privileges`

## 当前已知限制

- 当前代码虽然已补了 Windows Excel COM 的后台解析路径，但还没有在真实 Windows 服务器上实机验证。
- 服务器必须安装 Microsoft Excel，否则加密 `.xls` 仍无法回退解析。
- 如果 CNKI 登录态失效，后台抓取仍会失败，只是不会把浏览器窗口抛到当前用户前台。

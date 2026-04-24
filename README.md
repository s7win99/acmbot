# ACM Bot

ACM Bot 是一个基于 NcatBot 的 QQ 机器人项目，目标是服务 ACM/ICPC 训练群。

当前版本实现第三阶段：在基础命令之外，支持 Codeforces 用户基础信息、Codeforces + AtCoder 比赛查询、群比赛提醒和训练队内部 qrating 双分系统。

## 环境要求

- Python 3.11
- Conda 环境：`acmbot`
- 已运行并登录的 NapCat OneBot11 WebSocket 服务

## 安装依赖

先进入项目目录：

```bash
cd c:\Code\object\acmbot
```

激活 conda 环境：

```bash
conda activate acmbot
```

安装依赖：

```bash
pip install -r requirements.txt
```

说明：当前 PyPI 上可安装的 NcatBot 包名是 `ncatbot`，本项目使用 `ncatbot==4.4.1.post1`。

## 配置

启动前请编辑 `config.yaml`：

- 将 `bt_uin` 改为机器人 QQ 号。
- 将 `root` 改为管理员 QQ 号。
- 将 `plugin.admins` 改为可以管理 qrating 和比赛提醒的 QQ 号列表。
- 确认 `napcat.ws_uri` 和 `napcat.ws_token` 与 NapCat 的 WebSocket 服务端配置一致。

推荐先独立启动 NapCat，并在 NapCat WebUI 中开启 WebSocket 服务端，再让本项目使用远程模式连接：

```yaml
napcat:
  ws_uri: ws://127.0.0.1:3001
  ws_token: CHANGE_ME
  enable_webui: false
  remote_mode: true
```

qrating 管理员示例：

```yaml
plugin:
  admins:
  - "123456789"
  - "987654321"
```

## 启动方式

```bash
python main.py
```

如果没有激活 conda 环境，也可以直接使用环境里的 Python：

```bash
C:\software\Miniconda\envs\acmbot\python.exe main.py
```

## 当前支持命令

群聊和私聊均支持：

```text
/ping
/help
/about
/cf 用户名
/contest
/contest remind on
/contest remind off
/contest remind status
/qrating
/qrating rank
/qrating import 比赛名称
/admin log
```

命令说明：

```text
/ping          测试机器人是否在线
/help          查看帮助菜单
/about         查看机器人项目信息
/cf 用户名      查询 Codeforces 用户基础信息
/contest       查询近期比赛
/contest remind on/off/status  管理员配置当前群比赛提醒
/qrating       查询自己的 qrating
/qrating rank  查看 qrating 排行榜
/qrating import 比赛名称  从 VJudge xlsx 榜单生成 update 预览命令，仅管理员可用
/admin log     查看最近 10 条管理员操作日志，仅管理员可用
```

示例：

```text
/cf tourist
/contest
```

未知命令不会回复，避免机器人刷屏。

## 比赛查询

```text
/contest
```

展示 Codeforces + AtCoder 合并后的最近 5 场即将开始的比赛。

规则：

1. 只展示未来比赛。
2. 按开始时间从近到远排序。
3. CF 和 AtCoder 比赛合并后统一排序，不是各取 5 场再拼接。
4. 时间统一按北京时间 `Asia/Shanghai` 显示。
5. 如果一个平台抓取失败、另一个平台成功，仍然返回成功平台的结果。
6. 如果两个平台都失败，返回“比赛查询失败，请稍后重试。”。

目前支持的平台：

- Codeforces：官方 API `https://codeforces.com/api/contest.list`
- AtCoder：官方比赛页 `https://atcoder.jp/contests/`

## 比赛提醒

管理员命令：

```text
/contest remind on
/contest remind off
/contest remind status
```

作用：

- `/contest remind on`：开启当前群比赛提醒。
- `/contest remind off`：关闭当前群比赛提醒。
- `/contest remind status`：查看当前群比赛提醒状态。

提醒内容：

1. 每天 09:00 发送最近 5 场比赛速览。
2. 比赛开始前 60 分钟提醒。
3. 比赛开始前 5 分钟提醒。

说明：

- 目前支持 Codeforces 和 AtCoder。
- AtCoder 比赛数据来自 AtCoder 官方 contests 页面。
- 同一群同一比赛同一提醒类型只会提醒一次。
- 每日速览同一群同一天只会发送一次。
- 提醒功能仅支持在群聊中配置。

## qrating 系统

qrating 是训练队内部积分系统，当前采用双分系统：

- `internal_qrating`：内部计算分，只用于 Pairwise Elo 算法和管理员维护，不对普通用户展示。
- `display_qrating`：对外显示分，用于个人查询、排行榜、赛后变化榜。
- `rated_contest_count`：已参加正式 qrating 比赛场次，只会在 `/qrating update` 成功后增加。

新用户初始值：

- `internal_qrating = 1200`
- `display_qrating = 1000`
- `rated_contest_count = 0`

显示分使用 display offset v1 规则：

| 已参加场次 | offset |
| --- | --- |
| 0 场 | 200 |
| 1 场 | 120 |
| 2 场 | 70 |
| 3 场 | 40 |
| 4 场 | 15 |
| 5 场及以上 | 0 |

计算公式：

```text
display_qrating = max(0, internal_qrating - offset)
```

qrating 数据保存在 `data/acm_bot.db`。如果 `data/` 目录或数据库文件不存在，机器人会在插件加载时自动创建。

### 普通用户命令

```text
/qrating
```

查询自己的 qrating。普通用户只会看到显示 qrating、已参加场次和最近一次显示分变化，不显示 `internal_qrating`。

```text
/qrating rank
```

查看当前 qrating 排行榜，最多显示前 50 名。排行榜按 `display_qrating` 降序排序，分数相同按创建时间升序；该命令只显示排名、昵称和当前显示 qrating，不显示变化量。

### 管理员命令

```text
/qrating add QQ号 昵称
```

或批量添加：

```text
/qrating add
QQ号 昵称
QQ号 昵称
```

添加 qrating 用户。新用户会写入 `internal_qrating = 1200`、`display_qrating = 1000`、`rated_contest_count = 0`。批量添加时每一行填写一个 QQ 号和昵称，昵称可以包含空格。

```text
/qrating update 比赛名称
名次 昵称或QQ号
名次 昵称或QQ号
```

根据 ACM 比赛排名自动计算 qrating 变化。算法为 Pairwise Elo，K 值为 80，单场 internal 变化限制为 ±80。算法只使用 `internal_qrating` 计算；结算成功后参赛者 `rated_contest_count + 1`，再按 display offset v1 自动重算 `display_qrating`。公开回复、排行榜和赛后变化榜都按 `display_qrating` 展示。支持并列名次，例如 `1、2、2、4`。

```text
/qrating adjust 比赛名称
昵称或QQ号 +25
昵称或QQ号 -10
```

手动调整 qrating，用于特殊修正。`/qrating adjust` 只修改 `internal_qrating`，不会增加 `rated_contest_count`，也不允许直接手动修改 `display_qrating`；调整后会按当前场次自动重算显示分。手动修改请使用 `/qrating adjust`，不要再使用 `/qrating update` 输入 `+25` 或 `-10`。

```text
/qrating import 比赛名称
```

从 VJudge 导出的 xlsx 榜单生成 `/qrating update` 预览命令。第一版不会直接更新 qrating，管理员确认后需要手动发送生成的 update 命令。

```text
/qrating rank diff
```

查看最近一次未回滚 qrating 事件后的变化榜，仅管理员可用，适合赛后统一发布。该命令显示排名、昵称、当前显示 qrating 和最近一次事件中的 `display_delta`；未出现在最近一次事件中的用户显示 `(-)`。变化榜排序仍按当前 `display_qrating` 降序。

```text
/qrating rollback
```

回滚最近一次 qrating 修改，可以回滚按排名自动计算的 update，也可以回滚手动 adjust。回滚会按 `qrating_changes` 中记录的旧值恢复 `internal_qrating`、`display_qrating` 和 `rated_contest_count`。

管理员 QQ 号在 `config.yaml` 的 admins 配置中维护；当前项目使用 `plugin.admins`，代码也兼容顶层 `admins`。该权限不依赖 QQ 群管理员身份。

## VJudge 榜单导入（预览模式）

管理员先发送一个 VJudge 导出的 `.xlsx` 文件，然后回复该文件消息发送：

```text
/qrating import 比赛名称
```

例如：

```text
/qrating import 周赛第5场
```

机器人会解析 xlsx 中的 Rank 和 Team，并返回一段合法的 `/qrating update` 完整命令文本。

注意：

1. 第一版不会直接更新 qrating。
2. 第一版只返回 update 命令，管理员确认后再手动发送执行。
3. Team 的昵称提取规则优先取括号中的姓名，例如：`test(测试) -> 测试`。
4. 候选昵称必须能唯一精确匹配到 qrating 用户，否则导入失败。
5. 第一版只读取第一个 sheet，只识别 Rank 和 Team 列。

## 管理员日志

```text
/admin log
```

查看最近 10 条管理员操作日志，仅管理员可用。

当前记录的操作包括：

1. 添加 qrating 用户。
2. 按比赛排名更新 qrating。
3. 手动调整 qrating。
4. 回滚 qrating 修改。
5. 查看 qrating 变化榜。

管理员日志保存在 SQLite 数据库 `data/acm_bot.db` 的 `admin_logs` 表中。

## 当前阶段说明

当前版本仍然以文字版查询为主，不包含图片卡片、用户绑定、定时提醒或自动导入执行。后续再扩展 OJ 查询、比赛提醒、账号绑定、训练统计等功能。

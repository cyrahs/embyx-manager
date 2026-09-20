# 榜单播放列表:把 jinjier.art 的榜单同步成 embyx 的 Emby 播放列表

日期:2026-09-20。同日确认的决定见「结论」。

## 实施进度

| 步骤 | 状态 |
| --- | --- |
| 1 Emby 客户端 + `emby` 配置节 + 设置页卡片与连接测试 | 已实现(分支上,**Postgres 测试仅 CI 验证**) |
| 2 jinjier 数据源客户端 + 榜单解析与入选规则 | 已实现(分支上):`clients/jinjier.py`,真实库跑出 70 张榜/4041 番号,与调研一致 |
| 3 迁移 v15 `playlists` 表 + 仓储 + `playlists` 流水线 + 调度接入 + API | 已实现(分支上,**Postgres 测试仅 CI 验证**):看板已有卡片与运行筛选,设置页已有「榜单播放列表」卡片;补全目录的解析结果要到第 5 步才进 API |
| 4 前端「列表」页 + 看板流水线卡片 | 已实现(分支上):`/playlists`,三组展示,展开缺失清单带账本状态,启停,立即同步;补全按钮在第 5 步 |
| 5 补全:通用补全流程的来源参数化 + 补全接口与按钮 | 已实现(分支上):`ManualIntakeSource.submit(source=, limit=)`,`POST /api/playlists/{key}/fill` 单榜一次全投,页面上两步确认(补全 → 确认补全 N 部) |

## 结论

- **做成播放列表而不是合集**:用户要保留名次顺序。已在 embyx(Emby 4.10.0.40)上验证
  `POST /Playlists?Name=&Ids=&MediaType=Video` 按 Ids 顺序建立;API key 建的列表对唯一用户
  x 直接可见,不需要 UserId 或共享设置;列表落在 `/config/data/playlists/<名>/<名>.m3u`。
- **数据源是 jinjier.art 的 SQLite,不是 javranking.cc**:javranking 只是它的展示层。
  `https://jinjier.art/<yyyymmdd>.gif` 实为 zip,内含 `jinjier.sqlite3`,单表
  `ranks(id, kind, number, name, date, note, icon_url)`。当前文件名写在 `https://jinjier.art/sql`
  页内联脚本的 `dbFile="20260112.gif"`。一对 `(kind, note)` 是一张榜,对应一个播放列表。
- **入选的榜**:JavDB 有码 TOP250(kind 7)、JavLibrary TOP500(kind 5)、JavDB 2008–2025
  年度榜(kind 等于年份)、金鸡儿奖各奖项(kind 4,每个奖项一个列表,人名行解析不出番号即跳过)。
  **不要**:JavDB 总榜(kind 6,剔除无码后与有码榜完全重合)、FANZA 通贩月榜与半年/全年榜
  (kind 2/3)、无码榜(8)、欧美榜(9)、FC2 榜(10)、女优榜(0/1)。
- **embyx 只收正规有码 JAV**:任何榜里的无码条目一并剔除——`name` 里带 ` 無碼 ` 标记、番号在
  无码榜(kind 8)里、`FC2-` 前缀、`010115_001` 这类日期番号、以及一组无码厂牌前缀(HEYZO、
  CARIB、1PONDO、MKBD、SMBD、LAFBD、CWPBD、LAF、SKYHD、SKY 等)。规则是代码常量,不做成配置。
- **新增「列表」标签页**:列出由 embyx-manager 管理的列表,每张可启用/停用,显示缺口
  (缺失番号清单),带「补全」按钮。补全默认投到 Rank 分类的离线目录,走通用补全流程
  (与手动添加、fill-actor 同一条 `AcquisitionIntake`)。
- **补全不自动触发**:同步只建播放列表,缺失只统计不下载;下载由操作员按榜点按钮。
  截至 2026-09-20 的缺口:70 张榜去重 4041 个番号,库里缺 1549 个(38%);缺得少的是有码
  TOP250(30)与 2018/2019/2024 年度榜(36–44),2008 年度榜缺 80%。

## 已核实的事实

### Emby(embyx 实例)

- 集群内地址 `http://embyx.media.svc.cluster.local:80`,与 embyx-manager 同 namespace;
  入口是 strm-proxy 边车,普通 API 原样透传到 Emby。
- 认证:`X-Emby-Token: <api key>` 头。集群里原本没有 Emby key;用户 2026-09-20 生成了一把,
  由设置页保存到数据库(`SECRET_FIELDS`,不回显),不进 SOPS。
- 全库索引:`GET /Items?Recursive=true&IncludeItemTypes=Movie&Fields=Path&Limit=50000`
  一次返回全部 31724 部,约 8.5 秒、17 MB。`Path` 形如
  `/media/local/rank/HMN/HMN-911/HMN-911.strm`;mapping 流水线保证父目录名就是番号,
  但库内补零不统一(`LAFBD-006` 与 `LAF-06`、`CWPBD-46` 并存),所以**父目录名要过一遍
  AvidParser 再当键**,榜单侧同样归一化。94% 的条目文件名主干等于目录名,其余是分集或带后缀,
  因此只按父目录名匹配,不看文件名。
- 单番号查找 `GET /Items?...&Path=<完整路径>` 可用,但同步用全库索引更省。
- 播放列表:`POST /Playlists?Name=&Ids=a,b,c&MediaType=Video` → `{Id, Name, ItemAddedCount}`;
  `GET /Playlists/{Id}/Items?Fields=Path` 按顺序返回,每项带 `PlaylistItemId`;
  `POST /Playlists/{Id}/Items?Ids=` 追加;`DELETE /Playlists/{Id}/Items?EntryIds=` 移除;
  `GET /Items?Recursive=true&IncludeItemTypes=Playlist` 枚举现有列表。
  `GET /Items/{Id}` 不带用户上下文返回 404,要用 `/Users/{uid}/Items/{Id}`——同步逻辑不需要它。

### jinjier.art 数据库

- 各 kind 的含义:0/1 女优榜;2 FANZA 通贩影片月榜(每月 100,2021-07 至 2025-12);
  3 FANZA 半年/全年榜;4 金鸡儿奖各奖项(四届,90 个 note,含人名行);5 JavLibrary TOP500;
  6 JavDB 总榜;7 有码 TOP250;8 无码;9 欧美;10 FC2;2008–2025 年度 TOP250。
- `name` 以番号开头,后接标题;`number` 是名次;FANZA 榜有蓝光重复行(同番号两行),入选的榜里
  没有,但解析仍按「同榜内番号去重、保留首次名次」处理。
- 库大约每月更新一次,文件名随更新变化(`20260112`)。
- 用仓库 AvidParser 解析入选各榜:除欧美式条目外全部成功;`LAFBD-41` 会归一为 `LAFBD-041`,
  `SERO-0127` 归一为 `SERO-127`。
- javranking.cc 有 Markdown 镜像(`/en/rankings/javdb-top250.md` 等,`/llms-full.txt` 列全),
  可作备用来源,但只覆盖 8 张榜,本方案不实现。

### 仓库里可复用的部分

- `AcquisitionIntake.enqueue(avid, source=, task_dir_path=, ctx=)` 是所有来源共用的入口;
  `ManualIntakeSource.submit` 在它前面加了「目录有路由、目录存在、库里已有则不入账」三道检查,
  补全应复用这三道检查而不是重写。`AcquisitionSource` 是枚举,但 ledger 的 `source` 列是自由
  文本(`rss:<分类>` 已如此),补全来源写 `playlist:<key>`。
- `MonitorScheduler` 按 `PipelineName` 分派,`_ready`、`_locks`、看板卡片与运行历史都按枚举
  展开;加第四个流水线要同时改 `PipelineName`、调度器分派、`bootstrap` 装配、看板的
  `PIPELINE_LABELS/DESCRIPTIONS` 与筛选按钮。
- 配置节模型在 `config/models.py` 的 `SECTION_MODELS` 登记即可出现在设置页 API;设置页卡片
  在 `SettingsPage.tsx` 声明,`kind: 'secret'` 字段不回显;CloudDrive 的 `testTarget` 是连接
  测试按钮的现成模式。
- 迁移写在 `db.py` 的 `_MIGRATIONS[n]`,`CURRENT_SCHEMA_VERSION` 同步加一;Postgres 类测试只在
  CI 跑,本地不起数据库。

## 方案

### A. 配置

两个新配置节,都在设置页有卡片:

- `emby`:`address`(HTTP base URL,集群内填 `http://embyx.media.svc.cluster.local`)、
  `api_key`(secret)。卡片带「测试连接」,用未保存的表单值(密钥留空则用已存的)调
  `GET /System/Info` 并回显 ServerName 与版本。
- `playlists`:`enabled`(定时同步开关,默认关)、`interval_seconds`(默认 86400)、
  `source_url`(默认 `https://jinjier.art/sql`,只在站点改结构时改)、`task_dir_path`
  (补全投放目录;**空表示用标签为 `Rank` 的 RSS 分类目录**,设置页 hint 说明;两者都拿不到
  有路由的目录时补全按钮禁用并说明原因)。

### B. 数据源:`clients/jinjier.py`

- `discover_database_name(source_url)`:抓 `/sql` 页,正则取 `dbFile="(\d{8})\.gif"`。
- `download_database(base, name)`:取 `/<name>.gif`,校验 zip 头,解出 `jinjier.sqlite3`
  的字节。1.1 MB,直接内存处理。
- `parse_lists(sqlite_bytes, parser: AvidParser) -> tuple[RankedList, ...]`:
  `RankedList(key, kind, note, name, entries: tuple[RankedEntry, ...])`,
  `RankedEntry(rank, avid, title)`。`key` 形如 `k7`、`k2024`、`k4:<note>`;只产出入选 kind,
  按结论里的无码规则剔除,同榜内番号去重保留首次名次,`name` 沿用 `note`。
- 入选规则与剔除规则是模块常量;测试不用二进制夹具,`tests/test_jinjier.py` 用 sqlite3 在内存里
  建一张含每种 kind 的小表(蓝光重复、無碼标记、人名行、FC2、日期番号、无码集合命中)再 `serialize()`。

### C. Emby 客户端:`clients/emby.py`

httpx 异步客户端,方法只覆盖同步需要的:`system_info()`、`movie_index()`(返回
`dict[avid, item_id]`,父目录名过 AvidParser;重复番号取第一个)、`list_playlists()`、
`create_playlist(name, item_ids)`、`playlist_entries(id)`、`add_entries(id, item_ids)`、
`remove_entries(id, entry_ids)`、`delete_playlist(id)`(`POST /Items/Delete?Ids=`,已在 embyx 上验证返回 204;
`DELETE /Items/{Id}` 未验证,不用)。索引按 `StartIndex/Limit` 分页拉取,服务器按 `TotalRecordCount` 收尾。
错误统一成 `EmbyError`,401 单独成 `EmbyAuthError`,让设置页测试与流水线都能说清楚原因。

### D. 存储:迁移 v15

```
CREATE TABLE playlists (
    key TEXT PRIMARY KEY,               -- k7 / k2024 / k4:<note>
    kind INTEGER NOT NULL,
    note TEXT NOT NULL,
    name TEXT NOT NULL,                 -- Emby 里的列表名,沿用 note
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    entries_json TEXT NOT NULL,         -- [[rank, avid, title], ...] 按名次
    present_json TEXT NOT NULL DEFAULT '[]',  -- 上次同步时库里已有的 avid,按名次
    missing_json TEXT NOT NULL DEFAULT '[]',  -- 上次同步时缺的 avid,按名次
    emby_playlist_id TEXT,              -- 停用或尚未同步时为 NULL
    last_synced_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE playlist_source (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),   -- 单行
    database_name TEXT NOT NULL,        -- 20260112
    fetched_at TIMESTAMPTZ NOT NULL
);
```

新出现的榜(下一年的年度榜、下一届奖)默认启用;从数据源消失的榜保留行但标 `last_error`,
不自动删 Emby 列表——榜单站改名不该让用户的列表消失。`enabled` 是用户的选择,重新解析不覆盖。

### E. 流水线:`monitor/playlists.py`,`PipelineName.PLAYLISTS`

一次运行:

1. 发现数据库文件名;与 `playlist_source` 不同或表为空时下载并解析,upsert `playlists` 的
   `entries_json`(不动 `enabled`);相同则跳过下载,只用表里的条目。下载失败记警告,
   继续用表里的条目同步——数据源挂了不该让 Emby 侧停更。
2. 拉 Emby 全库索引一次。
3. 对每张启用的榜:按名次把 avid 映射成 item id,得到 `present`(有序 item id)与 `missing`;
   没有 `emby_playlist_id`、或该 id 在 `list_playlists()` 里已不存在(用户手删了)→ 新建
   (**库里一部都没有的榜不建空列表**,缺失数已说明一切;有了第一部再建);
   否则读现有条目,与目标序列逐位比较,不同就**清空再按序重加**(一次 250 条,不逐条 move);
   相同则跳过。写回 `present_json/missing_json/last_synced_at`,失败写 `last_error` 并继续下一张。
4. 对每张停用且仍有 `emby_playlist_id` 的榜:删掉 Emby 列表,清空 id。停用的语义是
   「Emby 里不出现」,重新启用会重建。
5. 统计:`lists_synced`、`lists_created`、`lists_rebuilt`、`lists_removed`、`missing_total`。

就绪条件:`emby.address` 与 `api_key` 都已配置。调度:`_playlists_loop` 按
`interval_seconds`,与 rss loop 同构;手动触发走现有 `/api/monitor/{pipeline}/trigger`。

### F. API:`/api/playlists`

- `GET /api/playlists`:全部列表(含停用),每项 `key, kind, note, name, enabled, total,
  present, missing, emby_playlist_id, last_synced_at, last_error`,外加 `source`
  (`database_name, fetched_at`)与补全目录解析结果(`fill_task_dir, fill_reason`)。
- `GET /api/playlists/{key}/missing`:缺失番号按名次,每条带标题,以及账本状态
  (`tracked: state|null`)——让用户看到哪些已经在下、哪些还没提交。
- `PATCH /api/playlists/{key}` `{enabled}`(需认证):只改标志;Emby 侧由下次同步生效,
  响应里带 `next_scheduled_at` 提示;页面提供「立即同步」按钮(触发流水线)。
- `POST /api/playlists/{key}/fill`(需认证):把 `missing_json` 交给通用补全,来源
  `playlist:<key>`,目录取 A 节的解析结果;返回与手动添加相同的逐条结果
  (submitted / already_tracked / already_in_library / no_magnet / submit_failed)。

### G. 补全:参数化手动来源

`ManualIntakeSource.submit(inputs, *, task_dir_path, source=AcquisitionSource.MANUAL)`
加 `source` 参数,`_submit_one` 透传;`MAX_MANUAL_INPUTS`(100)只约束 UI 的手动粘贴,
补全接口自己分批调用(每批 100)。`AcquisitionSource` 枚举不加成员——来源字符串
`playlist:<key>` 与 `rss:<分类>` 同一做法,加 `playlist_source(key)` 帮助函数。
补全结果写进流水线运行历史之外的一次 `RunContext` 日志即可,不新建运行记录。

### H. 前端

- 导航加「列表」(`/playlists`),`NAV_ITEMS` 与路由同步。
- 页面按三组展示:总榜(有码 TOP250、JavLibrary TOP500)、年度榜(按年倒序)、金鸡儿奖
  (按届分小节)。每行:启用开关、名称、条目数、已有、缺失(可展开缺失清单,带标题与账本状态)、
  「补全」按钮(缺失为 0、未登录、或补全目录不可用时禁用并说明)。
- 页头:数据源文件日期、上次同步时间、「立即同步」;流水线未就绪时显示原因并链接设置页。
- 看板加 `playlists` 流水线卡片与运行筛选。
- 复用 `SubscriptionsPage` 的 panel/表格样式与 `Notice`、`Spinner`。

### I. 测试

- `test_jinjier.py`:样本库解析、入选/剔除规则、去重保序、`dbFile` 发现。
- `test_emby.py`:httpx `MockTransport`,索引归一化、创建/重建/删除请求形状、401。
- `test_monitor_playlists.py`:FakeEmby + FakeRepository,覆盖新建、顺序不变跳过、顺序变化
  重建、用户手删后重建、停用删除、数据源失败沿用旧条目、单榜失败不影响其他榜。
- `test_playlists_api.py`:补全走 ManualIntakeSource 的三道检查,来源字符串正确,分批。
- Postgres 仓储测试只在 CI 跑。

### J. 风险与边界

- jinjier.art 是个人站,文件名规则与 `/sql` 页结构没有承诺;发现失败时流水线沿用上次条目,
  面板显示数据源错误。备用来源(javranking Markdown)留作以后。
- Emby 列表名重名:用户自己建了同名列表时,以 `emby_playlist_id` 为准,不按名字找;首次同步
  前若已有同名列表则新建一个(Emby 允许重名),由用户自行删旧的。
- 补全一次最多投几百个番号进队列,与 Rank 分类首次调度时的情形相同;这是操作员点按钮的
  显式动作,不做自动。
- 索引 17 MB 每次同步拉一次,每天一次可接受;不做增量。

### K. 实施顺序

按「实施进度」表的 1→5,每步独立可合并:1 只加配置与客户端,不改行为;2 是纯函数;
3 之后流水线可跑但没有页面(看板能触发、看运行统计);4 加页面;5 加补全。

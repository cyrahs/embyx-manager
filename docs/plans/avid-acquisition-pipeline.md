# 归档采集追踪重构:从"扫文件夹"到"以 avid 为中心的下载账本"

日期:2026-08-13(实施进度更新 2026-08-14)。

## 实施进度

| 步骤 | 状态 | 提交 |
| --- | --- | --- |
| Step 0 许可证合规 | 已完成 | `43d2b05` |
| Step 1 账本 schema 与仓储层 | 已完成(**Postgres 测试仅 CI 验证**) | `7c8a363` |
| Step 2 CloudDrive 离线任务全量可见 | 已完成 | `b6cda88` |
| Step 3 解析器单点化 + 两套语料 | 已完成 | `f8c73d2` |
| Step 4 RSS 改为纯发现入口 | 已完成 | `7db02b0` |
| Step 5 定向归档器替换四阶段 | 已完成 | `2c1a837` |
| Step 6 Tracker 服务循环 | 已完成 | `940f4eb` |
| Step 7 全量扫描降级为 reconcile | 已完成 | `e2dd96c` |
| Step 8 API 与前端 | 已完成 | `7e5bb84` `576a545` |

实施中相对本计划的偏差,均已在对应提交信息中说明:

- 迁移号:账本是 v5,另加 v6 剥离 `cloud_name`/`cloud_account_id`(它们只服务于已删除的
  清理调用,前端提示文案原文即"用于清理已完成离线任务记录")。
- attempts 增加 `archiving` 态与 info_hash 的部分唯一索引,tracker 才能安全按 hash JOIN。
- `progress` 用 DOUBLE PRECISION:CloudDrive 的 `percendDone` 是 double,float4 会因精度差
  让"进度未变化"永远判为已变化,停滞检测直接失效。
- tracker 增加 `submit_grace_seconds`(默认 300):本轮刚提交的磁力不可能出现在本轮开始时
  拍下的任务列表快照里,否则会被立刻误判为 lost。
- reconcile 对"一个文件夹含两个番号"只能在进程内去重告警——账本以 avid 为主键,这类文件夹
  没有可用的主键可停放。
- 仓储层补 `claim_attempt`:手动提交的磁力要提交它本身,而不是"下一条待用的"。

部署后修订(2026-08-14):

- **撤销"tracker 目的地独立配置"**:`task_dir_local`/`task_dst`/`task_priority` 与
  `clouddrive.task_dir_path` 及路由表重复配置同一目录。现阶段离线目录只配置一处
  (`clouddrive.task_dir_path`,RSS 全部离线到此),tracker 在归档路由表中定位完成的
  下载,落在哪条路由就按该路由的目标与优先级归档(迁移 v7 剥离旧键)。计划正文
  §Step 6 的相关描述以此为准。
- 兜底扫描不再搭 RSS 间隔的车:新增 `archive.scan_cron`(cronsim 校验,服务器本地
  时间),调度器为归档单独开 cron 循环;设置页在 cron 输入下实时显示中文自然语言描述
  (cronstrue zh_CN)。

## 原计划(v3)

## 背景与目标

现状是两条互不知情的管线:RSS 管线把 avid 解析成 magnet 丢给 CloudDrive 离线下载后即忘
(fire-and-forget),归档管线定期全量扫描下载目录、从文件名反推 avid、把视频搬进媒体库。
文件夹本身是唯一的"记录",导致:无法跟踪某个 avid 的下载进度;离线失败/垃圾资源无法自动
换 magnet 重试;解析不了的文件夹每轮重复告警且永远堆积;各阶段边遍历边改名,靠 sleep 赌
挂载一致性。

目标:引入数据库账本(acquisition ledger),以 avid 为主键跟踪 发现 → 选磁力 → 离线下载 →
定向归档 的全生命周期,由 CloudDrive 离线任务的 `infoHash` 与账本中记录的 magnet btih 对应,
实现事件驱动、定向检测、失败自动换 magnet。全量扫描保留,但降级为兜底的对账(reconcile)
扫描——这是显式的产品功能(接住手动放入/账本外的资源),不是兼容层。

本计划按"干净重构"原则:不为旧组件保留过渡结构,旧机制要么作为领域策略被显式保留,
要么删除。

## 事实对齐(已核实的代码现状)

- 下载目录是 clouddrive2 挂载的 115 离线目录,文件由离线下载原子出现,不存在写入中被归档
  的问题;稳定性检测只为兼容其他来源,非正确性要求。
- **删除整个文件夹是设计**:只要视频文件,字幕/nfo/广告全部丢弃,节省云空间。保留此设计,
  但选择视频时必须递归看到嵌套子目录(现状 `_flatten_folder` 只看直接子项、删除却是递归的,
  嵌套视频会被无视后连带删除)。
- 离线任务监控/清理代码已存在:`rss.py::_refresh_finished_magnets` 用
  `ListOfflineFilesByPath` 过滤 `OFFLINE_FINISHED` 刷新已完成任务目录,并用
  `ClearOfflineFiles(filter=Finished, deleteFiles=False)` 清理记录。清理的唯一目的是防止
  重复扫描;账本接管终态记录后,清理动作与相关接口一并删除。
- proto 的 `OfflineFile` 携带 `infoHash`、`status`(INIT/DOWNLOADING/FINISHED/ERROR/UNKNOWN)、
  `name`、`size`、`url`、`add_time`、`percendDone`、`peers`——进度与身份信息齐全。
  现有 client 只暴露了 FINISHED 过滤版,需换成全量版。
- magnet 的 btih 就在我们已经拿到的 magnet URI 里(sukebei 侧甚至已裁剪为
  `xt=urn:btih:... + &dn=`),avid → hash 在添加离线任务时即可记录。
- 数据库迁移体系(`db.py`,当前 v4)与 `rss_failed_avids` 冷却表已存在,加表是常规操作。
- Postgres 相关测试只在 CI 跑(本地不起数据库)。
- **avid 解析现状**(2026-08-13 调研):`core/avid.py` 是 JavSP(GPL-3.0)`get_id` 的
  近似逐行移植;`remove_00`/`is_4k_video` 是散落在 `archive.py` 的补丁;`rss.py` 解析
  avid 不做 `remove_00` 而 `archive.py` 做——同一影片两管线可能得出不同 avid,与账本
  主键设计冲突,解析必须收敛单点。上游对比:JavSP 上游停更且仍不处理 00 补零;
  mdcx(GPL-3.0)对 00 的处理与 `remove_00` 同构;metatube-sdk-go(Apache-2.0)架构最
  干净,`fanza.ParseNumber` 用 int 归一 + `%03d` 正确处理 cid 去零,并有系统性标签剥离
  与 ~250 条测试语料;JavSP 有 653 条真实脏文件名语料(`unittest/testdata_avid.txt`)。
  没有可直接换用的更好 Python 库,结论:收编补丁进解析器,移植两家语料做回归。
- **许可证现状**:仓库 public 但无 LICENSE 文件(法律上"保留所有权利",且已含 GPL
  衍生代码)。决定:项目采用 GPL-3.0(Step 0),此后 JavSP/mdcx 的代码与语料均可取用。

## 目标架构

```
RSS/手动 ──发现,item 立即全部标读──▶ acquisitions(avid 账本)
                      │ 选磁力(sukebei top-N / RSS item / javbus,存候选列表)
                      ▼
                magnet_attempts(按候选逐个尝试)
                      │ AddOfflineFiles,记录 infoHash
                      ▼
        Tracker 服务循环(轮询离线任务列表,按 infoHash JOIN;非 pipeline run)
              ├─ DOWNLOADING → 更新进度(percendDone/peers)
              ├─ ERROR / 停滞超时 → 该 attempt 失败 → 提交下一个候选
              ├─ FINISHED → CAS 领取(finished→archiving) → 定向归档 task_dir/<name>
              │               ├─ 递归选视频(多分片/4K/副本策略不变)
              │               ├─ 重命名为 AVID,按品牌路由搬进媒体库(promote 逻辑不变)
              │               ├─ 删除文件夹(设计如此)
              │               ├─ 有视频 → acquisition = archived
              │               └─ 无合格视频 → attempt = junk → 提交下一个候选   ← 关键增益
              └─ resolver:捞 next_action_at 到期的 resolve_failed/exhausted 行,
                 重新解析候选并提交(不依赖 RSS 重现)
兜底:reconcile 扫描(原全量扫描降级),处理手动放入/账本外的文件夹,补录进账本
```

关键状态机:

- `acquisitions.state`:`discovered` → `downloading` → `archived`(终态);
  分支:`resolve_failed`(找不到 magnet,带冷却时间)、`exhausted`(候选用尽)、
  `needs_attention`(需人工)、`ignored`(人工忽略,终态)。
- `magnet_attempts.state`:`pending` → `submitted` → `downloading` → `finished` →
  `archiving`(CAS 领取,多副本安全)→ `archived`;失败分支 `error` / `stalled` /
  `junk` / `lost`(infoHash 从任务列表消失且无 FINISHED 记录,只会由外部手动清理导致)。
  `任务已存在` 去重返回视为 `submitted` 继续跟踪。

相对现状的行为收益:

1. 离线出错、长期无进度、下载完发现全是广告——三种情况都自动换下一个 magnet,而不是
   现在的"死等 + 垃圾文件夹删完就完了"。
2. 归档是定向的:知道哪个文件夹、期望哪个 avid,不再每轮全量 iterdir + 从文件名猜。
   `_settle` 的盲睡换成 CloudDrive API 定向 force_refresh。
3. 解析失败/多 avid 等无法决策的情况变成账本里可见、可操作的 `needs_attention` 行,
   不再是每轮重复的日志噪音。
4. RSS 端入口去重:avid 已 archived/进行中的直接跳过,省去重复离线与重复下载。
5. 已完成离线任务记录不再清理:防重复扫描的职责由账本承接,记录留在 115 侧还能让
   "任务已存在"的原生去重继续生效。
6. avid 解析单点化 + 两套上游语料回归(~900 条),账本主键的稳定性有测试背书。

## 数据模型(迁移 v5)

```sql
CREATE TABLE archive_acquisitions (
    avid TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN (
        'discovered', 'downloading', 'archived',
        'resolve_failed', 'exhausted', 'needs_attention', 'ignored'
    )),
    source TEXT NOT NULL,              -- rss_actor / rss_rank / manual / reconcile
    note TEXT,                          -- needs_attention 的原因等
    archived_paths_json TEXT NOT NULL DEFAULT '[]',
    next_action_at TIMESTAMPTZ,        -- 冷却到期 / 停滞判定时间
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX archive_acquisitions_state_idx ON archive_acquisitions (state, next_action_at);

CREATE TABLE archive_magnet_attempts (
    avid TEXT NOT NULL REFERENCES archive_acquisitions (avid) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    magnet TEXT NOT NULL,
    info_hash TEXT,                    -- btih,大写 hex;提交成功后必非空
    magnet_source TEXT NOT NULL,       -- sukebei / rss_item / javbus / manual
    size_hint BIGINT,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'submitted', 'downloading', 'finished',
        'archiving', 'archived', 'junk', 'error', 'stalled', 'lost'
    )),
    progress REAL,                     -- percendDone 快照
    error TEXT,
    submitted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (avid, attempt_no)
);
CREATE INDEX archive_magnet_attempts_hash_idx ON archive_magnet_attempts (info_hash);
```

迁移时把 `rss_failed_avids` 现有行导入为 `state='resolve_failed'` + `next_action_at =
failed_at + cooldown`,随后删除该表(冷却语义由账本承接)。

所有状态迁移用 `WHERE state = $expected` 的 CAS 式 UPDATE:既做状态机合法性校验,也是
多副本下的并发控制(`finished → archiving` 的领取即靠它,不依赖任何进程内锁)。

## 实施步骤

每一步独立可合并、可测试;标注的验收标准即该步 PR 的完成定义。

### Step 0:许可证合规(前置,独立完成)

- 仓库根添加 GPL-3.0 的 `LICENSE` 文件;`pyproject.toml` 补 license 字段。
- `core/avid.py` 顶部补 JavSP 来源与版权声明;今后从 mdcx 取用的代码同样标注。
- 从 metatube-sdk-go(Apache-2.0)移植的逻辑/语料按 Apache 要求保留其版权与许可声明
  (测试文件头注明来源与 License)。
- 验收:LICENSE 与声明就位;无代码行为变化。

### Step 1:账本 schema 与仓储层

- `db.py` 迁移 v5(上述两表 + `rss_failed_avids` 数据迁移与删表)。
- 新增 `monitor/acquisitions.py`:`AcquisitionRepository`,typed 方法覆盖
  upsert 发现、追加候选、CAS 状态迁移、按 info_hash 批量查询、`next_action_at` 到期查询、
  面向 UI 的分页列表。
- 验收:仓储层单元测试(CI Postgres gate),含状态机非法迁移被拒、CAS 领取并发互斥的用例。

### Step 2:CloudDrive 客户端(离线任务全量可见 + btih 提取)

- `client.py` / `aio.py`:新增 `list_offline_files_by_path`(不过滤状态,dict 视图返回
  name/size/url/status/infoHash/add_time/percendDone/peers,infoHash 统一大写)。
- 新增 `core/magnet.py`:`extract_info_hash(magnet: str) -> str | None`,hex40 与 base32
  两种编码归一为大写 hex。
- 旧的 FINISHED 过滤版与 `clear_finished_offline_files` 不在本步删除(调用方还在),
  在 Step 4 随调用方一并删除——不做双轨过渡期。
- 验收:单元测试(proto 消息本地构造,不需要真实服务)。

### Step 3:avid 解析器单点化(账本主键的前置条件)

- **收编 `remove_00` 进解析器**:`get_id` 增加 cid 规范化——移植 JavSP `get_cid`
  (识别整个文件名即 cid 形式),命中后与无分隔符分支产出的 `XXX-00\d{3,4}` 形态统一
  走去零归一:数字部分 int 转换 + `%03d` 保底(metatube `fanza.ParseNumber` 手法),
  保留现有 `00\d{3,4}` 的克制约束(不误伤 `ABP-012` 这类真带前导零的 3 位番号)。
  `archive.py` 的 `remove_00` 删除;RSS/归档/账本三处 avid 自动一致。
- **内置标签剥离预处理**:移植 metatube `number.Trim` 的标签清单(画质/编码/容器/
  leak/uncensored/`-C`/`-UC`/`-CH`/`cdN` 循环剥尾),作为 `AvidParser` 匹配前的默认
  预处理;**去掉裸 `A|B|C|D` 档**(无分隔符剥尾会误伤番号尾字母),仅带分隔符时剥。
  现有 `ignored_id_patterns` 配置保留,语义变为"用户追加",不再承担默认值职责。
- **4K 变体判定 token 化**:解析器暴露共享的变体标签检测(如
  `variant_tags(name) -> set[str]`,基于同一张标签清单);`is_4k_video` 的
  `stem[-3]` hack 删除,归档侧变体优选改用显式 token 判断。
- **移植两套测试语料**(约 900 条,输入直接复用,期望值按本解析器的规范形适配:
  统一大写、补 `-`;与上游输出形态不一致的逐条核对):
  - JavSP `unittest/testdata_avid.txt`:653 条真实脏文件名(中文站点前缀、CJK 标题),
    含第三列 ignore 标记一并移植(GPL-3.0,Step 0 后合规);
  - metatube `common/number/number_test.go`:~250 条(画质/字幕/分段后缀变体),
    Apache-2.0,文件头保留来源声明。
  - 初次跑不过的条目逐条分诊:解析器缺陷 → 修;语义差异(上游 Trim 形态 vs 我们的
    规范形)→ 调整期望值;确实无解 → xfail 标注留档,作为后续改进清单。
- 顺带修正:`multi_part_video_check` 对单元素列表抛 ValueError 的别扭 API 改为返回
  bool(调用方语义不变)。
- 验收:两套语料回归通过(xfail 除外);`grep remove_00 src/` 无归档层残留;
  RSS 与归档对同一文件名解析结果一致的显式测试。

### Step 4:RSS 重塑为纯发现入口

- 解析出 avid 后 upsert `discovered`;账本中已 `archived`/`downloading`/`ignored` 的直接
  跳过(入口去重)。
- **avid 一经记账,其 RSS item 立即全部标读**。FreshRSS 的未读状态不再兼职重试队列——
  重试完全由账本驱动(Step 6 的 resolver)。删除"找不到 magnet 留 1 条未读"的逻辑。
- 磁力解析从"只拿一个"改为"拿候选列表":sukebei 结果取 top-N(默认 5,复用现有
  trusted 加权排序)、RSS item 内嵌磁力、javbus 按体积排序的列表,按优先级合并、按 btih
  去重,写入 `magnet_attempts` 为 `pending`。
- 提交第一个候选:成功 → `submitted` + 记录 infoHash,acquisition → `downloading`;
  `任务已存在` → 同样记 `submitted`;全部候选都拿不到 → `resolve_failed` +
  `next_action_at`。
- 删除:`_refresh_finished_magnets` 及其全部辅助方法、`POST_ADD_SLEEP_SECONDS` 盲等、
  cooldown_lookup/cooldown_record 回调注入,以及 client/aio 侧的
  `list_finished_offline_files_by_path`(FINISHED 过滤版)与 `clear_finished_offline_files`。
- 过渡窗口说明:本步合并后、Step 6 上线前,`resolve_failed` 的 avid 不会自动重试
  (旧机制靠 RSS 重现,已删除);账本行带着到期时间等待,Step 6 的 resolver 上线后统一
  捞起,无数据丢失。期间 FINISHED 任务仍由现行全量扫描归档,不断档。
- 验收:入口去重、候选合并去重、resolve_failed 冷却、item 全量标读的测试。

### Step 5:定向归档器(替换四阶段,而非包装)

- `archive.py` 重写为单一入口
  `archive_folder(folder, dst_subdir, *, priority, expected_avid=None) -> Outcome`:
  - 签名与 route 概念无关:目的地 = `dst_dir / dst_subdir` + brand 路由;调用方自己决定
    传哪个 dst_subdir(reconcile 传 route 的映射值,tracker 传 `task_dst`);
  - 递归收集视频(修复嵌套盲区),应用领域策略:min_size、副本清理、多分片判定、
    4K 变体优选(Step 3 的 token 判定)、brand_mapping、priority 的 promote/held;
  - `expected_avid` 给定时只认该 avid 的视频;存在视频但对不上 → 返回
    `needs_attention`,不删不动;
  - 命名为 `AVID[.-cdN].ext` 搬入媒体库,随后整体删除文件夹(设计如此);
  - 返回结构化结果:archived(含目标路径列表)/ junk / needs_attention(原因)/ failed。
- **删除而非吸收旧阶段**:
  - `clear_dirname` 整体删除——它只是旧流程拿文件名当通信介质的补丁,新归档器只按
    文件的 stat + 后缀选文件,目录名无关;
  - `flatten` 中转删除——不再"先搬到 route 根再归档",folder → 库一步到位;route 根部
    散落的视频文件(历史 flatten 残留/手动放置)按"退化文件夹"由同一入口处理;
  - `rename` 独立阶段删除,并入归档动作;`_settle`/`POST_MUTATION_SLEEP_SECONDS` 盲睡
    删除;`exist_avids` 快照去重删除(由账本入口去重 + 目的地占位检查替代)。
- 本步完成后 `ArchivePipeline` 的四阶段结构不复存在;全量扫描 = 枚举 route 目录逐个调用
  `archive_folder`。本步不接数据库。
- 验收:现有 30+ 归档测试按新结构改造后语义等价通过;新增嵌套子目录、expected_avid
  不匹配、根部散落文件三组用例。

### Step 6:Tracker 服务循环(核心新增)

- **定位:常驻服务循环,不是 pipeline run**。与 `_config_refresh_loop` 同级(默认 300s,
  可配),不写 `pipeline_runs`、不占 `PipelineName`;可观测性由账本行 + tracker 状态接口
  (上次轮询时间/上次错误)承担。并发控制完全靠账本 CAS(见数据模型),多副本安全,
  不依赖进程内锁。
- 每轮:
  1. `list_offline_files_by_path(task_dir_path)` 全量拉取,按 infoHash JOIN 账本;
     账本外的 hash 直接跳过(不清理、不报错);
  2. `DOWNLOADING/INIT` → 刷新 `progress`;停滞窗口(默认 24h,可配)内 `percendDone`
     无变化 → attempt `stalled` → 提交下一候选;
  3. `ERROR` → attempt `error` → 提交下一候选;
  4. `FINISHED` → CAS 领取(`finished → archiving`)→ 定向 force_refresh 该任务目录 →
     `archive_folder(task_dir_local / name, task_dst, priority=task_priority,
     expected_avid=avid)`:archived → 终态;junk → 下一候选;needs_attention → 挂起;
  5. 候选用尽 → acquisition `exhausted` + `next_action_at`(冷却后 resolver 可重新解析);
  6. 账本中 `submitted/downloading` 但列表中无此 hash → `lost`(只会由外部手动清理
     记录导致):对应文件夹已在挂载中出现则走定向归档,否则下一候选;
  7. resolver:捞 `next_action_at` 到期的 `resolve_failed`/`exhausted` 行,重新走候选
     解析与提交(不依赖 RSS 重现)。
- 不清理已完成任务记录:`ClearOfflineFiles` 相关代码已在 Step 4 删除。若日后 115 任务
  列表有配额压力,可选加"账本已终态且超过 N 天"的定期清理,不在本计划范围。
- 配置(`ArchiveConfig` 扩展,**与 route 表解耦**):`task_dir_local`(任务目录在挂载中
  的绝对路径)、`task_dst`(dst_dir 下的目标子目录)、`task_priority`(是否按优先级路由
  语义促迁)、`tracker_interval_seconds`、`stall_timeout_hours`、`max_attempts`(默认 5)。
  readiness 拆分:tracker 只需 clouddrive + task_* 配置即可运行,route 表(mapping/
  priority_mapping)从此只属于 reconcile 扫描。
- 验收:tracker 状态迁移的集成测试(mock AsyncCloudDrive + tmp_path 挂载模拟),覆盖
  finished→archived、junk→重试、stalled→重试、账本外 hash 跳过、lost 两分支、CAS 领取
  互斥、resolver 捞起到期行。

### Step 7:全量扫描降级为 reconcile 对账(保留兜底)

- 定时 archive 运行(仍是 `PipelineName.ARCHIVE` 的调度运行)改为对账语义:
  - 跳过账本中处于活跃状态(`downloading`,含 attempt `archiving`)的 avid——账本即锁,
    与 tracker 不会同抢一个文件夹;
  - 稳定性检测:文件夹两次观察(间隔一轮)size 聚合不变才处理,兼容非离线来源;
  - 账本外的文件夹(手动放入/历史遗留)推断 avid 后补录
    `acquisitions(source='reconcile')`,统一走 `archive_folder`;
  - 无法决策(解析失败/多 avid)→ 补录 `needs_attention` 行,同一文件夹不再每轮告警
    (账本行存在即跳过日志);
  - route 源目录不存在按"无事可做"处理,不再计为失败。
- 验收:重复运行的幂等测试;告警去重测试;与 tracker 的 avid 级互斥测试。

### Step 8:API 与前端

- `monitor/api.py` 新增:acquisitions 列表(按状态过滤/分页)、详情(attempts 历史)、
  tracker 状态(上次轮询/错误),操作:重试下一候选、手动提交 magnet、标记 ignored、
  从 needs_attention 恢复。
- 前端 monitor 页新增"下载追踪"面板:avid、状态、进度条(percendDone)、当前 magnet
  来源、尝试次数、错误摘要;needs_attention 单独分组置顶。
- 设置页补 Step 6 的新配置项。
- 验收:API 测试 + 前端构建通过;操作按钮走既有权限/确认模式。

## 完成后删除的旧机制(干净度清单)

- `ArchivePipeline` 四阶段结构(clear_dirname / flatten / rename / archive)及
  `_settle`/`POST_MUTATION_SLEEP_SECONDS` 盲睡、`exist_avids` 快照去重。
- `archive.py` 层的 `remove_00` 补丁(收编进解析器)与 `is_4k_video` 的 `stem[-3]` hack
  (改为共享 token 判定)。
- `_refresh_finished_magnets` 及辅助方法、`POST_ADD_SLEEP_SECONDS` 盲等。
- "找不到 magnet 留 1 条未读"——FreshRSS 未读状态兼职重试队列的语义。
- `rss_failed_avids` 表与 cooldown_lookup/cooldown_record 回调注入。
- client/aio 的 `list_finished_offline_files_by_path`(FINISHED 过滤版)与
  `clear_finished_offline_files`。

保留的领域策略(业务规则,非兼容代码):avid 解析(单点化后)、cid 去零归一、min_size、
副本清理、多分片/4K 判定、brand_mapping、priority 的 promote/held、route 表(仅
reconcile 使用)、兜底扫库本身。

## 开放决策(已按建议值写入上文,可改)

| 决策 | 建议 | 理由 |
| --- | --- | --- |
| 项目许可证 GPL-3.0 | 采纳 | 现有 avid.py 已是 JavSP 衍生;GPL 化后 JavSP/mdcx 代码与语料均可取用 |
| 解析器骨架保留 JavSP 式识别器 | 采纳 | 需要规范形输出、全文搜索、目录回退;metatube Trim 是清洗器,不能整体替代 |
| tracker 目的地独立配置(task_dir_local/task_dst/task_priority) | 采纳 | 与 route 表解耦;route 概念只为"从目录位置推断意图"的扫描器存在 |
| FreshRSS item 一经记账立即全部标读 | 采纳 | 重试状态归账本;unread 兼职重试队列是旧耦合 |
| tracker 不作为 pipeline run | 采纳 | 300s 轮询写 runs 表是噪音;并发控制靠账本 CAS,多副本安全 |
| 候选 magnet 上限 | 5 | sukebei top-5 + javbus 合并后通常足够;上限防止死磕冷门番号 |
| 停滞超时 | 24h | 115 离线要么秒中缓存要么长期无源;给非缓存资源留一天窗口 |
| Tracker 轮询间隔 | 300s | 单次 gRPC 调用,成本低;比复用 rss interval(1800s)反馈快 |
| 多 avid 文件夹 | needs_attention,不删 | 与"junk 才删"区分:有视频但身份存疑时不做不可逆操作 |
| `rss_failed_avids` | 并入账本后删表 | 冷却本质是 acquisition 的一个状态,两处记录必然漂移 |

## 迁移与部署注意

- schema 迁移随进程启动自动执行(advisory lock 串行化),GitOps 正常发版即可。
- Step 3 合并后 avid 规范化收敛:极少数历史归档文件名若与新解析结果不一致(00 形态边缘
  案例),由 reconcile 的 needs_attention 兜住,人工处理。
- Step 4 合并后 RSS 行为变化:冷却数据已迁移;`resolve_failed` 的 avid 在 Step 6 上线前
  不会自动重试(带着 `next_action_at` 在账本中等待 resolver),期间 FINISHED 任务仍由
  现行全量扫描归档,不断档。
- Step 6 上线首轮,任务列表中现存的账本外离线任务直接跳过,其对应文件夹由 Step 7 的
  reconcile 补录归档。历史 FINISHED 记录从此留在 115 侧,不再有清理动作。
- 回滚:各步独立;Step 5 之前旧扫描行为保持等价(Step 3 的解析收敛除外,属缺陷修复),
  Step 5 是行为等价的结构重写(除嵌套盲区修复),Step 6/7 由配置项控制生效。

## 参考资料

- JavSP 解析器与语料:`javsp/avid.py`、`unittest/testdata_avid.txt`(GPL-3.0)
  https://github.com/Yuukiy/JavSP
- metatube 解析器与语料:`common/number/number.go`、`number_test.go`、
  `provider/fanza/fanza.go::ParseNumber`、`shirouto.go`(Apache-2.0)
  https://github.com/metatube-community/metatube-sdk-go
- mdcx 参考(GPL-3.0,标签清单/特例正则可借鉴):`mdcx/number.py`
  https://github.com/sqzw-x/mdcx

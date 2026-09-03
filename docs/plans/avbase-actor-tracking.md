# 演员跟踪从 JavBus 迁到 AVBase,并摘掉 FreshRSS:调研与方案

日期:2026-09-02。同日确认两项决定:FreshRSS 摘掉;Rank 继续走 RSSHub。

## 实施进度

| 步骤 | 状态 |
| --- | --- |
| 1 账本发售日节奏 + 带磁力 sighting 唤醒 | 已实现(分支上,**Postgres 测试仅 CI 验证**) |
| 2 订阅表 + 轮询器 + FreshRSS 导入 | 已实现(分支上,**Postgres 测试仅 CI 验证**) |
| 3a AVBase 客户端 + talent 类型订阅 + JavBus→AVBase 迁移脚本 | 已合并部署;**线上迁移已于 2026-09-03 完成**(316 条 JavBus star 订阅 → 282 个 AVBase talent) |
| 3b fill-actor 走 AVBase ∪ JavBus 并集目录 + "订阅此演员" + 订阅改地址 + 删 RSSHub 预热/FreshRSS | 已实现(分支上) |
| 3c 番号补零归一到目录拼写(`%03d`)+ 账本键迁移 v13 | 已实现(分支上,**Postgres 测试仅 CI 验证**) |
| 4 JavBus 磁力评分排序(可选) | 未开始 |

3b 相对方案的偏差:
- 目录取 AVBase ∪ JavBus 并集(先按名字/别名找 talent,再用同一组名字搜 JavBus star 页),而不是只走 AVBase:覆盖审计发现改名前的旧作和下架作品只有 JavBus 还列着。
- 只补两个小厂牌(OLM、MBRBN)未上 AVBase 的新作不做处理——用户决定不理会。
- "订阅此演员"直接建 `avbase_talent` 订阅并挂起 seed;订阅面板给 rss 类订阅加了改地址,方便修 RSSHub 主机名之类的问题。
- FreshRSS 客户端/配置节、RSSHub 预热、`fill_actor_job_feeds` 表(迁移 v14 删除)一并移除。

第 1 步相对本计划的偏差:

- **长尾不"放弃"**,而是压到每 30 天一次(`cadence.LONG_TAIL_FLOOR`)。计划写的是"N 次后
  exhausted/needs_attention",但那会把"老片没磁力"变成操作员队列里的噪音;每月三次请求
  的代价可以忽略,而重传确实会出现。
- 节奏用**时间**而非探测计数分段(发售前 7 天以上每周一次、发售窗口每 4 小时、之后按
  发售龄 1d/3d/7d 退避),不需要新增计数列;看板显示"等待发售 · 发售日"或"下次探测"
  而不是"第 N 次探测"。
- `park_unresolved` 改为从行的当前状态 CAS(原来固定假设 `discovered`):被唤醒的
  `resolve_failed` 行这次仍找不到磁力时才能正确续期,否则会带着过期的计时器空转。
- tracker 的"候选用尽"(`exhausted`)也改用同一节奏,无发售日时回退到停滞超时。
- 迁移 v11 加 `archive_acquisitions.release_date DATE`(可空);第 1 步没有任何来源写入
  它——AVBase JSON 在第 3 步接入,所以这一步实际生效的是唤醒和从当前状态续期,节奏
  对存量行仍是固定冷却。

第 2 步相对本计划的偏差:

- **导入不在导入时抓 feed 填游标**,而是给订阅打 `seed_pending` 标记,由首次轮询把当时的
  条目记为已见、不摄取。导入因此瞬间完成,不受 RSSHub 冷抓取速度影响;fill-actor 在
  第 3 步创建订阅时用同一标记。
- **导入是设置页的按钮而不是 CLI**:FreshRSS 地址与 API key 本来就在数据库里,先预览
  (每条 feed 标为 将导入/已存在/分类未配置/地址无效)再确认写入,不需要起集群 Job。
- 表名 `feed_subscriptions`;`avbase_talent` 类型已在 schema 与轮询器里(feed URL 由
  talent id 推导),但要到第 3 步才有创建入口。
- 番号取自标题,标题解析不出再取链接末段并剥离 `prefix:`——所以 AVBase talent feed 从
  这一步起就能作为普通 RSS URL 订阅。
- `rss` 流水线的就绪条件不再要求 FreshRSS;FreshRSS 配置段与客户端保留到导入完成后的
  第 3 步再删。

第 3 步的拆分与偏差:

- **JavBus star 订阅到 AVBase talent 的迁移是一次性的,由人手工分批完成**
  (`scripts/migrate_javbus_subscriptions.py`),不做应用内的"升级"流程。脚本三段:
  `resolve` 读订阅列表(部署后读 manager API,部署前可读 FreshRSS 的 OPML 导出),用
  名字桥(feed 标题、JavBus star 页名字,AVBase 任一别名都能命中)和番号桥(star 页首页
  作品在 AVBase 的 casts 交集)找到 talent,写 `mapping.json` 供审阅;`apply --batch N`
  逐批创建 `avbase_talent` 订阅(`seed_pending`)并**停用**而非删除旧的 JavBus 行,便于
  回退;`verify` 核验新行存在、旧行停用、talent feed 可达且名字对得上、轮询后 seed 落地
  且无错误。实测四位真实演员(含改名的河北彩花/彩伽)都由名字桥命中。
- `clients/avbase.py` 用 curl_cffi 过 Cloudflare:buildId 从首页取、404 时刷新一次再判
  未找到;`talent(name)`/`talent_works(name)`/`work(id)` 三个入口。
- API 的 `POST /api/monitor/subscriptions` 接受 `kind=avbase_talent`(talent_id、name、
  aliases、seed);视图增加 `cursor_size` 供核验。

线上迁移记录(2026-09-03,手工分批):

- FreshRSS 导入 318 条(316 条 JavBus star,其中 2 条是 `/javbus/ja/star/` 路径;2 条 Rank
  javlibrary 榜单)。`resolve`:315 条名字桥命中,1 条(塔乃花鈴)靠番号桥落到 輝星きら
  (改名);名字匹配再用 star 页作品在 AVBase 的出演者交叉核对,293 确认、10 未确认(合集
  作品不列全出演者,不是错配)、13 找不到作品可核对。
- 批次 3 → 10 → 30 → 60 → 100 → 113,每批 `apply` 后 `verify --trigger` 触发一轮轮询核验:
  全部 316 条通过(新行存在、旧行停用、feed 名字对得上、seed 落地、游标数等于 feed 去重后
  的条目数)。316 条 JavBus 行对应 282 个 talent(JavBus 每个别名一个 star 页,AVBase 合并)。
- **坑:FreshRSS 导入的 URL 主机是 `http://rsshub/`**(FreshRSS 与 RSSHub 同 namespace 的短
  名),manager 在 `media` namespace 解析不到,首轮轮询 315 条全部 `Name or service not known`。
  JavBus 行随迁移停用,不受影响;2 条 Rank 行以 `http://rsshub.rss.svc.cluster.local/`
  重建(seed)并删除旧行。以后导入前应先把 FreshRSS 里的 RSSHub 地址换成集群全名,或给
  订阅加"改地址"功能(3b 待办)。
- 旧的 JavBus `rss` 行在删前逐条核验(每条都有已 seed、无错误的 talent 订阅)后已删除。
  FreshRSS 可以下线。

AVBase 收录范围审计(2026-09-03,起因:橘梨紗 的 talent 在 AVBase 零作品):

- 对 316 个 JavBus star 页各取最近 8 部(共 2453 部)去 AVBase 查:**95% 收录,92% 的出演者
  里带着映射到的 talent**;差的 3% 全是几十人的大合集,AVBase 合集出演者名单不全,不是错配。
  逐部核对过的"未确认"行(沖田杏梨、葵/小野夕子、松嶋真麻/桃乃木かな、輝月あんり/天木ゆう、
  宇都宮しをん/安齋らら)在有单体作品时 talent id 全部正确。
- 缺失的 119 部分四类:53 部合集/再发行(Prestige プレミアムプライス ORT、ROOKIE RBB、
  S1 OFJE、E-BODY MKCK、million MQNC 等 AVBase 不收的再版品牌);48 部 2023 年前的老单体
  (退役演员被下架的作品:原紗央莉 SDMT/STAR、橘梨紗 STAR-4xx、伊東ちなみ MIDE-5xx、
  蒼井そら);**真正的新作缺口只有 3 部**(OLYMPUS 的 OLM-332、マーレー 的 MBRBN-065/066,
  两个 FANZA/MGS/DUGA 之外的小品牌),外加 ヨリヌキ/AIリマスター 这类再剪辑品。
- **改名演员的旧名作品会从 AVBase 消失**:塔乃花鈴 → 輝星きら 之后,AVBase 只有 輝星きら 名下
  42 部,塔乃花鈴 时期的 MIDA-388~501、REBD-994 都不在;JavBus 仍列着。对"发现新作"无影响,
  对补全扫描有影响。
- **番号补零形态不一致**:JavBus 写 `HTTM-0066`/`MXDLP-0337`,AVBase 写 `HTTM-066`/
  `MXDLP-337`;`AvidParser` 两种形态各自保留,同一作品会在账本里落成两个键,库内持有检查
  也对不上。待办:数字段按 int 归一后 `%03d`(metatube 的做法),但要先评估存量账本行。
  另:`YRNKMTNDVAJ-741` 被解析成 `RNKMTNDVAJ-741`(首字母被当成标签剥掉)。
- 结论:迁移正确;AVBase 作为发现源对活跃演员的新作覆盖 ≈ 99.9%;缺口在退役演员的下架
  作品与再版品牌,这两类由 JavBus 磁力源(按番号)与 3b 的 fill-actor 补全(建议 JavBus 与
  AVBase 目录取并集)兜底。

## 结论

JavBus 的 star feed 之所以"出现即有磁力",是因为它同时扮演两个角色:作品目录与磁力目录。
JavBus 默认只列出已有磁力的作品,RSSHub 路由又把磁力表直接塞进 item,所以订阅一个 feed
就等于"作品 + 磁力 + 质量筛选"三件事一起完成。AVBase 只做前一个角色:它是作品与演员身份
的权威目录(别名聚合、发售日、原生 RSS),但 feed 里的作品比发售日早 3~5 周,比磁力出现
早 4~6 周,而且不含磁力。

因此"结合两者优点"的关键不是再找一个数据源,而是把**发现**与**磁力出现**拆成两个事件:

- 发现由 AVBase 负责(talent feed + JSON 目录),解决别名与全量覆盖;
- 磁力出现由账本按发售窗口主动探测(sukebei + JavBus 详情页,与现在相同的磁力源);
- 账本的冷却节奏从"固定 24h"改为"以发售日为锚的分段节奏",这是整个方案里唯一真正
  需要新逻辑的地方。

不改账本节奏而直接把 AVBase feed 接进现有 RSS 管线,后果是每部作品在 24h 冷却里空转
约 30 次才等到发售,磁力出现后最多再等 24h,事件驱动退化成盲轮询。

第二个决定:**FreshRSS 摘掉**。它在链路里的六个职责——定时抓取、未读队列、分类到目录、
订阅注册表、订阅 UI 与预检、guid 去重——中,定时抓取与去重常驻后端和账本已经覆盖,未读
队列自迁移 v5 起就不再承担重试,分类到目录本来就是后端配置,只有"订阅注册表 + 管理 UI"
需要新建。用户自动化之后已不再用 FreshRSS 的阅读界面,摘掉没有损失。RSSHub 保留,但只
作为 Rank 这类"任意页面"的抓取器,不再是 fill-actor 的前置条件。

## 已核实的事实

### 现状:JavBus 路径

- fill-actor 扫描:`JavBusClient.scrape` 分页抓 `/star/<id>`,带 `Cookie: existmag=all`
  拿到**全量**作品(含无磁力的);缺失番号经 `AcquisitionIntake.queue` 入账本
  `discovered` + `next_action_at=now`,tracker 后台首次解析。
- RSS 路径:RSSHub `/javbus/star/<id>` → FreshRSS 分类 → `RssPipeline`:从 item **标题**
  解析番号,item 内嵌磁力表取最大者作 `rss_item` 候选,排序 sukebei → rss_item → javbus
  (按体积)。
- RSSHub 的 javbus 路由(`lib/routes/javbus/index.tsx`)**不设** `existmag` cookie,所以
  拿到的是 JavBus 默认列表——只有已有磁力的作品;每个 item 再请求详情页与
  `uncledatoolsbyajax.php`,把磁力表渲染进 description,并把评分最高的磁力
  (`score = 链接数^8 × 体积`,链接数即字幕/高清标签数)作为 enclosure。这就是"出现即有
  磁力,且往往是可靠磁力"的全部机制。
- 冷却:`rss.failed_avid_cooldown_seconds` 默认 86400;`retry_due` 在每次 tracker poll
  末尾运行(`tracker_interval_seconds` 默认 1800,每次最多 50 行)。
- `AcquisitionRepository.discover` 对冷却未到期的行返回 False:再次 sighting——哪怕这次
  item 带着磁力——会被计为 `skipped_known` 并标读,磁力信息随之丢弃,只能等冷却到期后
  由 sukebei/javbus 重新解析。

### 现状:FreshRSS 与 RSSHub

- 后端只用 FreshRSS 的三个接口:`stream/contents`(按标签取未读)、`edit-tag`(标读)、
  `subscription/list`(fill-actor 的"已订阅"预检)。
- 订阅内容:Actor 分类 = RSSHub `/javbus/star/<id>`;Rank 分类 = RSSHub javlibrary 榜单
  (item 标题含番号且无分隔符,解析器已验证可用,见 [[rss-categories-offline-dirs]])。
- FreshRSS/RSSHub 专用代码约 1250 行:`clients/freshrss.py`、`fill_actor/feeds.py`
  (RSSHub 预热器)、`fill_actor/subscriptions.py`、前端 `ActorFeeds`、两组测试;外加
  `fill_actor_job_feeds` 表、设置页的 `freshrss` 与 `feeds` 两张卡。
- RSSHub 预热器存在的原因是 javbus 路由首抓要逐个请求详情页,FreshRSS 首抓会超时。
  后端自己抓 RSSHub 后超时与重试由自己控制,预热器失去意义。

### AVBase

| 项目 | 事实 |
| --- | --- |
| talent feed | `https://www.avbase.net/talents/<talent_id>/feed`,talent 页面 `<link rel="alternate" type="application/rss+xml">` 暴露 |
| 服务端可达性 | feed 端点对 curl 直接 200;**HTML 与 `_next/data` JSON 对 httpx/curl 返回 403 Cloudflare challenge**;用 curl_cffi 浏览器指纹(chrome/safari/firefox)全部 200 |
| feed 内容 | 固定 30 条,不接受 page/limit/on_sale 参数;**按登记时间正序**(最旧在前);pubDate 是登记时间 |
| feed 标题 | **不含番号**,只有作品标题(如 `べっぴんなのに厨二病 石川澪`);番号只在 `<link>`/`<guid>`:`https://www.avbase.net/works/FWAY-091`(无 prefix);`<category>` 列出全部出演者名 |
| 登记 vs 发售 | 石川澪 feed 30 条,登记日全部比发售日早 24~35 天(如 MIZD-555 登记 8/31,发售 10/2) |
| 别名 | talent 聚合多个 actor 记录:河北彩花/河北彩伽 同 talent 5022,麻里梨夏 有 4 个名字;任一别名 URL 都解析到同一 talent;feed 按 talent 聚合,别名作品全部进 feed |
| JSON 目录 | `/_next/data/<buildId>/talents/<name>.json?name=<name>&page=N`:30 条/页,`total`,每条 `work_id`/`prefix`/`min_date`(发售日)/`casts`;buildId 来自首页 `__NEXT_DATA__`,随部署变化 |
| AVID → 演员 | `/_next/data/<buildId>/works/<id>.json?id=<id>`:`casts[].actor.{id,name,talent.id}` |
| id ↔ name | 数字 id 不能直接开页面(`/talents/46144` 404);feed 的 channel `<link>` 给出 name URL,可由 id 反查 |
| 非标准 work_id | `gyutto-285412`、`P162F-01`、`takesyobo:TSDS-43136`;现有解析器把 `sodcreate:3DSVR-2013` 解析成 `DSVR-2013`(错),需先剥离 `prefix:` |

### Sukebei

- 原生 RSS `https://sukebei.nyaa.si/?page=rss&q=<关键字>&c=2_2&f=0`,item 自带
  `nyaa:infoHash`、`nyaa:size`、`nyaa:trusted`;`f=2` 只要可信上传者;`|` 表示 OR。
  后端自己控制节奏后不再需要它做触发 feed,记录在此只为说明"按演员名搜索有效"。
- 时序样本:DLDSS-515 AVBase 发售日 9/3,sukebei 首个种子 8/6(FANZA 先行配信);
  MIDA-798 发售 8/28,种子 8/27。**`min_date` 只是发售的近似下界,先行配信可提前约 4 周。**

## 差距:JavBus 的"低摩擦"由三件事构成,AVBase 一件都没有

1. **过滤**——feed 只含已有磁力的作品,订阅方不需要等待逻辑。
2. **磁力随 item 到达**——零额外请求,一次轮询内完成提交。
3. **质量筛选**——多人上传 + 字幕/高清标签,RSSHub 取评分最高者。

AVBase feed 是"作品被登记"的信号,不是"可下载"的信号。它的优点在另一维度:别名聚合、
全量目录、发售日、服务端直接可抓。两者不是替代关系,是分工关系。

## 方案

### A. 角色分离

| 角色 | 来源 | 状态 |
| --- | --- | --- |
| 发现(discover) | 后端订阅轮询:AVBase talent 订阅(抓开放 feed)+ 通用 RSS 订阅(RSSHub javlibrary 榜单等);fill-actor 扫描 AVBase JSON 目录 | 新 |
| 磁力解析 | sukebei 搜索 + JavBus 详情页(`get_magnets`)+ 订阅 item 内嵌磁力 | 不变 |
| 磁力出现 | 账本在发售窗口内主动探测;带磁力的 item 唤醒冷却中的行 | 新 |

"触发 feed"不再是独立概念:后端自己控制节奏,不需要等别人推送。若保留 JavBus star feed
作为一条通用 RSS 订阅,其 item 自带磁力,自然成为唤醒源,但它不再承担演员身份:fill-actor
与订阅预检都改用 AVBase talent,JavBus 详情页作为磁力源照旧,它只按番号工作,与别名无关。

### B. 账本:以发售日为锚的分段节奏(核心)

账本行增加 `release_date`(AVBase `min_date`,可空),`next_action_at` 由策略函数计算,
替代单一的 `failed_avid_cooldown_seconds`:

| 阶段 | 相对发售日 | 节奏 | 理由 |
| --- | --- | --- | --- |
| 预热 | `< release - 7d` | 每 7d 一次探测 | 先行配信会提前放出种子,完全不探测会漏;每周一次代价可忽略 |
| 发售窗口 | `release - 7d ~ release + 14d` | 每 2~4h | 磁力最可能在此出现;配合 1800s 的 tracker poll 即每 4~8 轮一次 |
| 长尾 | `> release + 14d` | 1d → 3d → 7d 指数退避 | 老作品磁力偶发;N 次后 `exhausted`/`needs_attention`,不再无限轮询 |
| 无发售日 | — | 沿用现有固定冷却 | manual/reconcile 建的行,以及取不到发售日的订阅行 |

配套两条改动:

- **带磁力的 sighting 唤醒冷却中的行**。`discover` 目前对冷却中的行一律返回 False;
  改为当调用方带 `item_magnet` 时,把 `next_action_at` 拉到 now(或直接 inline 解析),
  并把该磁力作为 `rss_item` 候选记入 attempts。磁力出现到提交只隔一次轮询,与今天的
  JavBus 路径持平。
- **看板可见**:等待中的行显示"等待发售(发售日 X)/等待磁力(第 N 次探测)",替代今天
  含义模糊的 `resolve_failed`。

`release_date` 的来源:fill-actor 扫描时 JSON 里就有;订阅轮询的 item 没有发售日,首次
解析时用 works JSON 补一次(需要 Cloudflare 绕过,见 G),取不到就落回固定冷却。

### C. 订阅模型与轮询器(替代 FreshRSS)

新表 `subscriptions`(迁移 v10 或下一号):

| 列 | 含义 |
| --- | --- |
| `id`、`kind`、`enabled`、时间戳 | `kind` ∈ {`avbase_talent`, `rss`} |
| `category` | 引用 `rss.categories` 的 label;离线目录与账本 source `rss:<label>` 由分类决定 |
| `talent_id`、`name`、`aliases_json` | `avbase_talent` 专用 |
| `url` | `rss` 专用,完整 URL,通常指向 RSSHub |
| `cursor_json` | 上一轮抓到的 item key 集合,长度不超过 feed 本身 |
| `last_polled_at`、`last_error` | 面向 UI |

唯一约束 `(kind, talent_id)` 与 `(kind, url)`。分类保留而不是每条订阅自带目录:目录决定
归档路由(迁移 v8 的取舍),同类订阅共用一个目录,改目录改一处;账本 source 与看板分组
不变。配置段名 `rss` 与 `PipelineName.RSS` 也保留——它抓的仍是 feed,改名只有迁移成本
没有语义收益。

轮询器 = 现有 `RssPipeline` 去掉 FreshRSS 读写:

- 每次运行按分类遍历启用的订阅;一条订阅失败只计 `subscriptions_failed`,不影响其他,
  与今天分类之间的隔离同理。
- 抓取:`avbase_talent` 抓 `https://www.avbase.net/talents/<id>/feed`(开放端点,httpx
  即可);`rss` 抓 `url`。解析用 defusedxml(已有依赖),不引入 feedparser。
- item → 番号:标题优先,失败时取链接最后一段并剥离 `prefix:`,再过 `AvidParser`;
  `rss_magnet.get_magnet_from_item` 继续从内容里取内嵌磁力(JavBus 类 feed)。非标准 id
  被解析器拒绝即跳过,不入账本。
- 去重:账本 `discover` 按番号去重;`cursor_json` 只用于跳过上一轮已见的 item,让解析
  不出番号的 item 只告警一次。
- 首次轮询:由 fill-actor 扫描创建的订阅,创建时游标直接填满当前 feed——扫描已经覆盖
  全量目录,不必再吃 30 条旧作;手动创建的订阅正常摄取 30 条,库中已有的由
  `_skip_library_held` 兜住,其余按 B 的长尾节奏处理。
- 运行记录、看板"立即运行"、`rss.interval_seconds` 不变;统计项从 items/marked_read
  改为 subscriptions_polled/items/new_avids/subscriptions_failed。

订阅管理 UI:设置页新面板,按分类列出;添加时输入演员名、别名或 talent 页面 URL
(服务端归一到同一 talent),或一条 RSS URL;启用/禁用、删除;显示上次轮询时间与错误。
删除订阅不影响在途账本行(目录钉在行上)也不影响 tracker 轮询集合(目录仍在分类里),
这一点与删分类的坑不同。

### D. fill-actor 换 AVBase

- 新 `clients/avbase.py`:
  - 传输层用 curl_cffi(浏览器 TLS 指纹),httpx 过不了 Cloudflare;作为独立依赖,失败时
    fill-actor 报 `actor_catalog_error`,不影响订阅轮询(feed 不受拦截);
  - buildId 从首页 `__NEXT_DATA__` 取并缓存,JSON 路由 404 时刷新一次再重试;
  - `list_video_ids(talent)`:按 `total` 分页(30/页),产出 `work_id` 并剥离 `prefix:`,
    同时带回 `min_date` 供 B 使用;
  - `talent_by_name(name)`:任一别名解析到 talent id + 主名 + 别名列表;
  - `talents_for_avid(avid)`:works JSON 的 `casts`。
- actor 的语义从 JavBus star id 变为 AVBase talent;用户输入允许名字、别名或 talent URL。
- 扫描结果页加"订阅此演员":写一条 `avbase_talent` 订阅到指定分类(默认取目录等于
  `fill_actor.task_dir_path` 的分类)并填满游标;预检 `actors_already_subscribed` 改查
  订阅表。
- 删除:`RSSHubFeedWarmer`、`fill_actor_job_feeds` 表与 `JobFeedRecord` 状态机、
  `fill_actor/subscriptions.py`、前端 `ActorFeeds`、`feeds` 配置段。`freshrss` 配置段与
  `FreshRSSClient` 在 E 的导入完成后一并删除。

### E. FreshRSS 下线与订阅导入

一次性命令 `embyx-manager import-subscriptions --freshrss-url … --api-key …`(与
`import-config` 同类,集群内 Job 运行):

- 读 `subscription/list`;`/javbus/star/<id>` 的 feed 用标题里的演员名查 AVBase talent,
  命中写 `avbase_talent`,未命中打印清单人工处理;
- 其他 feed(javlibrary 榜单等)原样写成 `rss` 类型,分类取 FreshRSS 的 category 名,与
  `rss.categories` 的 label 对不上就报错退出,不猜;
- 导入后游标填满,FreshRSS 里已读过的不重吃;之后 FreshRSS 可下线;
- 迁移剥离 `freshrss`/`feeds` 两段配置(`extra='forbid'`,同 v7 手法)。

### F. 磁力质量:把 JavBus 的优点留在解析层

- JavBus 详情页仍是候选来源;可把 RSSHub 的评分思路搬进 `JavBusClient.get_magnets`
  的排序——标签数(字幕/高清)与体积加权,而不是今天的纯体积排序。这是 JavBus 磁力
  "往往可靠"的实际来源,与它的 feed 无关。
- 订阅 item 的内嵌磁力以 `rss_item` 进候选,排序位置不变。

### G. 风险与边界

- **Cloudflare 策略随时可能收紧**。设计上让"发现"只依赖 feed(当前开放),让"目录扫描"
  与"发售日补全"依赖 JSON(需指纹模拟)。JSON 挂掉只影响 fill-actor 全量扫描与发售日,
  订阅轮询退化为固定冷却,不断档。
- **buildId 随部署变化**:每次 AVBase 发版 JSON 路由 404,客户端要能自动刷新。
- **RSSHub 成为 Rank 的单点**:后端抓 RSSHub 的超时与重试自己控制;javbus 路由首抓慢
  的问题由请求超时兜住,不再需要预热。
- **先行配信**:发售窗口前的种子要等到预热探测;若某位演员这类情况多,可缩短预热间隔。
- **feed 只有 30 条**:高产演员(麻里梨夏 1571 部)只覆盖最近 30 次登记,轮询间隔小于
  登记频率即可,登记频率远低于每周 30 部。

### H. 实施顺序(每步独立可合并,且每步之后都是可运行的中间态)

1. 账本 `release_date` + 分段节奏策略 + 带磁力 sighting 唤醒;看板文案。与来源无关。
2. 订阅表 + 轮询器(先只做 `rss` 类型,用 RSSHub 的 javbus/javlibrary feed 复现今天的
   行为)+ 管理 UI + 导入命令。**这一步之后 FreshRSS 即可下线**,JavBus 仍在。
3. `clients/avbase.py` + `avbase_talent` 类型 + fill-actor 目录/AVID 查演员切换 +
   "订阅此演员";删除 RSSHub 预热、`fill_actor_job_feeds`、`feeds`/`freshrss` 配置、
   `FreshRSSClient`。**这一步之后 JavBus 只剩磁力源角色**。
4. 可选:JavBus 磁力评分排序。

第 3 步是唯一引入新依赖(curl_cffi)的一步;第 2 步是唯一动部署拓扑的一步(下线
FreshRSS)。两步互不阻塞,但按此顺序每一步都能单独上线观察。

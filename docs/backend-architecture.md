# 后端分层与分析流程

本文对应 1.2.2。QQ读取、模型调用、任务协调和结果存储分别管理；消息标签与人物画像使用独立入口和版本，不因调整消息标签而重新设计画像流程。

## 模块职责

| 模块 | 职责 | 不应承担的职责 |
| --- | --- | --- |
| `bridge/real_http.py` | HTTP 参数、响应和请求生命周期；组装应用服务 | SQL、模型进程管理 |
| `bridge/backend_service.py` | 账号作用域、分析任务、画像及缓存清理协调 | QQ原库读取、底层 Node 通信 |
| `bridge/qq_source.py` | 当前账号校验、只读会话与消息适配 | 分析结果存储、推理任务调度 |
| `bridge/node_analysis.py` | Node 进程、请求与流式事件、恢复及响应处理 | 账号结果数据库访问 |
| `bridge/result_store.py` | SQLite 结果、进度、摘要、API 缓存及事务 | 模型调用、QQ原库读取 |
| `bridge/batch_engine.py` / `batch_state.py` | 本地批处理、扫描位置与批次进度 | HTTP、界面状态 |
| `bridge/api_tasks.py` | API 任务注册、锁、运行计数与失效处理 | 业务模型调用、SQL、历史扫描 |
| `bridge/message_contracts.py` / `portrait_contracts.py` | 消息标签与画像各自的版本、作用域和契约 | 服务、存储、运行时依赖 |
| `bridge/message_results.py` | 本地 fine 与画像结果的纯校验 | IO、锁、调度 |
| `bridge/message_input.py` / `shared/message-input.ts` | 消息身份、来源、时间和引用元数据的校验与兼容投影 | QQ读取、媒体解码、OCR |
| `electron/api-message-insights.ts` | API 消息提示词、标签提取与结果整理 | 画像推理、持久化 |
| `electron/api-portrait.ts` | API 画像及画像维度分析 | 消息标签渲染 |
| `electron/local-message-insights.ts` | 本地逐句标签的问题构造与答案处理 | 模型实例、运行环境、数据库 |
| `electron/model-connectors.ts` | 各 API 协议、流式响应、用量与连接错误 | 账号与会话存储 |
| `chatui/message-insight-adapters.js` / `message-labels.js` | 本地与 API 标签适配及无状态渲染 | 网络、任务、缓存 |
| `chatui/view-state.js` | 聊天、标签、画像、设置四类前端状态 | 模型推理、数据库 |

`real_backend.py` 与 `electron/api-insights.ts` 保留旧导入路径的显式兼容导出，不承载新增业务。纯契约模块不能反向依赖服务、存储或进程适配器。导入模块不应启动数据库、模型或网络请求。

## API 消息标签

1. 前端收集当前聊天中已加载、需要分析的消息 ID，向 `/api/model-insights` 提交批次。同一会话串行处理；当前任务结束后检查新增消息。
2. 后端确认账号、会话和模型来源，读取对应文本，准备本批消息与已有的简短画像参考。前端不传 API Key。
3. `api-message-insights.ts` 每批发送简短提示词和消息列表，要求一个情绪、一个意图，每个标签四字以内。模型调用不再使用两条消息的固定批次。
4. Responses、Chat Completions 接口请求流式输出；连接器把文本增量传给 Node JSONL 通道，Python 更新任务的 `partialText`。前端解析出完整的一对标签后即可显示。
5. 最终响应由程序提取标签，可读取 JSON、Markdown 代码块和带“情感 / 意图”字段的文本，忽略额外说明。优先按目标 ID 对应，缺少可用 ID 的结果按本批待填目标顺序整理；缺失标签不编造内容。
6. 完成结果按原有存储作用域保存。流式临时显示与最终响应分别处理，增量事件不会提前结束整批任务。

当前逐句版本为 `free-label-v5-simple`，本地标签 schema 为 `generic-v9`。API 存储继续兼容旧的标量或 `affect / intents` 字段，但当前显示至多一个情绪和一个意图。`routine`、`uncertain`、`insufficient` 仍可作为旧结果的终态读取。

当前逐句输入保留每批最多 500 个目标、512 条消息及字符量边界。这里的“已加载消息”不是自动扫描全部历史。提示词及标签解析不强制模型只输出严格 JSON；连接器遇到不兼容流式响应的服务时，可以回退一次普通响应。

消息标签对超时、网络错误和限流按独立策略重试，目前为间隔 2 秒、最多 5 次。画像沿用原有重试配置，不能把两类任务的参数混为一项。SDK 的隐式重试关闭，由应用协调任务状态。

## 本地分析与标签显示

`electron/analysis.ts` 管理共享 Laya 实例、运行设备与缓存。本地 fine 分支委派 `generateFineMessageInsight`，画像继续使用原有分析入口。已准备问题按 `BATCH_SIZE = 8` 交给推理器；测试验证分组、顺序和答案合并，具体速度取决于设备。

本地和 API 标签经过独立适配后进入同一个渲染组件，消息行各显示一个情绪与意图，不展示概率和颜文字。底层概率仍可用于本地候选排序。纯标点不会仅因不含汉字而被过滤；模型没有标签时保持空白。

## 任务、身份与存储

- `ApiTaskCoordinator` 统一持有锁、条件变量、任务注册表和运行计数。线程启动与退出成对登记；清理注册表不等于任务已经退出。
- `model_source_revision` 表示模型选择版本；任务失效同时检查任务对象身份，防止切换来源或会话后，迟到响应写入新任务。
- 账号、会话、模型来源与分析版本参与结果隔离；本地结果和 API 结果不会作为同一份标签缓存混用。
- 消息输入保留 `SELF / OTHER`、真实发送者和引用元数据。缺失身份保持未知，不把所有 `OTHER` 当作同一个人；此层不实现 OCR 或恢复原文未提供的引用。
- `ResultStore` 管理事务、保存与缓存读取；服务通过仓储方法访问结果，不把数据库连接作为业务接口。
- 画像继续按原有主体和进度保存、增量处理；本版未实现新的“批次评价汇总后重新生成人格”流程。

## 桌面启动与静态资源

`start-real-client.py` 记录本次启动的 bridge 身份。启动失败后，只清理与安装位置、PID 创建时间、控制记录及分析脚本路径匹配的进程。Electron 初始化失败时，仅关闭本次创建的 bridge；复用已有服务时不把它当作本次新建进程关闭。

`real_http.py` 对 `.js`、`.mjs` 显式返回 `text/javascript; charset=utf-8`，避免 Windows 注册表的 MIME 映射导致脚本被浏览器拒绝执行。保留 `nosniff`、其他资源类型判断及静态路径校验。

## 模型下载与更新

- 标准 Windows ZIP 不包含模型。桌面下载器读取 `scripts/model-asset.json`，下载固定版本的独立 GitHub 模型 ZIP，检查大小与 SHA-256，再由自带 Python 验证每个文件并安装到 `.local/models/laya`。
- 下载完成后，界面选择 `downloaded`；`ModelSource` 返回该目录，Node 分析进程通过 `LAYA_MODEL_DIR` 或模型切换命令使用它。
- 源码命令 `npm run setup:models` 使用 `scripts/setup-models.ts` 从固定 Hugging Face revision 下载各文件，支持 `.part`、Range 续传和失败重试。桌面下载器目前不保留下载进度，两条入口的能力不同。
- 应用更新验证 Release 清单、Ed25519 签名和 ZIP 哈希，保留用户数据与模型。发布私钥只在发布端使用，不进入源码或应用包。

## 兼容、打包与验证

新增运行模块必须加入 `scripts/stage-real-client.py` 的显式清单。运行包的依赖导入在隔离目录验证，避免源码目录掩盖漏打包文件。

`npm test` 顺序执行类型检查、Node 主测试、桌面脚本测试和 Python 测试。合成测试覆盖账号作用域、任务失效、流式标签、纯标点、启动失败回收、静态 MIME 和更新保留模型等路径；它们不能代替真实服务商或用户设备的验收。

本版源码与已发布运行包的对应关系、应用内更新验收和交付文件见 [1.2.2 发布记录](releases/1.2.2.md)。

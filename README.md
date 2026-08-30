# workflow-service

平台工作流服务，负责流程定义、版本发布、流程实例和人工任务。HTTP API 面向管理端页面，内部同步调用使用中央 `platform.workflow.v1.WorkflowService` gRPC 合约；状态变化通过 NATS JetStream 领域事件发布，Temporal 负责持久化执行、等待、重试、定时器与补偿。

本仓库不包含 Web UI，也不复制其他服务的数据库或 Proto。身份由 identity-service 签发，角色与任务分配从 authorization-service 查询，服务间合约来自固定版本的 `platform-protos`。

## 执行链路

```text
HTTP / gRPC command
        │
        ▼
PostgreSQL transaction ── workflow state + Outbox event
        │
        ▼
NATS JetStream durable consumer
        │
        ▼
Temporal workflow ── approval / timer / CEL condition / dynamic gRPC task
        │
        └── activity transaction ── task or status + Outbox event
```

数据库提交和事件写入同一事务。Outbox dispatcher 只发布已提交记录，并使用事件 ID 去重；JetStream consumer 显式确认、有限重试并将最终失败写入 DLQ。Temporal workflow ID 与实例 ID 稳定对应，重复启动、信号和 Activity 重试必须保持幂等。

## API

所有业务 HTTP 接口统一使用 `POST application/json` 和响应结构：

```json
{"code":0,"message":"success","body":{},"request_id":"01..."}
```

页面接口包括：

- 流程定义：`create`、`update`、`publish`、`disable`、`get`、`list`
- 流程实例：`start`、`cancel`、`get`、`list`
- 我的任务：`claim`、`complete`、`delegate`、`get`、`list`

路径前缀为 `/api/v1/workflow`。列表接口使用独立的页面 DTO，不暴露 SQL 模型或 Proto。Swagger 位于 `/swagger/index.html`（按环境配置开启），契约由 `make swagger` 生成，CI 使用 `make swagger-check` 检查漂移。

内部 gRPC 在独立端口提供同等领域能力。JWT 通过 `authorization: Bearer <token>` 传递，PSK 通过 `Authorization: PSK <key>` 传递；健康检查使用标准 gRPC Health 服务。反射只在开发环境开启。

## 认证与授权

- 生产环境仅验证 identity-service 的 EdDSA/JWKS Token，并校验 issuer 与 `workflow-service` audience。
- HTTP、gRPC 的免认证与 PSK 路由均由配置控制并支持 Go `path.Match` 通配符。
- 服务端从认证 Principal 获取 tenant、membership 和 actor，禁止信任请求体中的角色或审计人。
- 人工任务角色通过 authorization-service 的 `ListBindings` 解析；跨租户访问直接拒绝。
- 所有写入显式维护 `created_at`、`updated_at`、`created_by`、`updated_by` 和 `version`，更新使用期望版本乐观锁。

## 配置与环境

配置优先级类似 Spring Boot：默认值 < `config.yaml` < `config-{env}.yaml` < `APP_*` 环境变量 < `-env` 参数。当前目录的配置文件会自动读取，实际 profile 和已加载文件保存在运行时配置上下文中。

主要生产依赖：

- PostgreSQL 数据库 `platform`、schema `workflow`、迁移表 `workflow_schema_migrations`
- Redis：限流、幂等与共享分布式锁基础设施
- NATS JetStream：`PLATFORM_EVENTS` / `platform.>`
- Temporal：`default` namespace / `workflow-service` task queue
- authorization-service：内部 gRPC
- identity-service：JWKS

敏感值只从 Secret 或密钥系统注入。生产配置缺少 JWKS、数据库、Temporal 等必要参数时会快速失败。

## 本地开发

```bash
make dev-up
make dev-logs
make dev-down
```

Compose 提供 PostgreSQL、MySQL、Redis、NATS、Temporal 和自动迁移，不启动 Prometheus、Grafana、Jaeger、OTel Collector 或 Web UI。完整平台联调优先使用总仓库的 `make dev-up`，其中会同时启动 authorization-service。

若只运行进程，可覆盖配置后执行：

```bash
go run ./cmd/api -config config/config.yaml -env development
```

HTTP 默认 `127.0.0.1:8080`，gRPC 默认 `127.0.0.1:9090`。`GET|POST /live` 检查进程，`GET|POST /ready` 对数据库和 Redis 使用独立超时。请求会接收或生成 `X-Request-ID`，并将 Request ID、Trace ID、环境和版本写入日志。

## 数据库迁移

PostgreSQL/Kingbase 使用 schema 隔离，MySQL 使用数据库隔离。每个服务拥有独立迁移记录表，启动自动迁移由数据库锁串行化：

```bash
make migrate-up
make migrate-down
go run ./cmd/migrate -steps 1
```

迁移同时提供 PostgreSQL、MySQL 和 Kingbase 版本。PostgreSQL/Kingbase 字符串优先 `TEXT`，时间使用 `TIMESTAMPTZ` 并以 `Asia/Shanghai` 展示。

## 测试与发布门禁

```bash
make test-race
go vet ./...
make lint
make swagger-check
make test-integration
```

集成测试通过 `integration` build tag 隔离，使用 Testcontainers 覆盖 PostgreSQL、MySQL、迁移 up/down、NATS JetStream 和 Redis；authorization-service 使用进程内 gRPC stub，因此本服务测试不依赖其他服务运行。Temporal 的流程逻辑使用官方 testsuite 做确定性测试。

`make build` 和 `make docker-build` 注入版本、Git commit 与 UTC 构建时间；`POST /api/v1/version` 返回版本、commit、启动时间和 uptime。CI 构建 amd64/arm64 镜像并发布到 `ghcr.io/lihongjie0209/workflow-service`，同时生成 SBOM 和 provenance。

Kubernetes 基线在 `deployments/`，包含双端口 Service、探针、HPA、PDB、资源限制、安全上下文和 NetworkPolicy。平台默认使用 init/startup migration；独立 migration Job 仅用于配置与 Secret 已预先创建的发布流程。

## 合约与共享 SDK

- gRPC 和事件消息：`github.com/lihongjie0209/platform-protos`
- Principal、认证、错误码、JetStream、Outbox、Redis 锁、动态 gRPC：`github.com/lihongjie0209/microservice-platform-go`

公共行为在至少两个服务稳定复用后才抽取到 SDK；工作流图、仓储、页面 DTO、授权语义和编排逻辑保留在本服务。

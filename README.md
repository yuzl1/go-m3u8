# go-m3u8

配合 [N_m3u8DL-RE](https://github.com/nilaoda/N_m3u8DL-RE) 的 m3u8 下载 Web 服务。浏览器配置参数、猫抓 (Cat-Catch) 一键发送下载，下载完成后自动同步到 Agent 节点（适合主服务器硬盘小的场景）。

## 架构

```
浏览器(猫抓) ──POST──▶ 主服务 ──exec──▶ N_m3u8DL-RE + ffmpeg
                          │
                          │ 下载完成 → 创建传输任务 → 删除本地文件(释放硬盘)
                          │
        Agent 节点(无公网) ◀── 主动拨出 WS 控制通道 + HTTP 拉取文件
```

同一个 Docker 镜像两种模式：
- 默认 = 主服务（Web UI + 下载 + Agent Hub）
- `-agent` / `AGENT_MODE=1` = 纯 Agent 节点

## 部署（Docker Hub 镜像，无需本地编译）

### 0. 一次性配置：GitHub Actions 自动构建推送到 Docker Hub

**a. Docker Hub 创建 Access Token**

1. 注册/登录 [hub.docker.com](https://hub.docker.com)
2. 右上角头像 → **Account Settings** → **Security** → **Access Tokens**
3. **New Access Token** → 描述填 `github-actions` → 权限选 **Read & Write** → Generate
4. 复制生成的 token（只显示一次）

**b. GitHub 仓库配置 Secrets**

1. 打开仓库 → **Settings** → **Secrets and variables** → **Actions**
2. **New repository secret** 添加两个：
   | Name | Value |
   |---|---|
   | `DOCKERHUB_USERNAME` | Docker Hub 用户名 |
   | `DOCKERHUB_TOKEN` | 上面复制的 Access Token |

**c. 触发构建**

推送代码到 `master` 或 `agent` 分支即自动构建：
- `master` 分支 → `yu1998/go-m3u8:latest`
- `agent` 分支 → `yu1998/go-m3u8:agent`
- 同时构建 **linux/amd64 + linux/arm64** 双架构（Oracle ARM 服务器直接可用）

> 仓库 Actions 页面可查看构建进度。也可手动：Actions → Build and Push Docker Image → Run workflow。

### 1. 主服务

```bash
git clone git@github.com:yuzl1/go-m3u8.git && cd go-m3u8
docker compose pull && docker compose up -d
# 访问 http://服务器IP:5000
```

### 2. Agent 节点（可选）

在 Agent 服务器上只放一个文件：

```bash
mkdir go-m3u8-agent && cd go-m3u8-agent
# 把仓库里的 docker-compose.agent.yml 拷进来，改好环境变量
docker compose -f docker-compose.agent.yml up -d
```

```yaml
# docker-compose.agent.yml 关键配置
environment:
  - AGENT_SERVER=ws://主服务器IP:5000/agent/ws   # 主服务地址
  - AGENT_TOKEN=主服务页面「Agent 管理」显示的Token  # 共享密钥
  - AGENT_NAME=home-agent                        # 节点名字
  - AGENT_DIR=/downloads                         # 文件保存目录
```

连接成功后，主服务「🤖 Agent 管理」页能看到节点在线。下载完成 → 自动传输 → 主服务删除本地文件。

## 猫抓 (Cat-Catch) 配置

猫抓 → 设置 → 调用程序 → 新增：

| 项 | 值 |
|---|---|
| 请求方式 | POST |
| 地址 | `http://主服务IP:5000/api/download` |
| Content-Type | `application/json` |
| 请求体 | `{"url":"${url}","title":"${title}","referer":"${referer}","cookie":"${cookie}","userAgent":"${userAgent}","fullFileName":"${fullFileName}"}` |

> 主服务网页「🐱 猫抓配置」标签有完整说明和标签速查表。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/download` | 触发下载 |
| GET | `/api/tasks` | 任务列表 |
| DELETE | `/api/tasks/{id}` | 取消任务；`?action=delete` 删除 |
| POST | `/api/tasks/{id}/retry` | 重试 |
| GET | `/api/tasks/{id}/log` | 任务完整日志 |
| GET/PUT | `/api/config` | 全局配置 |
| GET | `/api/files` | 下载目录文件列表 |
| GET | `/api/agents` | Agent 节点列表 |
| GET | `/api/transfers` | 同步传输任务 |
| GET | `/ws` | 任务实时推送（WebSocket） |
| GET | `/agent/ws?token=&name=` | Agent 控制通道 |
| GET | `/agent/files/{id}?token=` | Agent 拉取文件 |

## 本地开发

```bash
go run .                          # 主服务 (localhost:8080)
go run . -agent                   # Agent 模式（配 AGENT_SERVER/AGENT_TOKEN 环境变量）
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --build  # 本地构建镜像
```

# OceanBase Database Proxy

TODO: some badges here

OceanBase Database Proxy（简称 ODP）是 OceanBase 数据库专用的代理服务器。OceanBase 数据库的用户数据以多副本的形式存放在各个 OBServer 上，ODP 接收用户发出的 SQL 请求，并将 SQL 请求转发至最佳目标 OBServer，最后将执行结果返回给用户。

## 快速使用

请查看快速使用指南[link TODO]开始试用 ODP。

### build.sh 快速编译指南

`build.sh` 是 ODP 项目的主要构建脚本，支持多种编译模式和平台。以下是常用的编译命令：

#### 基本用法

```bash
# 清理之前的编译文件
./build.sh clean
# 初始化依赖（首次编译）
./build.sh init
# 初始化依赖（非首次编译）
./build.sh qinit
# 配置为调试版本编译（debug可换为release或其他选项）
./build.sh config debug
# 执行编译
./build.sh make
# 编译产物：二进制可执行文件位于 `src/obproxy/obproxy`

# 其他操作
# 生成 RPM 包（注意：在435及之前分支调用./build.sh rpm打包出现"cp: No such file or directory"的报错，可通过调用./build.sh rpm obproxy-ce来解决）
./build.sh rpm
```

## 文档

- 简体中文 [link TODO]
- 英文（English） [link TODO]

## 许可证

ODP 使用 [MulanPubL - 2.0](https://license.coscl.org.cn/MulanPubL-2.0/index.html) 许可证。您可以免费复制及使用源代码。当您修改或分发源代码时，请遵守木兰协议。

## 如何贡献

我们十分欢迎并感谢您为我们贡献。以下是您参与贡献的几种方式：

- 向我们提 issue [link TODO]。
- 提交 PR。详情参见[如何贡献](CONTRIBUTING.md)。

## 获取帮助

如果您在使用 ODP 时遇到任何问题，欢迎通过以下方式寻求帮助：

- GitHub Issue [link TODO]
- 官方论坛 [link TODO]
- 知识问答 [link TODO]

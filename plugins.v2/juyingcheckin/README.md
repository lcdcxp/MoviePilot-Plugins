# 聚影签到

MoviePilot V2 插件，用于聚影自动签到。

## 插件信息

- 插件名称：聚影签到
- 插件 ID：JuyingCheckin
- 插件目录：juyingcheckin
- 插件版本：1.1
- 作者：jidian
- 感谢：感谢大胖提供的支持。

## 功能

- 账号密码登录签到
- 单账号 / 多账号
- 默认每天 08:10 定时签到
- 保存后立即测试
- 使用 MoviePilot 的 PROXY_HOST 代理
- 失败重试：只对网络异常、超时、限流或服务器错误重试
- 成功、已签到、失败通知分别简洁展示
- 签到历史、用户信息、下次运行时间
- 保存后检测代理
- 保存后清空历史记录

## 配置方法

单账号填写“用户名”和“密码”。

多账号填写到“多账号，可选”中，每行一个：

```text
账号1#密码1
账号2#密码2
```

也支持青龙变量格式：

```bash
JUYING_ACCOUNT='账号1#密码1@账号2#密码2'
```

如果 MoviePilot 容器直连聚影出现 `Connection reset by peer`，请开启“使用代理”，并确认容器内可以读取到 `PROXY_HOST`。

## 安装

插件目录通常是：

```bash
/app/app/plugins/juyingcheckin
```

正确结构：

```bash
/app/app/plugins/juyingcheckin/__init__.py
```

建议先删除旧版目录：

```bash
rm -rf /app/app/plugins/juyingsignin
rm -rf /app/app/plugins/juyingcheckin
```

再复制新版 `juyingcheckin` 目录进去，然后重启 MoviePilot。

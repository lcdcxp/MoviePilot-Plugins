# 聚影签到

MoviePilot V2 插件，用于聚影自动签到。

## 插件信息

- 插件名称：聚影签到
- 插件 ID：JuyingCheckin
- 插件目录：juyingcheckin
- 插件版本：1.3
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
- 插件详情页一键清空历史记录
- 安全限制：站点与接口仅允许 HTTPS，完整接口地址必须与站点同源
- 代理、异常和历史记录中的敏感信息自动脱敏

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

邮箱账号也可以用于多账号配置，例如 `user@example.com#密码`。

如果 MoviePilot 容器直连聚影出现 `Connection reset by peer`，请开启“使用代理”，并确认容器内可以读取到 `PROXY_HOST`。

如果通知中出现 `failed to query the DNS server`、`127.0.0.53:53 timeout` 等提示，说明当前代理或容器 DNS 无法解析目标站点。请检查 `PROXY_HOST` 所在容器/宿主机 DNS，或临时关闭“使用代理”测试直连。

## 更新记录

- v1.2：修复邮箱多账号解析；限制站点/API 为 HTTPS 同源，降低凭据外发和 SSRF 风险；代理和异常信息脱敏；增强成功/失败/已签到判断。
- v1.3：把清空历史记录从配置页移动到插件详情页，在最近签到历史卡片中一键清空。

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

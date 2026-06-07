# MoviePilot-Plugins

个人 MoviePilot V2 插件仓库。

## 插件列表

- CloudDrive2通知：`plugins.v2/cd2notify`
- 聚影签到：`plugins.v2/juyingcheckin`
- 监控strm刮削网盘：`plugins.v2/localmetadatacleaner`

## MoviePilot 插件市场地址

把本仓库地址填入 MoviePilot 的插件市场，例如：

```text
https://github.com/你的GitHub用户名/你的仓库名
```

注意：MoviePilot 插件市场只读取 GitHub 仓库 main 分支。


## 更新记录

- LocalMetadataCleaner v1.1：修复配置布局与队列显示相关问题，优化待处理任务、最近处理记录和页面展示。
- LocalMetadataCleaner v1.2：修复已知问题并继续优化任务队列、页面布局和刮削处理逻辑。

- LocalMetadataCleaner v1.3：修复 MP 刮削 Path 参数和单集目标后缀匹配；优化队列定时恢复；新增单任务手动立即执行。
- LocalMetadataCleaner v1.4：新增入库事件 INFO 调试日志，放宽 channel 限制，便于排查媒体库和 STRM 路径匹配问题。

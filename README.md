# MoviePilot 插件仓库

本仓库用于存放自用 MoviePilot V2 插件，三个插件相互独立，方便后续单独更新。

## 插件列表

| 插件ID | 插件名称 | 当前版本 | 目录 |
| --- | --- | --- | --- |
| CD2Notify | CloudDrive2通知 | v1.2 | plugins.v2/cd2notify |
| JuyingCheckin | 聚影签到 | v1.3 | plugins.v2/juyingcheckin |
| LocalMetadataCleaner | 监控strm刮削网盘 | v2.8.1 | plugins.v2/localmetadatacleaner |

## 更新说明

- 每个插件保存在独立目录中。
- 更新某个插件时，只需要替换对应目录，并同步修改 `package.v2.json` / `package.json` 里的版本号。
- MoviePilot 插件市场添加本仓库地址后，刷新插件市场即可识别更新。

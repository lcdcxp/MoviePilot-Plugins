# MoviePilot 插件仓库

本仓库用于存放自用 MoviePilot 插件。V2 与 V3 两代插件并存：V3 宿主优先读 `package.v3.json`，找不到条目再回退 `package.v2.json` / `package.json`，因此老的 V2 插件无需迁移。

## 插件列表

| 插件ID | 插件名称 | 当前版本 | 代际 | 目录 |
| --- | --- | --- | --- | --- |
| CD2Notify | CloudDrive2通知 | v1.2 | V2 | plugins.v2/cd2notify |
| JuyingCheckin | 聚影签到 | v1.4 | V2 | plugins.v2/juyingcheckin |
| LocalMetadataCleaner | 监控strm刮削网盘 | v2.9.2 | V2 | plugins.v2/localmetadatacleaner |
| LibraryIntegrity | 媒体库缺集检查 | v1.0.15 | V3 | plugins.v3/libraryintegrity |

## LibraryIntegrity 说明

上游来源：[jxxghp/MoviePilot-Plugins](https://github.com/jxxghp/MoviePilot-Plugins) 的 `plugins.v3/libraryintegrity`，原作者 Derrors，协议 GPL-3.0（见 `LICENSE`）。本仓库版本在上游 1.0.12 基础上修改：

- **v1.0.15**：作者字段改为 `Derrors, lcdcxp`（保留原作者 + 追加维护者），`author_url` 指向本仓库。
- **v1.0.14**：显示名称改为「媒体库缺集检查」。插件 ID `LibraryIntegrity`、目录 `libraryintegrity`、配置前缀 `libraryintegrity_` 全部保持不变，已装插件的配置不会丢。
- **v1.0.13**：配置页的「Emby 媒体库名称」改为可选下拉。打开配置页时从已连接的 Emby 服务器读取影视库列表（自动剔除音乐、书籍、家庭视频等非影视库），可点选多个；读取失败时仍可手动输入后回车。

## 更新说明

- 每个插件保存在独立目录中。
- V2 插件更新：替换对应目录，并同步修改 `package.v2.json` / `package.json` 里的版本号。
- V3 插件更新：替换对应目录，并同步修改 `package.v3.json` 里的版本号与 `history`，三处版本必须一致：
  1. `plugins.v3/<插件目录>/__init__.py` 的 `plugin_version`
  2. `package.v3.json` 该条目的 `version`
  3. 同条目 `history` 里最新一版的 key（按语义版本降序）
- MoviePilot 插件市场添加本仓库地址后，刷新插件市场即可识别更新。

## 测试

`tests/v3/libraryintegrity/` 是上游的回归测试，改功能后可本地跑：

```bash
python3 -m pytest tests/v3/libraryintegrity -q
```

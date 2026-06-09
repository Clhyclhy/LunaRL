import asyncio
import random
import json
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger


@register(
    "astrbot_plugin_schedule_greeting",
    "LunaRL",
    "根据作息时间表定时给指定用户发送问候消息，支持模板/Set/日历管理",
    "1.0.0",
)
class ScheduleGreetingPlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self.target_umo = self.config.get("target_umo", "")
        self.enable_ai = self.config.get("enable_ai_enhance", False)
        self.timezone = self.config.get("timezone", "Asia/Shanghai")
        self._running_tasks = []
        self._conversation_state = {}  # 用于交互式命令的状态管理

    @property
    def weekday_templates(self) -> dict:
        return self.config.get("weekday_templates", {})

    @property
    def rest_templates(self) -> dict:
        return self.config.get("rest_templates", {})

    @property
    def sets(self) -> dict:
        return self.config.get("sets", {})

    @property
    def active_set(self) -> str:
        return self.config.get("active_set", "")

    @property
    def calendar(self) -> dict:
        return self.config.get("calendar", {})

    async def initialize(self):
        """插件初始化"""
        if not self.target_umo:
            logger.warning("[定时问候] 未配置目标用户 (target_umo)，请使用 !get_umo 命令获取会话 ID")
            return

        logger.info(f"[定时问候] 插件已加载，目标用户: {self.target_umo}")
        await self._register_daily_tasks()

        # 注册每日重置任务
        try:
            await self.context.cron_manager.add_basic_job(
                name="schedule_greeting_daily_reset",
                cron_expression="1 0 * * *",
                handler=self._daily_reset_handler,
                description="每日重置定时问候任务",
                timezone=self.timezone,
            )
            logger.info("[定时问候] 每日重置任务已注册")
        except Exception as e:
            logger.error(f"[定时问候] 注册每日重置任务失败: {e}")

    async def terminate(self):
        """插件销毁"""
        for task in self._running_tasks:
            if not task.done():
                task.cancel()
        self._running_tasks.clear()
        logger.info("[定时问候] 插件已卸载")

    def _get_today_template(self) -> list:
        """获取今天的时间表模板"""
        today = datetime.now().strftime("%Y-%m-%d")
        weekday = datetime.now().weekday()

        # 优先检查日历标记
        if today in self.calendar:
            mark = self.calendar[today]
            template_name = mark.get("template", "")
            if mark.get("type") == "rest":
                return self.rest_templates.get(template_name, [])
            else:
                return self.weekday_templates.get(template_name, [])

        # 使用当前 set
        current_set = self.sets.get(self.active_set, {})
        if weekday < 5:
            template_name = current_set.get("weekday", "")
            return self.weekday_templates.get(template_name, [])
        else:
            template_name = current_set.get("rest", "")
            return self.rest_templates.get(template_name, [])

    async def _register_daily_tasks(self):
        """注册当天的所有定时任务"""
        schedule = self._get_today_template()
        if not schedule:
            logger.warning("[定时问候] 当天时间表为空，请检查配置")
            return

        for idx, item in enumerate(schedule):
            time_str = item.get("time", "")
            if not time_str or ":" not in time_str:
                continue

            hour, minute = time_str.split(":")
            cron_expr = f"{minute} {hour} * * *"

            try:
                await self.context.cron_manager.add_basic_job(
                    name=f"schedule_greeting_{idx}",
                    cron_expression=cron_expr,
                    handler=self._send_greeting_handler,
                    description=f"定时问候 - {time_str}",
                    timezone=self.timezone,
                    payload={"config": item, "index": idx},
                )
                logger.info(f"[定时问候] 已注册任务: {time_str}")
            except Exception as e:
                logger.error(f"[定时问候] 注册任务失败 {time_str}: {e}")

    async def _daily_reset_handler(self, **payload):
        """每日重置任务回调"""
        logger.info("[定时问候] 执行每日重置...")
        try:
            jobs = await self.context.cron_manager.list_jobs()
            for job in jobs:
                if job.get("name", "").startswith("schedule_greeting_") and job.get("name") != "schedule_greeting_daily_reset":
                    await self.context.cron_manager.delete_job(job["id"])
            await self._register_daily_tasks()
        except Exception as e:
            logger.error(f"[定时问候] 每日重置失败: {e}")

    async def _send_greeting_handler(self, **payload):
        """发送问候消息的回调"""
        config = payload.get("config", {})
        idx = payload.get("index", 0)

        # 随机延迟
        random_range = config.get("random_range", 5)
        delay_seconds = random.randint(0, random_range * 60)
        if delay_seconds > 0:
            logger.info(f"[定时问候] 任务 {idx} 将延迟 {delay_seconds} 秒执行")
            await asyncio.sleep(delay_seconds)

        # 生成消息内容
        if self.enable_ai and config.get("ai_prompt"):
            text = await self._generate_ai_message(config["ai_prompt"])
        else:
            text = config.get("message", "你好~")

        # 发送消息
        try:
            chain = MessageChain().message(text)
            await self.context.send_message(self.target_umo, chain)
            logger.info(f"[定时问候] 已发送消息: {text[:30]}...")
        except Exception as e:
            logger.error(f"[定时问候] 发送消息失败: {e}")

    async def _generate_ai_message(self, ai_prompt: str) -> str:
        """使用 AI 生成消息内容"""
        try:
            providers = self.context.get_all_providers()
            if not providers:
                logger.warning("[定时问候] 无可用的 AI 提供商，使用预设消息")
                return ""

            provider = providers[0]
            prompt = f"你是一个温柔体贴的伴侣，请根据以下提示生成一句简短的问候消息（不超过30字）：\n{ai_prompt}"
            response = await provider.text_chat(prompt=prompt)
            return response.completion_text.strip()
        except Exception as e:
            logger.error(f"[定时问候] AI 生成失败: {e}")
            return ""

    async def _generate_ai_template(self, template_type: str, descriptions: list) -> list:
        """使用 AI 生成完整的时间表模板"""
        try:
            providers = self.context.get_all_providers()
            if not providers:
                return []

            provider = providers[0]
            desc_text = "\n".join([f"- {d}" for d in descriptions])
            prompt = f"""你是一个温柔体贴的伴侣，请根据以下描述生成一个{template_type}时间表。

要求：
1. 每个描述对应一个时间点
2. 时间格式为 HH:MM
3. 消息内容温馨自然，不超过30字
4. 返回 JSON 格式

描述：
{desc_text}

返回格式：
[
  {{"time": "07:00", "message": "消息内容"}},
  {{"time": "12:00", "message": "消息内容"}}
]"""

            response = await provider.text_chat(prompt=prompt)
            result_text = response.completion_text.strip()

            # 提取 JSON 部分
            start = result_text.find("[")
            end = result_text.rfind("]") + 1
            if start == -1 or end == 0:
                return []

            json_text = result_text[start:end]
            items = json.loads(json_text)

            # 补充默认字段
            for item in items:
                if "random_range" not in item:
                    item["random_range"] = 5
                if "ai_prompt" not in item:
                    item["ai_prompt"] = ""

            return items
        except Exception as e:
            logger.error(f"[定时问候] AI 模板生成失败: {e}")
            return []

    # ==================== 模板管理命令 ====================

    @filter.command_group("tpl")
    def tpl_group(self):
        """模板管理命令"""
        pass

    @tpl_group.command("list")
    async def tpl_list(self, event: AstrMessageEvent):
        """列出所有模板"""
        lines = ["=== 工作日模板 ==="]
        for name in self.weekday_templates:
            lines.append(f"  {name}")
        lines.append("\n=== 休息日模板 ===")
        for name in self.rest_templates:
            lines.append(f"  {name}")
        yield event.plain_result("\n".join(lines))

    @tpl_group.command("show")
    async def tpl_show(self, event: AstrMessageEvent, template_name: str):
        """显示模板详情"""
        # 在工作日模板中查找
        if template_name in self.weekday_templates:
            template_type = "工作日"
            items = self.weekday_templates[template_name]
        elif template_name in self.rest_templates:
            template_type = "休息日"
            items = self.rest_templates[template_name]
        else:
            yield event.plain_result(f"模板 '{template_name}' 不存在")
            return

        lines = [f"模板: {template_name} ({template_type})"]
        lines.append("-" * 30)
        for item in items:
            time_str = item.get("time", "??:??")
            msg = item.get("message", "")
            delay = item.get("random_range", 0)
            lines.append(f"  {time_str} - {msg} (延迟{delay}分钟)")

        yield event.plain_result("\n".join(lines))

    @tpl_group.command("add")
    async def tpl_add(self, event: AstrMessageEvent, template_type: str, template_name: str, time_str: str, message: str):
        """添加时间点到模板"""
        if template_type not in ["weekday", "rest"]:
            yield event.plain_result("类型必须为 weekday 或 rest")
            return

        templates = self.weekday_templates if template_type == "weekday" else self.rest_templates

        if template_name not in templates:
            yield event.plain_result(f"模板 '{template_name}' 不存在")
            return

        # 验证时间格式
        if ":" not in time_str:
            yield event.plain_result("时间格式错误，请使用 HH:MM")
            return

        templates[template_name].append({
            "time": time_str,
            "random_range": 5,
            "message": message,
            "ai_prompt": ""
        })

        # 按时间排序
        templates[template_name].sort(key=lambda x: x.get("time", ""))

        yield event.plain_result(f"已添加 {time_str} 到模板 '{template_name}'")

    @tpl_group.command("remove")
    async def tpl_remove(self, event: AstrMessageEvent, template_name: str, time_str: str):
        """从模板删除时间点"""
        # 查找模板
        if template_name in self.weekday_templates:
            templates = self.weekday_templates
        elif template_name in self.rest_templates:
            templates = self.rest_templates
        else:
            yield event.plain_result(f"模板 '{template_name}' 不存在")
            return

        # 查找并删除
        original_len = len(templates[template_name])
        templates[template_name] = [t for t in templates[template_name] if t.get("time") != time_str]

        if len(templates[template_name]) < original_len:
            yield event.plain_result(f"已从模板 '{template_name}' 删除 {time_str}")
        else:
            yield event.plain_result(f"模板 '{template_name}' 中未找到 {time_str}")

    @tpl_group.command("delete")
    async def tpl_delete(self, event: AstrMessageEvent, template_name: str):
        """删除整个模板"""
        if template_name in self.weekday_templates:
            if len(self.weekday_templates) <= 1:
                yield event.plain_result("至少保留一个工作日模板")
                return
            del self.weekday_templates[template_name]
            yield event.plain_result(f"已删除工作日模板 '{template_name}'")
        elif template_name in self.rest_templates:
            if len(self.rest_templates) <= 1:
                yield event.plain_result("至少保留一个休息日模板")
                return
            del self.rest_templates[template_name]
            yield event.plain_result(f"已删除休息日模板 '{template_name}'")
        else:
            yield event.plain_result(f"模板 '{template_name}' 不存在")

    @tpl_group.command("ai_generate")
    async def tpl_ai_generate(self, event: AstrMessageEvent, template_type: str):
        """AI 生成模板（交互式）"""
        if template_type not in ["weekday", "rest"]:
            yield event.plain_result("类型必须为 weekday 或 rest")
            return

        yield event.plain_result(
            "请发送时间点描述，每行一个，格式：\n"
            "时间 - 大致意思\n\n"
            "示例：\n"
            "07:00 - 温柔叫起床\n"
            "12:00 - 关心吃午饭\n"
            "18:00 - 安慰下班\n"
            "22:00 - 温柔晚安\n\n"
            "发送完毕后请回复 '完成'"
        )

        # 保存状态
        self._conversation_state[event.unified_msg_origin] = {
            "command": "tpl_ai_generate",
            "template_type": template_type,
            "descriptions": []
        }

    # ==================== Set 管理命令 ====================

    @filter.command_group("set")
    def set_group(self):
        """Set 管理命令"""
        pass

    @set_group.command("list")
    async def set_list(self, event: AstrMessageEvent):
        """列出所有 Set"""
        lines = ["=== Set 列表 ==="]
        for name, data in self.sets.items():
            active = " (当前使用)" if name == self.active_set else ""
            weekday = data.get("weekday", "?")
            rest = data.get("rest", "?")
            lines.append(f"  {name}{active}")
            lines.append(f"    工作日: {weekday}")
            lines.append(f"    休息日: {rest}")
        yield event.plain_result("\n".join(lines))

    @set_group.command("create")
    async def set_create(self, event: AstrMessageEvent, set_name: str, weekday_template: str, rest_template: str):
        """创建新 Set"""
        # 验证模板存在
        if weekday_template not in self.weekday_templates:
            yield event.plain_result(f"工作日模板 '{weekday_template}' 不存在")
            return
        if rest_template not in self.rest_templates:
            yield event.plain_result(f"休息日模板 '{rest_template}' 不存在")
            return

        self.sets[set_name] = {
            "weekday": weekday_template,
            "rest": rest_template
        }
        yield event.plain_result(f"已创建 Set '{set_name}'")

    @set_group.command("use")
    async def set_use(self, event: AstrMessageEvent, set_name: str):
        """切换当前使用的 Set"""
        if set_name not in self.sets:
            yield event.plain_result(f"Set '{set_name}' 不存在")
            return

        self.config["active_set"] = set_name
        yield event.plain_result(f"已切换到 Set '{set_name}'")

    @set_group.command("delete")
    async def set_delete(self, event: AstrMessageEvent, set_name: str):
        """删除 Set"""
        if set_name not in self.sets:
            yield event.plain_result(f"Set '{set_name}' 不存在")
            return

        if set_name == self.active_set:
            yield event.plain_result("不能删除当前使用的 Set")
            return

        if len(self.sets) <= 1:
            yield event.plain_result("至少保留一个 Set")
            return

        del self.sets[set_name]
        yield event.plain_result(f"已删除 Set '{set_name}'")

    # ==================== 日历管理命令 ====================

    @filter.command_group("calendar")
    def calendar_group(self):
        """日历管理命令"""
        pass

    @calendar_group.command("list")
    async def calendar_list(self, event: AstrMessageEvent):
        """列出所有日历标记"""
        if not self.calendar:
            yield event.plain_result("暂无日历标记")
            return

        lines = ["=== 日历标记 ==="]
        for date_str, mark in sorted(self.calendar.items()):
            mark_type = "休息" if mark.get("type") == "rest" else "工作"
            template = mark.get("template", "?")
            note = mark.get("note", "")
            note_str = f" ({note})" if note else ""
            lines.append(f"  {date_str} - {mark_type} - {template}{note_str}")

        yield event.plain_result("\n".join(lines))

    @calendar_group.command("mark")
    async def calendar_mark(self, event: AstrMessageEvent, date_str: str, mark_type: str, template_name: str, note: str = ""):
        """标记特殊日期"""
        # 验证日期格式
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            yield event.plain_result("日期格式错误，请使用 YYYY-MM-DD")
            return

        # 验证类型
        if mark_type not in ["rest", "work"]:
            yield event.plain_result("类型必须为 rest 或 work")
            return

        # 验证模板存在
        if mark_type == "rest" and template_name not in self.rest_templates:
            yield event.plain_result(f"休息日模板 '{template_name}' 不存在")
            return
        if mark_type == "work" and template_name not in self.weekday_templates:
            yield event.plain_result(f"工作日模板 '{template_name}' 不存在")
            return

        self.calendar[date_str] = {
            "type": mark_type,
            "template": template_name,
            "note": note
        }
        yield event.plain_result(f"已标记 {date_str} 为 {mark_type} ({template_name})")

    @calendar_group.command("unmark")
    async def calendar_unmark(self, event: AstrMessageEvent, date_str: str):
        """取消日历标记"""
        if date_str in self.calendar:
            del self.calendar[date_str]
            yield event.plain_result(f"已取消 {date_str} 的标记")
        else:
            yield event.plain_result(f"{date_str} 没有标记")

    @calendar_group.command("show")
    async def calendar_show(self, event: AstrMessageEvent, year_month: str = ""):
        """显示图形化日历"""
        # 解析年月
        if not year_month:
            now = datetime.now()
        else:
            try:
                now = datetime.strptime(year_month + "-01", "%Y-%m-%d")
            except ValueError:
                yield event.plain_result("格式错误，请使用 YYYY-MM")
                return

        year = now.year
        month = now.month

        # 生成日历 HTML
        html = self._generate_calendar_html(year, month)

        try:
            url = await self.html_render(html, {})
            yield event.image_result(url)
        except Exception as e:
            logger.error(f"[定时问候] 日历渲染失败: {e}")
            yield event.plain_result(f"日历渲染失败: {e}")

    def _generate_calendar_html(self, year: int, month: int) -> str:
        """生成日历 HTML"""
        import calendar

        cal = calendar.monthcalendar(year, month)
        month_name = f"{year}年{month}月"

        # 生成日期单元格
        rows_html = ""
        for week in cal:
            cells = ""
            for day in week:
                if day == 0:
                    cells += "<td class='empty'></td>"
                else:
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    is_marked = date_str in self.calendar
                    mark_class = "marked" if is_marked else ""
                    mark_info = ""
                    if is_marked:
                        mark = self.calendar[date_str]
                        mark_info = f"<div class='mark-info'>{mark.get('note', '')}</div>"

                    cells += f"<td class='{mark_class}'><div class='day'>{day}</div>{mark_info}</td>"
            rows_html += f"<tr>{cells}</tr>"

        html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{
    font-family: 'Microsoft YaHei', sans-serif;
    background: #FFF8E7;
    padding: 20px;
    margin: 0;
  }}
  .calendar {{
    background: #FFFDF5;
    border-radius: 12px;
    box-shadow: 0 2px 10px rgba(200, 180, 100, 0.2);
    overflow: hidden;
    max-width: 400px;
    margin: 0 auto;
  }}
  .header {{
    background: #F5DEB3;
    color: #8B6914;
    padding: 15px;
    text-align: center;
    font-size: 20px;
    font-weight: bold;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
  }}
  th {{
    background: #FAEBD7;
    color: #8B6914;
    padding: 10px;
    font-weight: bold;
  }}
  td {{
    padding: 8px;
    text-align: center;
    border: 1px solid #F5DEB3;
    vertical-align: top;
    height: 50px;
  }}
  .empty {{
    background: #FFF8E7;
  }}
  .day {{
    font-size: 16px;
    color: #8B6914;
    font-weight: bold;
  }}
  .marked {{
    background: #FFE4B5;
    position: relative;
  }}
  .marked .day {{
    color: #D2691E;
  }}
  .mark-info {{
    font-size: 10px;
    color: #D2691E;
    margin-top: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }}
  .legend {{
    padding: 10px;
    text-align: center;
    font-size: 12px;
    color: #8B6914;
    background: #FAEBD7;
  }}
  .legend span {{
    display: inline-block;
    width: 12px;
    height: 12px;
    background: #FFE4B5;
    border: 1px solid #D2691E;
    vertical-align: middle;
    margin-right: 5px;
  }}
</style>
</head>
<body>
<div class="calendar">
  <div class="header">{month_name}</div>
  <table>
    <tr>
      <th>一</th>
      <th>二</th>
      <th>三</th>
      <th>四</th>
      <th>五</th>
      <th>六</th>
      <th>日</th>
    </tr>
    {rows_html}
  </table>
  <div class="legend">
    <span></span> 特殊标记日
  </div>
</div>
</body>
</html>
"""
        return html

    # ==================== 状态和测试命令 ====================

    @filter.command("get_umo")
    async def get_umo(self, event: AstrMessageEvent):
        """获取当前会话的 unified_msg_origin"""
        umo = event.unified_msg_origin
        platform = event.get_platform_name()
        sender = event.get_sender_name()
        sender_id = event.get_sender_id()

        msg = (
            f"当前会话信息：\n"
            f"平台: {platform}\n"
            f"发送者: {sender}\n"
            f"发送者 ID: {sender_id}\n"
            f"UMO: {umo}\n\n"
            f"请将 UMO 填入插件配置的 target_umo 字段"
        )
        yield event.plain_result(msg)

    @filter.command("test_greeting")
    async def test_greeting(self, event: AstrMessageEvent):
        """测试发送问候消息"""
        if not self.target_umo:
            yield event.plain_result("未配置目标用户 (target_umo)")
            return

        schedule = self._get_today_template()
        if not schedule:
            yield event.plain_result("当前时间表为空")
            return

        # 随机选择一个时间点的消息
        item = random.choice(schedule)
        if self.enable_ai and item.get("ai_prompt"):
            text = await self._generate_ai_message(item["ai_prompt"])
        else:
            text = item.get("message", "你好~")

        try:
            chain = MessageChain().message(text)
            await self.context.send_message(self.target_umo, chain)
            yield event.plain_result(f"测试消息已发送: {text}")
        except Exception as e:
            yield event.plain_result(f"发送失败: {e}")

    @filter.command("schedule_status")
    async def schedule_status(self, event: AstrMessageEvent):
        """查看当前状态"""
        today = datetime.now().strftime("%Y-%m-%d")
        weekday = datetime.now().weekday()

        # 判断今天使用的时间表
        if today in self.calendar:
            mark = self.calendar[today]
            day_type = "日历标记"
            template_name = mark.get("template", "?")
            note = mark.get("note", "")
        elif weekday < 5:
            day_type = "工作日"
            current_set = self.sets.get(self.active_set, {})
            template_name = current_set.get("weekday", "?")
            note = ""
        else:
            day_type = "休息日"
            current_set = self.sets.get(self.active_set, {})
            template_name = current_set.get("rest", "?")
            note = ""

        schedule = self._get_today_template()

        lines = [
            f"当前状态：",
            f"日期: {today} ({day_type})",
            f"使用的 Set: {self.active_set}",
            f"使用的模板: {template_name}",
        ]
        if note:
            lines.append(f"备注: {note}")

        lines.append(f"\n今日时间表：")
        for item in schedule:
            lines.append(f"  {item.get('time', '??:??')} - {item.get('message', '无消息')}")

        lines.append(f"\nAI 增强: {'开启' if self.enable_ai else '关闭'}")
        lines.append(f"目标 UMO: {self.target_umo or '未配置'}")

        yield event.plain_result("\n".join(lines))

    # ==================== 消息处理（处理交互式命令） ====================

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """处理所有消息（用于交互式命令）"""
        umo = event.unified_msg_origin
        message = event.message_str.strip()

        # 检查是否有待处理的交互
        if umo not in self._conversation_state:
            return

        state = self._conversation_state[umo]

        if state.get("command") == "tpl_ai_generate":
            if message == "完成" or message == "ok":
                # 完成收集，开始生成
                descriptions = state.get("descriptions", [])
                template_type = state.get("template_type", "weekday")

                if not descriptions:
                    yield event.plain_result("未收到任何描述，请重新开始")
                    del self._conversation_state[umo]
                    return

                yield event.plain_result("正在生成模板...")

                # 调用 AI 生成
                items = await self._generate_ai_template(template_type, descriptions)

                if not items:
                    yield event.plain_result("AI 生成失败，请稍后重试")
                    del self._conversation_state[umo]
                    return

                # 显示结果
                lines = ["AI 生成结果："]
                for item in items:
                    lines.append(f"  {item.get('time')} - {item.get('message')}")
                lines.append("\n回复 'y' 确认保存，'n' 取消")

                state["generated_items"] = items
                state["waiting_confirm"] = True

                yield event.plain_result("\n".join(lines))
            elif state.get("waiting_confirm"):
                if message.lower() == "y":
                    # 保存模板
                    template_type = state.get("template_type", "weekday")
                    items = state.get("generated_items", [])

                    # 生成模板名称
                    base_name = "工作日" if template_type == "weekday" else "休息日"
                    timestamp = datetime.now().strftime("%H%M%S")
                    template_name = f"{base_name}_AI_{timestamp}"

                    # 保存
                    if template_type == "weekday":
                        self.weekday_templates[template_name] = items
                    else:
                        self.rest_templates[template_name] = items

                    yield event.plain_result(f"已保存模板 '{template_name}'")
                    del self._conversation_state[umo]
                elif message.lower() == "n":
                    yield event.plain_result("已取消")
                    del self._conversation_state[umo]
            else:
                # 收集描述
                if " - " in message:
                    parts = message.split(" - ", 1)
                    time_str = parts[0].strip()
                    desc = parts[1].strip()
                    state["descriptions"].append(f"{time_str} - {desc}")
                    yield event.plain_result(f"已添加: {time_str} - {desc}")
                else:
                    yield event.plain_result("格式错误，请使用: 时间 - 大致意思")

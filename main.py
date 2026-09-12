import asyncio
import os
from datetime import datetime
from typing import List, Optional, Set, Tuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import At, Plain
from astrbot.api.star import Context, Star, StarTools

from .core.api_client import ApiClient
from .core.card_manager import CardManager, display_name, format_days, format_left
from .core.rules import RuleEngine


class AdminCardPlugin(Star):
    """管理体验卡插件主类。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_admin_card")
        os.makedirs(self.data_dir, exist_ok=True)
        self.scheduler = AsyncIOScheduler()
        self.api = ApiClient(self)
        self.cards = CardManager(self)
        self.rules = RuleEngine(self)
        self.privileged_users = {str(u) for u in (config.get("privileged_users") or [])}
        self._background_tasks: Set[asyncio.Task] = set()

    def _spawn(self, coro) -> asyncio.Task:
        """注册并跟踪后台协程任务，卸载时可统一取消。"""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    # ---------- 生命周期 ----------

    async def initialize(self) -> None:
        """插件初始化：加载数据、启动调度器、恢复定时任务。"""
        self.cards.load()
        self.rules.load()
        self.rules.build_rules()
        self.scheduler.start()
        self.cards.restore_schedules()
        self.rules.start_rotation_jobs()
        self.rules.schedule_level_scan()
        self._spawn(self.rules.cleanup_removed_rotations())
        self._spawn(self._bootstrap_flow())
        active_count = sum(
            1 for c in self.cards.cards.values() if c["status"] == "active"
        )
        rotation_count = sum(
            1 for group_rules in self.rules.rules.values() if "rotation" in group_rules
        )
        logger.debug(
            f"定时任务注册完成：到期回收 {active_count} 个，轮换 {rotation_count} 个，"
            f"等级扫描 {1 if self.rules.has_level_card_rules() else 0} 个"
        )

    async def _bootstrap_flow(self) -> None:
        """客户端绑定流程：立即尝试绑定，失败则 30 秒后补试。

        冷启动时平台实例尚未加载、热重载时 WS 可能未重连，立即绑定可能失败，
        因此失败后统一走 30 秒延迟补试，保证绑定与首次任务最终执行。
        """
        if await self.api.bind():
            await self._bootstrap()
            return
        await asyncio.sleep(30)
        if not await self.api.bind():
            logger.warning("未能获取平台客户端，定时任务将延后运行")
            return
        await self._bootstrap()

    async def _bootstrap(self) -> None:
        """客户端就绪后立即执行首次任务：过期卡补跑、首次轮换抽签、首次等级扫描。"""
        logger.debug("平台客户端已就绪")
        self._retry_expired_cards()
        await self.rules.run_first_rotations()
        if self.rules.has_level_card_rules():
            await self.rules.scan_levels()

    def _retry_expired_cards(self) -> None:
        """补跑重启时已过期的 active 卡下管（失败会自动安排重试）。"""
        for card in self.cards.cards.values():
            if card["status"] != "active" or not card.get("expires_at"):
                continue
            try:
                if datetime.fromisoformat(card["expires_at"]) <= datetime.now():
                    self._spawn(self.cards.handle_expire(card["id"]))
            except (TypeError, ValueError):
                continue

    async def terminate(self) -> None:
        """插件卸载时：取消后台任务、暂停并关闭调度器。"""
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()
        self.rules.cancel_background_tasks()
        try:
            if self.scheduler.running:
                self.scheduler.pause()
                self.scheduler.shutdown(wait=False)
        except Exception as e:
            logger.error(f"关闭调度器失败: {e}")

    # ---------- 内部工具 ----------

    def _is_allowed(self, event: AstrMessageEvent) -> bool:
        """发卡/撤卡/名单/跳过本轮权限：AstrBot 管理员或特权用户。"""
        if event.is_admin():
            return True
        return str(event.get_sender_id()) in self.privileged_users

    @staticmethod
    def _extract_at_targets(event: AstrMessageEvent) -> List[Tuple[str, str]]:
        """从消息链提取被 @ 的成员 (QQ, 昵称)，排除 @全体。"""
        targets = []
        for comp in event.get_messages():
            if isinstance(comp, At):
                qq = str(comp.qq)
                if qq.lower() != "all":
                    targets.append((qq, str(comp.name or "")))
        return targets

    @staticmethod
    def _sender_role_from_event(event: AstrMessageEvent) -> Optional[str]:
        """从原始 OneBot 事件读取发送者群角色（owner/admin/member）。"""
        try:
            raw = getattr(event.message_obj, "raw_message", None)
            if not raw:
                return None
            sender = raw.get("sender") or {}
            role = str(sender.get("role") or "").lower()
            return role if role in ("owner", "admin", "member") else None
        except Exception:
            return None

    # ---------- 命令 ----------

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def on_group_message(self, event: AstrMessageEvent):
        """群消息实时等级检测"""
        try:
            if not self.rules.has_level_card_rules():
                return
            raw = getattr(event.message_obj, "raw_message", None)
            if not raw:
                return
            sender = raw.get("sender") or {}
            uid = str(sender.get("user_id") or "")
            if not uid:
                return
            bot_id = await self.api.get_self_id(event.get_platform_id())
            if bot_id and uid == str(bot_id):
                return
            level = sender.get("level")
            nickname = str(sender.get("card") or sender.get("nickname") or "")
            await self.rules.check_level_trigger(
                str(event.get_group_id()), uid, level, nickname
            )
        except Exception as e:
            logger.error(f"等级实时检测异常: {e}")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("管理体验卡")
    async def grant_card(self, event: AstrMessageEvent, days: str = ""):
        """发放体验卡
        用法: /管理体验卡 [天数] @目标 [@目标 ...]
        """
        if not self._is_allowed(event):
            yield event.plain_result("你没有权限执行该操作")
            return
        gid = str(event.get_group_id())
        targets = self._extract_at_targets(event)
        if not targets:
            yield event.plain_result("请 @ 要发卡的用户，例如：/管理体验卡 3 @用户")
            return
        # 同一人 @ 多次时只发一张；过滤 bot 自身
        seen: set = set()
        unique_targets = []
        bot_id = await self.api.get_self_id(event.get_platform_id())
        for uid, name in targets:
            if uid in seen or (bot_id and uid == bot_id):
                continue
            seen.add(uid)
            unique_targets.append((uid, name))
        targets = unique_targets
        if not targets:
            yield event.plain_result("请 @ 要发卡的用户，例如：/管理体验卡 3 @用户")
            return

        days_val = 0.0
        try:
            parsed = float(days.strip())
            days_val = parsed if parsed > 0 else 0.0
        except ValueError:
            days_val = 0.0
        if days_val <= 0:
            gcfg = self.rules.get_group_config(gid)
            days_val = gcfg.get("default_card_days", 0) if gcfg else 0
        if days_val <= 0:
            yield event.plain_result("请指定体验天数，例如：/管理体验卡 3 @用户")
            return

        issued_names = []
        for uid, name in targets:
            self.cards.issue(
                gid, uid, days_val, "manual", event.get_platform_id(), name
            )
            issued_names.append(display_name(name, uid))
        yield event.plain_result(
            f"已向 {'、'.join(issued_names)} 发放 {days_val:g} 天管理体验卡，"
            "发送「使用体验卡」即可激活"
        )

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("撤销体验卡")
    async def revoke_card(self, event: AstrMessageEvent, num: str = ""):
        """撤销体验卡
        用法: /撤销体验卡 [序号|0] [@目标]
        （0 = 终止对方正在体验的管理期限，未使用卡保留；序号 = 撤销该组一张未使用卡；
        不写 = 全部撤销；不 @ 则操作自己）
        """
        if not self._is_allowed(event):
            yield event.plain_result("你没有权限执行该操作")
            return
        gid = str(event.get_group_id())
        targets = self._extract_at_targets(event)
        if targets:
            uid = targets[0][0]
            name = targets[0][1]
        else:
            uid = str(event.get_sender_id())
            name = event.get_sender_name()
        card_ids = None
        revoke_days = 0
        if num.strip() == "0":
            # 终止对方正在体验的管理期限（未使用卡保留）
            result = await self.cards.terminate_experience(gid, uid)
            if result["removed"] == 0:
                yield event.plain_result(
                    f"{display_name(name, uid)} 当前没有正在进行的体验"
                )
                return
            msg = f"已终止 {display_name(name, uid)} 的管理体验"
            if not result["demote_ok"]:
                if self.api.last_perm_error:
                    msg += "，但下管失败：Bot 不是群主，无权限操作"
                else:
                    msg += "，但下管失败，请稍后重试或手动处理"
            yield event.plain_result(msg)
            return
        if num.strip().isdigit():
            groups = self.cards.unused_groups(gid, uid)
            idx = int(num.strip())
            if not 1 <= idx <= len(groups):
                yield event.plain_result(
                    f"序号无效，{display_name(name, uid)} 当前共有 {len(groups)} 组未使用的体验卡"
                )
                return
            revoke_days = groups[idx - 1][0]
            card_ids = [groups[idx - 1][1][0]["id"]]

        result = await self.cards.revoke_user(gid, uid, card_ids)
        if result["removed"] == 0:
            yield event.plain_result(
                f"{display_name(name, uid)} 名下没有可撤销的体验卡"
            )
            return
        if revoke_days:
            msg = f"已撤销 {display_name(name, uid)} 的 {format_days(revoke_days)} 天体验卡"
        else:
            msg = f"已撤销 {display_name(name, uid)} 的 {result['removed']} 张体验卡"
        if not result["demote_ok"]:
            if self.api.last_perm_error:
                msg += "，但下管失败：Bot不是群主，无权限操作"
            else:
                msg += "，但下管失败，请稍后重试或手动处理"
        yield event.plain_result(msg)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("使用体验卡")
    async def use_card(self, event: AstrMessageEvent, num: str = ""):
        """激活体验卡成为群管理员
        用法: /使用体验卡 [序号]（不写序号则全部未使用的卡合并激活，时间累加）
        """
        gid = str(event.get_group_id())
        uid = str(event.get_sender_id())
        groups = self.cards.unused_groups(gid, uid)
        if not groups:
            yield event.plain_result("你名下没有可使用的体验卡")
            return

        if num.strip().isdigit():
            idx = int(num.strip())
            if not 1 <= idx <= len(groups):
                yield event.plain_result(
                    f"序号无效，你当前共有 {len(groups)} 组未使用的体验卡"
                )
                return
            selected = [groups[idx - 1][1][0]]
        else:
            selected = [c for _, group_cards in groups for c in group_cards]

        role = self._sender_role_from_event(event)
        if not role:
            info = await self.api.get_group_member_info(
                gid, uid, platform_id=event.get_platform_id()
            )
            role = str(info.get("role", "member"))

        if self.cards.has_active_card(gid, uid) and role != "admin":
            # 体验已被中断（被手动下管/身份被撤）：关闭旧卡，重新走激活流程
            self.cards.close_active_cards(gid, uid)

        if role == "admin" and not self.cards.has_active_card(gid, uid):
            yield event.plain_result("你已是群管理员，无需使用体验卡")
            return

        gcfg = self.rules.get_group_config(gid)
        max_concurrent = gcfg.get("max_concurrent", 0) if gcfg else 0
        if (
            max_concurrent > 0
            and not self.cards.has_active_card(gid, uid)
            and len(self.cards.active_cards(gid)) >= max_concurrent
        ):
            yield event.plain_result(
                f"当前同时体验人数已达上限（{max_concurrent} 人），请等待现有体验结束后再激活"
            )
            return

        # 仅在尚未体验时需要上管（合并激活已有 active 卡时无需重复上管）
        need_promote = not self.cards.has_active_card(gid, uid)
        if need_promote:
            ok = await self.api.set_group_admin(
                gid, uid, True, platform_id=event.get_platform_id()
            )
            if not ok:
                if self.api.last_perm_error:
                    yield event.plain_result(
                        "激活失败：Bot不是群主，无法设置或取消管理员"
                    )
                else:
                    yield event.plain_result("激活失败，请稍后再试，卡片仍可使用")
                return

        result = self.cards.activate_merged(gid, uid, selected)
        if result is None:
            if need_promote:
                # 上管成功但合并激活失败，回滚下管
                await self.api.set_group_admin(
                    gid, uid, False, platform_id=event.get_platform_id()
                )
            yield event.plain_result("激活失败，请稍后再试，卡片仍可使用")
            return
        expires = result["expires_at"].replace("T", " ")[:16]
        yield event.plain_result(
            f"激活成功！你已成为群管理员，体验至 {expires} 到期，到期自动回收"
        )

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("我的体验卡", alias={"查看体验卡"})
    async def my_cards(self, event: AstrMessageEvent):
        """查看体验卡
        用法: /我的体验卡（自己）；/查看体验卡 @目标（管理员/特权用户查看他人）
        """
        gid = str(event.get_group_id())
        targets = self._extract_at_targets(event)
        if targets:
            if not self._is_allowed(event):
                yield event.plain_result("你没有权限执行该操作")
                return
            uid = targets[0][0]
            name = targets[0][1]
            display = display_name(name, uid)
            header = f"{display} 的体验卡："
        else:
            uid = str(event.get_sender_id())
            display = "你"
            header = "你的体验卡："
        cards = self.cards.user_cards(gid, uid)
        lines = []
        has_active = False
        for c in cards:
            if c["status"] == "active":
                has_active = True
                lines.append(f"体验中：剩余 {format_left(c['expires_at'])}")
        groups = self.cards.unused_groups(gid, uid)
        if groups:
            if has_active:
                lines.append("未使用：")
            for i, (days, group_cards) in enumerate(groups, 1):
                lines.append(f"[{i}] {format_days(days)} 天 x{len(group_cards)}")
        if not lines:
            yield event.plain_result(f"{display} 名下没有体验卡")
            return
        yield event.plain_result(header + "\n" + "\n".join(lines))

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("体验卡名单")
    async def card_list(self, event: AstrMessageEvent):
        """查看本群体验卡名单
        用法: /体验卡名单
        """
        if not self._is_allowed(event):
            yield event.plain_result("你没有权限执行该操作")
            return
        gid = str(event.get_group_id())
        active = self.cards.active_cards(gid)
        lines = ["当前体验管理员："]
        if active:
            for c in active:
                lines.append(
                    f"- {display_name(c.get('nickname', ''), c['user_id'])} "
                    f"剩余 {format_left(c['expires_at'])}"
                )
        else:
            lines.append("- 无")
        # 未使用卡按用户聚合：共几张、共几天
        unused_by_user = {}
        for c in self.cards.cards.values():
            if c["group_id"] == gid and c["status"] == "unused":
                entry = unused_by_user.setdefault(
                    c["user_id"],
                    {"nickname": c.get("nickname", ""), "count": 0, "days": 0},
                )
                entry["count"] += 1
                entry["days"] += c["days"]
        lines.append("持有未使用卡的用户：")
        if unused_by_user:
            for uid, entry in unused_by_user.items():
                lines.append(
                    f"- {display_name(entry['nickname'], uid)} "
                    f"共 {entry['count']} 张（{format_days(entry['days'])} 天）"
                )
        else:
            lines.append("- 无")
        yield event.plain_result("体验卡名单\n" + "\n".join(lines))

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("抽体验卡")
    async def draw_card(self, event: AstrMessageEvent, days: str = ""):
        """从等级达标成员中随机抽取一人发放体验卡
        用法: /抽体验卡 [天数]（不写天数则使用规则默认时长）
        """
        if not self._is_allowed(event):
            yield event.plain_result("你没有权限执行该操作")
            return
        gid = str(event.get_group_id())
        days_val = 0.0
        try:
            parsed = float(days.strip())
            days_val = parsed if parsed > 0 else 0.0
        except ValueError:
            days_val = 0.0
        ok, msg, winner = await self.rules.draw_card(gid, days_val)
        if not ok or not winner:
            yield event.plain_result(msg)
            return
        chain = [At(qq=winner)]
        chain.append(Plain(f" {msg}"))
        yield event.chain_result(chain)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("查看本轮")
    async def view_round(self, event: AstrMessageEvent):
        """查看当前轮换周期状态：有没有人轮值、距离下次轮换还剩多久
        用法: /查看本轮
        """
        gid = str(event.get_group_id())
        rot = self.rules.rotations.get(gid)
        if not rot:
            yield event.plain_result("该群未配置轮换规则")
            return
        current_uids = [str(uid) for uid in (rot.get("current_uids") or [])]
        member_infos = await asyncio.gather(
            *(
                self.api.get_group_member_info(
                    gid, uid, platform_id=event.get_platform_id()
                )
                for uid in current_uids
            )
        )
        names = {
            uid: str(info.get("card") or info.get("nickname") or "")
            for uid, info in zip(current_uids, member_infos)
        }
        lines = ["本轮状态"]
        if current_uids:
            parts = [
                display_name(names.get(uid, ""), uid) for uid in current_uids
            ]
            lines.append(f"当前轮值：{'、'.join(parts)}")
        else:
            lines.append("当前无人轮值")
        if rot.get("cycle_end"):
            lines.append(f"距离下次轮换：{format_left(rot['cycle_end'])}")
        yield event.plain_result("\n".join(lines))

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("跳过本轮")
    async def skip_round(self, event: AstrMessageEvent):
        """跳过当前轮换周期
        用法: /跳过本轮
        """
        if not self._is_allowed(event):
            yield event.plain_result("你没有权限执行该操作")
            return
        gid = str(event.get_group_id())
        ok, msg, demoted = await self.rules.skip_rotation(gid)
        if not ok:
            yield event.plain_result(msg)
            return
        if demoted:
            chain = []
            for uid in demoted:
                chain.append(At(qq=uid))
            chain.append(Plain(" 本轮轮换已被管理员跳过\n"))
            chain.append(Plain(msg))
            yield event.chain_result(chain)
        else:
            yield event.plain_result(msg)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.command("重抽本轮")
    async def reroll_round(self, event: AstrMessageEvent):
        """立即重新抽签本轮轮换（下掉现任并重新抽人，周期重新计时）
        用法: /重抽本轮
        """
        if not self._is_allowed(event):
            yield event.plain_result("你没有权限执行该操作")
            return
        gid = str(event.get_group_id())
        ok, msg = await self.rules.run_rotation(gid)
        if ok:
            yield event.plain_result("已重抽本轮，新一轮轮换已开始")
        else:
            yield event.plain_result(msg)

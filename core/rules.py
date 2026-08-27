"""规则引擎：等级达标自动发卡 + 周期随机轮换管理员。"""

import asyncio
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from astrbot.api import logger

from .card_manager import format_days, parse_dt

VALID_RULE_TYPES = ("level_card", "rotation", "draw_card")


def parse_level(raw) -> Optional[int]:
    """解析群等级字段（可能为空、纯数字或含前缀，如 'LV.12'）。"""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    match = re.search(r"\d+", s)
    if not match:
        return None
    try:
        return int(match.group())
    except ValueError:
        return None


async def level_scan_job(plugin) -> None:
    """等级达标扫描定时任务（module-level，供 apscheduler 调用）。"""
    await plugin.rules.scan_levels()


async def rotation_job(plugin, group_id: str) -> None:
    """轮换触发定时任务（module-level，供 apscheduler 调用）。"""
    await plugin.rules.run_rotation(str(group_id))


def _to_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class RuleEngine:
    """按群规则：配置解析、等级检测、轮换抽签。"""

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        # 群规则表：{gid: {rule_type: rule}}，同一群不同类型规则可并存
        self.rules: Dict[str, Dict[str, Dict]] = {}
        # 群级配置：{gid: {max_concurrent, blacklist, default_card_days}}
        self.group_configs: Dict[str, Dict] = {}
        self.level_track: Dict[str, Dict[str, int]] = {}
        self.rotations: Dict[str, Dict] = {}
        self._level_warned: Set[Tuple[str, str]] = set()
        self._rotation_locks: Dict[str, asyncio.Lock] = {}
        self._background_tasks: Set[asyncio.Task] = set()

    def _spawn_background(self, coro) -> asyncio.Task:
        """注册并跟踪一个后台协程任务，卸载时可统一取消。"""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def cancel_background_tasks(self) -> None:
        """取消全部未完成的后台任务（插件卸载时调用）。"""
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()

    def _get_rotation_lock(self, gid: str) -> asyncio.Lock:
        """每群一把互斥锁，防止定时 job 与手动指令并发执行轮换。"""
        lock = self._rotation_locks.get(gid)
        if lock is None:
            lock = asyncio.Lock()
            self._rotation_locks[gid] = lock
        return lock

    # ---------- 配置与数据 ----------

    def load(self) -> None:
        """从共享 data.json 载入轮换与等级追踪状态。"""
        self.level_track = self.plugin.cards.data.setdefault("level_track", {})
        self.rotations = self.plugin.cards.data.setdefault("rotations", {})

    def build_rules(self) -> None:
        """从插件配置构建群规则表与群级配置。

        group 模板为群级配置（max_concurrent/blacklist/default_card_days），
        level_card 与 rotation 为规则；同群同类型多条仅取第一条，不同类型
        可并存。群号留空跳过。
        """
        raw_list = self.plugin.config.get("rules") or []
        if not isinstance(raw_list, list):
            raw_list = []
        seen: Dict[str, Dict[str, Dict]] = {}
        group_configs: Dict[str, Dict] = {}
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            gid = str(item.get("group_id") or "").strip()
            if not gid:
                continue
            template_key = str(item.get("__template_key") or "").strip()
            if template_key == "group":
                if gid in group_configs:
                    logger.warning(f"群 {gid} 配置了多条群级配置，仅使用第一条")
                    continue
                group_configs[gid] = self._normalize_group_config(item)
                continue
            rule = self._normalize_rule(item)
            if not rule:
                continue
            rule_type = rule["rule_type"]
            if rule_type == "rotation" and rule["cycle_days"] <= 0:
                logger.warning(f"群 {gid} 轮换周期必须大于 0，已调整为 1 天")
                rule["cycle_days"] = 1
            elif rule_type == "rotation" and rule["cycle_days"] < 0.01:
                logger.warning(f"群 {gid} 轮换周期过小，已调整为 0.01 天（约 15 分钟）")
                rule["cycle_days"] = 0.01
            if rule_type == "level_card" and any(d <= 0 for d in rule["card_days"]):
                logger.warning(f"群 {gid} 体验卡时长必须大于 0，已调整为 1 天")
                rule["card_days"] = [max(d, 1) for d in rule["card_days"]]
            if rule_type == "draw_card" and rule["card_days"] <= 0:
                logger.warning(f"群 {gid} 抽卡体验卡时长必须大于 0，已调整为 1 天")
                rule["card_days"] = 1
            group_rules = seen.setdefault(gid, {})
            if rule_type in group_rules:
                logger.warning(f"群 {gid} 配置了多条 {rule_type} 规则，仅使用第一条")
                continue
            group_rules[rule_type] = rule
        self.rules = seen
        self.group_configs = group_configs

    @staticmethod
    def _normalize_group_config(item: Dict) -> Dict:
        """解析群级配置：max_concurrent / blacklist / default_card_days。"""
        return {
            "max_concurrent": _to_int(item.get("max_concurrent"), 0),
            "blacklist": {str(b) for b in (item.get("blacklist") or [])},
            "default_card_days": _to_float(item.get("default_card_days"), 0),
        }

    def get_group_config(self, group_id) -> Optional[Dict]:
        """获取指定群的群级配置（无则返回 None）。"""
        return self.group_configs.get(str(group_id))

    def get_group_blacklist(self, group_id) -> Set[str]:
        """获取指定群的群级黑名单（无群级配置时为空集）。"""
        cfg = self.group_configs.get(str(group_id))
        return cfg.get("blacklist", set()) if cfg else set()

    def _normalize_rule(self, item: Dict) -> Optional[Dict]:
        rule_type = str(item.get("__template_key") or "").strip()
        if rule_type not in VALID_RULE_TYPES:
            logger.warning(f"未知规则类型 {rule_type}，已跳过该规则")
            return None
        rule = {
            "rule_type": rule_type,
            "card_days": _to_float(item.get("card_days"), 0),
            "cycle_days": _to_float(item.get("cycle_days"), 0),
            "admin_count": _to_int(item.get("admin_count"), 1),
            "level_weighted": bool(item.get("level_weighted", False)),
            "round_robin": bool(item.get("round_robin", False)),
        }
        if rule_type == "level_card":
            min_levels = self._parse_level_thresholds(item.get("min_level"))
            rule["min_level"] = min_levels
            # 时长与等级门槛一一对应：每个门槛发对应时长的卡
            rule["card_days"] = self._parse_card_days(
                item.get("card_days"), len(min_levels)
            )
        else:
            rule["min_level"] = _to_int(item.get("min_level"), 0)
            rule["card_days"] = _to_float(item.get("card_days"), 0)
        return rule

    @staticmethod
    def _parse_card_days(value, size: int, default: float = 3.0) -> List[float]:
        """解析等级发卡时长：支持 list（与等级门槛一一对应）或单个数字（所有门槛同值）。

        list 不足门槛数时补默认值，多余截断。
        """
        if isinstance(value, (list, tuple)):
            days = [_to_float(v, default) for v in value]
        else:
            single = _to_float(value, default)
            days = [single] * size
        if not days:
            days = [default]
        return (days + [default] * size)[:size]

    @staticmethod
    def _parse_level_thresholds(value) -> List[int]:
        """解析多个等级门槛（支持 list 或单个 int），返回升序去重列表。"""
        if isinstance(value, (list, tuple, set)):
            thresholds = {_to_int(v, 0) for v in value}
        else:
            thresholds = {_to_int(value, 0)}
        return sorted(t for t in thresholds if t >= 0)

    def get_rule(self, group_id, rule_type: Optional[str] = None) -> Optional[Dict]:
        """获取指定群的规则。

        Args:
            group_id: 群号。
            rule_type: 规则类型（level_card/rotation）；为空时返回该群全部规则表。
        """
        group_rules = self.rules.get(str(group_id))
        if not group_rules:
            return None
        if rule_type is None:
            return group_rules
        return group_rules.get(rule_type)

    def has_level_card_rules(self) -> bool:
        """是否存在等级达标发卡规则。"""
        return any("level_card" in grp for grp in self.rules.values())

    # ---------- 等级达标发卡 ----------

    def schedule_level_scan(self) -> None:
        """注册等级达标扫描任务（每天 22:00 兜底，实时检测见消息监听），无 level_card 规则时不注册。"""
        if not self.has_level_card_rules():
            return
        self.plugin.scheduler.add_job(
            level_scan_job,
            trigger=CronTrigger.from_crontab("0 22 * * *"),
            id="level_scan",
            args=[self.plugin],
            max_instances=1,
        )

    async def scan_levels(self) -> None:
        """扫描所有 level_card 规则群，达标自动发卡并通知。"""
        for gid, group_rules in self.rules.items():
            rule = group_rules.get("level_card")
            if not rule:
                continue
            try:
                await self._scan_group(gid, rule)
            except Exception as e:
                logger.error(f"群 {gid} 等级扫描异常: {e}")
        self.plugin.cards.save()

    async def _scan_group(self, gid: str, rule: Dict) -> None:
        pid = None
        members = await self.plugin.api.get_group_member_list(gid, platform_id=pid)
        if not members:
            logger.warning(f"群 {gid} 等级扫描：无法获取成员列表，跳过本轮")
            return
        group_blacklist = self.get_group_blacklist(gid)

        track = self.level_track.setdefault(gid, {})
        thresholds = rule["min_level"]
        bot_id = await self.plugin.api.get_self_id(pid)
        current_uids: Set[str] = set()
        # 门槛组合 -> [(uid, nickname)]：同一次扫描中达到同一组门槛的成员合并到一条通知
        reached: Dict[Tuple[int, ...], List[Tuple[str, str]]] = {}

        for member in members:
            uid = str(member.get("user_id") or "")
            if not uid:
                continue
            current_uids.add(uid)
            if bot_id and uid == str(bot_id):
                continue
            if uid in group_blacklist:
                continue
            nickname = str(member.get("card") or member.get("nickname") or "")

            level = parse_level(member.get("level"))
            if level is None:
                if (gid, uid) not in self._level_warned:
                    self._level_warned.add((gid, uid))
                    logger.warning(f"群 {gid} 成员 {uid} 无群等级数据，跳过等级检测")
                continue
            new_thresholds = [
                t for t in thresholds if level >= t and track.get(uid, -1) < t
            ]
            for t in new_thresholds:
                # 每个门槛各领一张卡，时长与门槛一一对应
                idx = thresholds.index(t)
                days = (
                    rule["card_days"][idx]
                    if idx < len(rule["card_days"])
                    else rule["card_days"][-1]
                )
                self.plugin.cards.issue(gid, uid, days, "level", pid, nickname)
                track[uid] = t
            if new_thresholds:
                # track 只记录"已触发的门槛"，不记录未达标时的普通等级，
                # 否则下调门槛后历史达标成员会被旧 track 挡住而无法补发
                reached.setdefault(tuple(new_thresholds), []).append((uid, nickname))

        for ths_tuple, users in sorted(reached.items()):
            segments = self._build_level_notice(rule, ths_tuple, users)
            await self.plugin.api.send_group_segments(gid, segments, platform_id=pid)
        # 清理已退群/不在成员列表中的旧 track 记录，避免误判历史门槛
        for uid in [u for u in track if u not in current_uids]:
            del track[uid]
        self._level_warned = {
            (g, u) for (g, u) in self._level_warned if g != gid or u in current_uids
        }

    @staticmethod
    def _build_level_notice(
        rule: Dict, ths_tuple: Tuple[int, ...], users: List[Tuple[str, str]]
    ) -> List[Dict]:
        """构造等级达标的群通知段列表（@ 穿插在文案中间）。"""
        thresholds = rule["min_level"]
        segments = [{"type": "text", "data": {"text": "恭喜 "}}]
        segments.extend({"type": "at", "data": {"qq": uid}} for uid, _ in users)
        day_text = "、".join(
            f"{format_days(rule['card_days'][thresholds.index(t)])}" for t in ths_tuple
        )
        card_text = f"获得 {day_text} 天管理体验卡"
        if len(ths_tuple) > 1:
            card_text += f"（{len(ths_tuple)} 张）"
        segments.append(
            {
                "type": "text",
                "data": {
                    "text": (
                        f" 达到群等级 {'、'.join(str(t) for t in ths_tuple)} 级，"
                        f"{card_text}，发送「使用体验卡」激活"
                    )
                },
            }
        )
        return segments

    async def check_level_trigger(
        self, gid: str, uid: str, level, nickname: str = ""
    ) -> bool:
        """事件驱动单人等级检测：达标立即发卡并通知（返回是否触发）。

        由群消息监听调用，消息事件自带发送者等级，无需额外接口请求。
        """
        group_rules = self.rules.get(gid)
        rule = group_rules.get("level_card") if group_rules else None
        if not rule or not uid:
            return False
        if uid in self.get_group_blacklist(gid):
            return False
        level = parse_level(level)
        if level is None:
            return False
        track = self.level_track.setdefault(gid, {})
        thresholds = rule["min_level"]
        new_thresholds = [
            t for t in thresholds if level >= t and track.get(uid, -1) < t
        ]
        if not new_thresholds:
            return False
        for t in new_thresholds:
            # 每个门槛各领一张卡，时长与门槛一一对应
            idx = thresholds.index(t)
            days = (
                rule["card_days"][idx]
                if idx < len(rule["card_days"])
                else rule["card_days"][-1]
            )
            self.plugin.cards.issue(gid, uid, days, "level", None, nickname)
            track[uid] = t
        self.plugin.cards.save()
        segments = self._build_level_notice(
            rule, tuple(new_thresholds), [(uid, nickname)]
        )
        await self.plugin.api.send_group_segments(gid, segments, platform_id=None)
        return True

    # ---------- 周期轮换 ----------

    @staticmethod
    def _rule_fingerprint(rule: Dict) -> str:
        """轮换抽取相关参数的指纹，用于检测配置变更。

        黑名单为群级配置，不参与指纹（变化不重置轮次记录，抽签时按新黑名单处理）。
        """
        parts = [
            str(rule["min_level"]),
            str(rule["admin_count"]),
            str(rule["level_weighted"]),
            str(rule["round_robin"]),
        ]
        return "|".join(parts)

    # ---------- 抽体验卡 ----------

    async def draw_card(
        self, group_id, days: float = 0
    ) -> Tuple[bool, str, Optional[str]]:
        """抽体验卡：从等级达标成员中随机抽取一人发放体验卡。

        Args:
            group_id: 群号。
            days: 手动指定的体验卡时长；小于等于 0 时使用规则默认时长。

        Returns:
            (是否成功, 结果文案, 被抽中者 QQ；失败时 QQ 为 None)。
        """
        gid = str(group_id)
        rule = self.rules.get(gid, {}).get("draw_card")
        if not rule:
            return False, "该群未配置抽卡规则", None
        members = await self.plugin.api.get_group_member_list(gid, platform_id=None)
        if not members:
            return False, "获取成员列表失败，请稍后再试", None
        bot_id = await self.plugin.api.get_self_id(None)
        blacklist = self.get_group_blacklist(gid)
        active_uids = {c["user_id"] for c in self.plugin.cards.active_cards(gid)}
        current_uids = set(self.rotations.get(gid, {}).get("current_uids") or [])
        candidates = []
        for member in members:
            uid = str(member.get("user_id") or "")
            if not uid:
                continue
            if bot_id and uid == str(bot_id):
                continue
            if uid in blacklist:
                continue
            # 排除非临时的真管理员：role 为 owner/admin 且不在我们的
            # 体验中（active 卡）/轮值中（current_uids）记录里；
            # 临时管理员可参与抽卡（抽到的是体验卡，可攒着使用）
            if (
                str(member.get("role") or "") in ("owner", "admin")
                and uid not in active_uids
                and uid not in current_uids
            ):
                continue
            level = parse_level(member.get("level"))
            if level is None or level < rule["min_level"]:
                continue
            nickname = str(member.get("card") or member.get("nickname") or "")
            candidates.append((uid, level, nickname))
        if not candidates:
            return False, "当前没有符合条件的成员可抽取", None

        if rule["level_weighted"]:
            weights = [max(level, 1) for _, level, _ in candidates]
            uid, _, nickname = random.choices(candidates, weights=weights, k=1)[0]
        else:
            uid, _, nickname = random.choice(candidates)

        card_days = days if days > 0 else rule["card_days"]
        self.plugin.cards.issue(gid, uid, card_days, "draw", "", nickname)
        return (
            True,
            f"恭喜被抽中，获得 {card_days:g} 天管理体验卡，发送「使用体验卡」激活",
            uid,
        )

    def start_rotation_jobs(self) -> None:
        """启动各轮换群的周期任务：首次载入建周期并标记待抽签，恢复未到期周期，补跑已到期周期。"""
        for gid, group_rules in self.rules.items():
            rule = group_rules.get("rotation")
            if not rule:
                continue
            rot = self.rotations.get(gid)
            fp = self._rule_fingerprint(rule)
            if not rot or not rot.get("cycle_end"):
                # 首次载入：先建周期，等客户端就绪后立即抽签（见 run_first_rotations）
                self._begin_cycle(gid, rule)
                self.rotations[gid]["first_round"] = True
                self.rotations[gid]["rule_fp"] = fp
                self.plugin.cards.save()
                continue
            if rot.get("rule_fp") not in (None, fp):
                # 抽取相关配置（门槛/人数/加权/轮次/黑名单）变更 → 轮次记录重置
                logger.info(f"群 {gid} 轮换抽取配置已变更，重置轮次抽取记录")
                rot["round_picked"] = []
            rot["rule_fp"] = fp
            cycle_end = parse_dt(rot["cycle_end"])
            if cycle_end > datetime.now():
                self._schedule_rotation(gid, cycle_end)
            else:
                # 首次抽签前的标记群由 run_first_rotations 唯一负责，
                # 避免"补跑 + 立即执行"双路径导致轮换重复
                if rot.get("first_round"):
                    continue
                self._spawn_background(self.run_rotation(gid))

    async def run_first_rotations(self) -> None:
        """客户端就绪后，为首次载入的轮换群立即执行首次抽签。"""
        for gid, group_rules in self.rules.items():
            if not group_rules.get("rotation"):
                continue
            rot = self.rotations.get(gid)
            if not rot or not rot.get("first_round"):
                continue
            try:
                await self.run_rotation(gid)
            except Exception as e:
                logger.error(f"群 {gid} 首次轮换抽签异常: {e}")

    async def cleanup_removed_rotations(self) -> None:
        """清理已删除/更换的轮换规则群：下掉在任轮换管理员并移除记录。"""
        for gid in list(self.rotations.keys()):
            if self.rules.get(gid, {}).get("rotation"):
                continue
            rot = self.rotations[gid]
            pid = rot.get("platform_id") or None
            for old_uid in list(rot.get("current_uids") or []):
                ok = await self.plugin.api.set_group_admin(
                    gid, old_uid, False, platform_id=pid
                )
                if ok:
                    logger.info(f"群 {gid} 轮换规则已删除，下掉成员 {old_uid}")
                else:
                    logger.error(
                        f"群 {gid} 轮换规则已删除但下管 {old_uid} 失败，请人工处理"
                    )
            del self.rotations[gid]
        self.plugin.cards.save()

    def _begin_cycle(self, gid: str, rule: Dict) -> None:
        """开启/顺延一个新周期并注册触发任务。"""
        now = datetime.now()
        rot = self.rotations.get(gid)
        if not rot:
            rot = {
                "current_uids": [],
                "cycle_start": None,
                "cycle_end": None,
                "round_picked": [],
                "platform_id": "",
            }
            self.rotations[gid] = rot
        rot["cycle_start"] = now.isoformat(timespec="seconds")
        rot["cycle_end"] = (now + timedelta(days=rule["cycle_days"])).isoformat(
            timespec="seconds"
        )
        rot.pop("first_round", None)
        self._schedule_rotation(gid, parse_dt(rot["cycle_end"]))

    def _schedule_rotation(self, gid: str, run_date: datetime) -> None:
        try:
            self.plugin.scheduler.remove_job(f"rotation_{gid}")
        except Exception:
            pass
        self.plugin.scheduler.add_job(
            rotation_job,
            trigger=DateTrigger(run_date=run_date),
            id=f"rotation_{gid}",
            args=[self.plugin, gid],
            max_instances=1,
        )

    async def run_rotation(self, gid: str) -> Tuple[bool, str]:
        """周期到期/重抽：下掉现任 -> 抽签 -> 上任 -> 顺延下一周期。

        Returns:
            (是否成功, 结果文案)。定时触发时忽略返回值。
        """
        gid = str(gid)
        async with self._get_rotation_lock(gid):
            return await self._run_rotation_locked(gid)

    async def _run_rotation_locked(self, gid: str) -> Tuple[bool, str]:
        """加锁后的轮换执行体（防定时 job 与启动补跑并发重复执行）。"""
        rule = self.rules.get(gid, {}).get("rotation")
        if not rule:
            logger.warning(f"群 {gid} 无轮换规则，跳过轮换")
            return False, "该群未配置轮换规则"
        rot = self.rotations.get(gid)
        if not rot:
            rot = {
                "current_uids": [],
                "cycle_start": None,
                "cycle_end": None,
                "round_picked": [],
                "platform_id": "",
            }
            self.rotations[gid] = rot
        pid = rot.get("platform_id") or None

        # 1. 下掉现任（下管失败的保留记录，等待下一周期再试）
        old_uids = list(rot.get("current_uids") or [])
        demoted_old: List[str] = []
        for old_uid in old_uids:
            ok = await self.plugin.api.set_group_admin(
                gid, old_uid, False, platform_id=pid
            )
            if ok:
                demoted_old.append(old_uid)
            else:
                logger.error(
                    f"群 {gid} 成员 {old_uid} 轮换下台失败，保留记录等待后续处理"
                )
        rot["current_uids"] = [u for u in old_uids if u not in demoted_old]

        # 2. 抽签（轮次抽取开启时排除所有本圈已抽中者；关闭时上一任可连任）
        count = max(rule.get("admin_count") or 1, 1)
        winners = await self._pick(gid, rule, rot, pid, count)

        # 3. 上任 + 顺延下一周期
        self._begin_cycle(gid, rule)
        self.plugin.cards.save()
        if not winners:
            logger.debug(f"群 {gid} 本轮无合格候选人，周期已顺延")
            return False, "本轮无合格候选人，周期已顺延"
        promoted: List[str] = []
        for winner in winners:
            ok = await self.plugin.api.set_group_admin(
                gid, winner, True, platform_id=pid
            )
            if not ok:
                logger.error(f"群 {gid} 抽中 {winner} 但上任失败")
                continue
            promoted.append(winner)
        rot["current_uids"] = [u for u in old_uids if u not in demoted_old] + promoted
        self.plugin.cards.save()
        if not promoted:
            logger.error(f"群 {gid} 本轮全部上任失败，周期已空置")
            return

        # 4. 卸任 + 上任合并为一条通知（@ 穿插在文案中）
        segments = []
        if demoted_old:
            segments.extend({"type": "at", "data": {"qq": uid}} for uid in demoted_old)
            possessive = "你的" if len(demoted_old) == 1 else "你们的"
            segments.append(
                {
                    "type": "text",
                    "data": {"text": f" {possessive}管理员轮值已结束；恭喜 "},
                }
            )
        else:
            segments.append({"type": "text", "data": {"text": "恭喜 "}})
        segments.extend({"type": "at", "data": {"qq": uid}} for uid in promoted)
        segments.append(
            {
                "type": "text",
                "data": {"text": f" 成为本轮体验管理员，任期 {format_days(rule['cycle_days'])} 天"},
            }
        )
        await self.plugin.api.send_group_segments(gid, segments, platform_id=pid)
        return True, "轮换已执行"

    async def _pick(
        self, gid: str, rule: Dict, rot: Dict, pid: str, count: int = 1
    ) -> List[str]:
        """从合格人群中抽签，返回中选者 QQ 列表。"""
        members = await self.plugin.api.get_group_member_list(gid, platform_id=pid)
        if not members:
            return []
        bot_id = await self.plugin.api.get_self_id(pid)
        # 校验 Bot 是否为群主：不是则跳过本轮抽签（设/撤管理员必定失败）
        if bot_id:
            bot_member = next(
                (m for m in members if str(m.get("user_id") or "") == str(bot_id)),
                None,
            )
            if bot_member is None or str(bot_member.get("role") or "") != "owner":
                logger.warning(f"群 {gid} Bot 不是群主，跳过轮换抽签")
                return []
        round_picked: Set[str] = set(rot.get("round_picked") or [])
        qualified: List[Tuple[str, int]] = []
        all_qualified: Set[str] = set()
        group_blacklist = self.get_group_blacklist(gid)
        active_uids = {c["user_id"] for c in self.plugin.cards.active_cards(gid)}
        current_uids = set(rot.get("current_uids") or [])

        for member in members:
            uid = str(member.get("user_id") or "")
            if not uid:
                continue
            if bot_id and uid == str(bot_id):
                continue
            if uid in group_blacklist:
                continue
            # 体验卡激活中的人已在当管理员，轮换抽中无意义（浪费体验名额），排除
            if uid in active_uids:
                continue
            # 排除非临时的真管理员：role 为 owner/admin 且不在我们的
            # 轮值中（current_uids）记录里；轮值中者可参与轮换抽签（可连任）
            if (
                str(member.get("role") or "") in ("owner", "admin")
                and uid not in current_uids
            ):
                continue
            level = parse_level(member.get("level"))
            if level is None or level < rule["min_level"]:
                continue
            all_qualified.add(uid)
            # 轮次抽取排除本圈所有已抽中者（含上一任）；关闭时无此限制，可连任
            if rule["round_robin"] and uid in round_picked:
                continue
            qualified.append((uid, level))

        if not qualified:
            return []

        pool = qualified
        winners: List[str] = []
        for _ in range(max(count, 1)):
            if not pool:
                break
            if rule["level_weighted"]:
                weights = [max(level, 1) for _, level in pool]
                winner = random.choices([uid for uid, _ in pool], weights=weights, k=1)[
                    0
                ]
            else:
                winner = random.choice(pool)[0]
            winners.append(winner)
            pool = [p for p in pool if p[0] != winner]

        if rule["round_robin"]:
            # 清理已退群/降级/进黑名单的成员，避免候选池被残旧记录越挤越小
            round_picked &= all_qualified
            for winner in winners:
                round_picked.add(winner)
            if all_qualified and round_picked >= all_qualified:
                round_picked.clear()
            rot["round_picked"] = sorted(round_picked)
        return winners

    async def skip_rotation(self, group_id) -> Tuple[bool, str, List[str]]:
        """跳过本周期：下掉现任、周期顺延。

        Returns:
            (是否成功, 提示文案, 被下管的卸任者 QQ 列表)。
        """
        gid = str(group_id)
        async with self._get_rotation_lock(gid):
            return await self._skip_rotation_locked(gid)

    async def _skip_rotation_locked(self, gid: str) -> Tuple[bool, str, List[str]]:
        rule = self.rules.get(gid, {}).get("rotation")
        if not rule:
            return False, "该群未配置轮换规则", []
        rot = self.rotations.get(gid)
        pid = (rot or {}).get("platform_id") or None
        demoted_old: List[str] = []
        if rot:
            old_uids = list(rot.get("current_uids") or [])
            for old_uid in old_uids:
                ok = await self.plugin.api.set_group_admin(
                    gid, old_uid, False, platform_id=pid
                )
                if ok:
                    demoted_old.append(old_uid)
                else:
                    logger.error(
                        f"群 {gid} 成员 {old_uid} 轮换下台失败，保留记录等待后续处理"
                    )
            rot["current_uids"] = [u for u in old_uids if u not in demoted_old]
        self._begin_cycle(gid, rule)
        self.plugin.cards.save()
        return True, "已跳过本轮，下一周期已顺延", demoted_old

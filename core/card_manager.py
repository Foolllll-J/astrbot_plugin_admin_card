"""体验卡状态机、持久化与到期定时调度。

数据统一保存在 data.json（cards / rotations / level_track），
rotations 与 level_track 由 rules 模块读写同一份 data。
"""

import json
import os
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.date import DateTrigger

from astrbot.api import logger

STATUS_UNUSED = "unused"
STATUS_ACTIVE = "active"
STATUS_EXPIRED = "expired"
STATUS_CANCELLED = "cancelled"
STATUS_CONSUMED = "consumed"

MAX_EXPIRE_RETRIES = 12
"""到期下管最大重试次数（每次间隔 30 分钟，约 6 小时），之后放弃并请人工处理。"""


def parse_dt(value: str) -> datetime:
    """解析 ISO 格式时间字符串。"""
    return datetime.fromisoformat(value)


def format_left(expires_at: str) -> str:
    """计算距离过期时间的剩余时长文本。"""
    left = parse_dt(expires_at) - datetime.now()
    total_seconds = max(int(left.total_seconds()), 0)
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days > 0:
        return f"{days} 天 {hours} 小时"
    if hours > 0:
        return f"{hours} 小时 {minutes} 分钟"
    return f"{minutes} 分钟"


def display_name(nickname: str, user_id) -> str:
    """组装展示名：有昵称时返回 昵称(uid)，否则仅 uid。"""
    nickname = (nickname or "").strip()
    if not nickname:
        return str(user_id)
    return f"{nickname}({user_id})"


def format_days(days) -> str:
    """天数文案：整数天数去掉小数（1.0 -> 1），非整数保留（0.3 / 1.5）。"""
    value = float(days)
    if value == int(value):
        return str(int(value))
    return f"{value:g}"


async def expire_card_job(plugin, card_id: str) -> None:
    """到期下管定时任务（module-level，供 apscheduler 调用）。"""
    await plugin.cards.handle_expire(card_id)


class CardManager:
    """体验卡数据管理与状态流转。"""

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        self.data_file = os.path.join(plugin.data_dir, "data.json")
        self.data: Dict[str, Any] = {"cards": {}, "rotations": {}, "level_track": {}}
        self.cards: Dict[str, Dict] = self.data["cards"]

    def load(self) -> None:
        """从文件加载全部数据。"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                self.data = {
                    "cards": loaded.get("cards") or {},
                    "rotations": loaded.get("rotations") or {},
                    "level_track": loaded.get("level_track") or {},
                }
                self.cards = self.data["cards"]
            except Exception as e:
                logger.error(f"加载数据失败: {e}")

    def save(self) -> None:
        """持久化全部数据。"""
        try:
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存数据失败: {e}")

    def _new_card_id(self) -> str:
        return f"C{int(time.time() * 1000)}{random.randint(1000, 9999)}"

    def issue(
        self,
        group_id,
        user_id,
        days: int,
        source: str,
        platform_id: str = "",
        nickname: str = "",
    ) -> Dict:
        """发放一张未使用的体验卡。"""
        card = {
            "id": self._new_card_id(),
            "group_id": str(group_id),
            "user_id": str(user_id),
            "nickname": str(nickname or ""),
            "days": float(days),
            "source": source,
            "platform_id": platform_id or "",
            "issued_at": datetime.now().isoformat(timespec="seconds"),
            "activated_at": None,
            "expires_at": None,
            "status": STATUS_UNUSED,
        }
        self.cards[card["id"]] = card
        self.save()
        return card

    def get(self, card_id: str) -> Optional[Dict]:
        return self.cards.get(card_id)

    def user_cards(self, group_id, user_id) -> List[Dict]:
        """该用户在指定群的全部卡片。"""
        gid = str(group_id)
        uid = str(user_id)
        return [
            c
            for c in self.cards.values()
            if c["group_id"] == gid and c["user_id"] == uid
        ]

    def has_pending_card(self, group_id, user_id) -> bool:
        """是否持有未使用或体验中的卡。"""
        return any(
            c["status"] in (STATUS_UNUSED, STATUS_ACTIVE)
            for c in self.user_cards(group_id, user_id)
        )

    def has_active_card(self, group_id, user_id) -> bool:
        return any(
            c["status"] == STATUS_ACTIVE for c in self.user_cards(group_id, user_id)
        )

    def active_cards(self, group_id) -> List[Dict]:
        gid = str(group_id)
        return [
            c
            for c in self.cards.values()
            if c["group_id"] == gid and c["status"] == STATUS_ACTIVE
        ]

    def unused_cards(self, group_id) -> List[Dict]:
        gid = str(group_id)
        return [
            c
            for c in self.cards.values()
            if c["group_id"] == gid and c["status"] == STATUS_UNUSED
        ]

    def unused_groups(self, group_id, user_id) -> List[Tuple[int, List[Dict]]]:
        """按天数分组的未使用卡，保持出现顺序。

        Returns:
            形如 [(days, [card, ...]), ...] 的列表，供展示与按组操作。
        """
        groups: List[Tuple[int, List[Dict]]] = []
        index: Dict[int, int] = {}
        for c in self.user_cards(group_id, user_id):
            if c["status"] != STATUS_UNUSED:
                continue
            days = c["days"]
            if days in index:
                groups[index[days]][1].append(c)
            else:
                index[days] = len(groups)
                groups.append((days, [c]))
        return groups

    def schedule_expiry(self, card: Dict) -> None:
        """为一张体验中的卡注册到期下管任务（重复注册时先移除旧任务）。"""
        expires = parse_dt(card["expires_at"])
        self.remove_expiry_job(card["id"])
        self.plugin.scheduler.add_job(
            expire_card_job,
            trigger=DateTrigger(run_date=expires),
            id=f"expire_{card['id']}",
            args=[self.plugin, card["id"]],
            max_instances=1,
        )

    def remove_expiry_job(self, card_id: str) -> None:
        try:
            self.plugin.scheduler.remove_job(f"expire_{card_id}")
        except Exception:
            pass

    def restore_schedules(self) -> None:
        """重启后恢复所有未过期卡的到期任务。"""
        for card in self.cards.values():
            if card["status"] != STATUS_ACTIVE or not card.get("expires_at"):
                continue
            expires = parse_dt(card["expires_at"])
            if expires > datetime.now():
                self.schedule_expiry(card)

    def activate(self, card_id: str) -> bool:
        """将一张 unused 卡激活为体验中（调用方需先完成上管与各项检查）。"""
        card = self.get(card_id)
        if not card or card["status"] != STATUS_UNUSED:
            return False
        now = datetime.now()
        card["status"] = STATUS_ACTIVE
        card["activated_at"] = now.isoformat(timespec="seconds")
        card["expires_at"] = (now + timedelta(days=card["days"])).isoformat(
            timespec="seconds"
        )
        self.schedule_expiry(card)
        self.save()
        return True

    def activate_merged(self, group_id, user_id, cards: List[Dict]) -> Optional[Dict]:
        """合并激活：把若干 unused 卡的天数累加进同一张 active 卡。

        已有 active 卡时在其到期时间上累加；否则以第一张卡为载体新建。
        被合并的卡标记为 consumed。返回最终 active 卡，无可合并卡时返回 None。

        Args:
            group_id: 群号。
            user_id: 用户 QQ。
            cards: 待合并的 unused 卡列表。
        """
        usable = [c for c in cards if c["status"] == STATUS_UNUSED]
        if not usable:
            return None
        total_days = sum(c["days"] for c in usable)
        active = next(
            (c for c in self.active_cards(group_id) if c["user_id"] == str(user_id)),
            None,
        )
        now = datetime.now()
        if active:
            expires = parse_dt(active["expires_at"]) + timedelta(days=total_days)
            active["expires_at"] = expires.isoformat(timespec="seconds")
            active.pop("expire_retries", None)
            self.schedule_expiry(active)
            result = active
        else:
            result = usable[0]
            result["status"] = STATUS_ACTIVE
            result["activated_at"] = now.isoformat(timespec="seconds")
            result["expires_at"] = (now + timedelta(days=total_days)).isoformat(
                timespec="seconds"
            )
            self.schedule_expiry(result)
        for c in usable:
            if c is not result:
                c["status"] = STATUS_CONSUMED
                self.remove_expiry_job(c["id"])
        self.save()
        return result

    def close_active_cards(self, group_id, user_id) -> bool:
        """将某人的 active 卡标记为过期（体验已被中断：管理员身份已不存在）。"""
        changed = False
        for card in self.active_cards(group_id):
            if card["user_id"] == str(user_id):
                card["status"] = STATUS_EXPIRED
                card.pop("expire_retries", None)
                self.remove_expiry_job(card["id"])
                changed = True
        if changed:
            self.save()
        return changed

    async def handle_expire(self, card_id: str) -> None:
        """到期处理：下管并标记过期。

        失败时：先检查成员是否仍是管理员（被手动下管/退群则直接回收），
        仍为管理员则 30 分钟后重试，最多 MAX_EXPIRE_RETRIES 次。
        """
        card = self.get(card_id)
        if not card or card["status"] != STATUS_ACTIVE:
            return
        expires = parse_dt(card["expires_at"])
        if expires > datetime.now():
            return
        gid = card["group_id"]
        uid = card["user_id"]
        pid = card.get("platform_id")
        ok = await self.plugin.api.set_group_admin(gid, uid, False, platform_id=pid)
        if ok:
            card["status"] = STATUS_EXPIRED
            card.pop("expire_retries", None)
            self.save()
            segments = [
                {"type": "at", "data": {"qq": uid}},
                {
                    "type": "text",
                    "data": {"text": " 你的管理体验已到期，管理员身份已回收"},
                },
            ]
            await self.plugin.api.send_group_segments(gid, segments, platform_id=pid)
            return

        retries = int(card.get("expire_retries") or 0)
        if retries >= MAX_EXPIRE_RETRIES:
            logger.error(
                f"群 {gid} 成员 {uid} 体验卡下管重试 {MAX_EXPIRE_RETRIES} 次仍失败，"
                "放弃自动重试，请人工处理"
            )
            return

        info = await self.plugin.api.get_group_member_info(gid, uid, platform_id=pid)
        role = str(info.get("role", "")) if isinstance(info, dict) else ""
        if role and role != "admin":
            logger.info(f"群 {gid} 成员 {uid} 已非管理员，体验卡直接回收")
            card["status"] = STATUS_EXPIRED
            card.pop("expire_retries", None)
            self.save()
            return

        card["expire_retries"] = retries + 1
        self.save()
        logger.error(
            f"群 {gid} 成员 {uid} 体验卡到期下管失败"
            f"（第 {retries + 1}/{MAX_EXPIRE_RETRIES} 次），30 分钟后重试"
        )
        retry_at = datetime.now() + timedelta(minutes=30)
        self.plugin.scheduler.add_job(
            expire_card_job,
            trigger=DateTrigger(run_date=retry_at),
            id=f"expire_{card['id']}",
            args=[self.plugin, card["id"]],
            max_instances=1,
            replace_existing=True,
        )

    async def terminate_experience(self, group_id, user_id) -> Dict:
        """提前终止该用户的当前体验：下管并标记过期，未使用卡保留。

        Returns:
            {"removed": 终止的体验数, "demote_ok": 是否全部下管成功}。
        """
        removed = 0
        demote_ok = True
        for card in self.active_cards(group_id):
            if card["user_id"] != str(user_id):
                continue
            ok = await self.plugin.api.set_group_admin(
                group_id, user_id, False, platform_id=card.get("platform_id")
            )
            if ok:
                card["status"] = STATUS_EXPIRED
                card.pop("expire_retries", None)
                self.remove_expiry_job(card["id"])
                removed += 1
            else:
                demote_ok = False
        if removed:
            self.save()
        return {"removed": removed, "demote_ok": demote_ok}

    async def revoke_user(self, group_id, user_id, card_ids=None) -> Dict:
        """撤销某人的体验卡：体验中立即下管，未使用的作废。

        Args:
            group_id: 群号。
            user_id: 用户 QQ。
            card_ids: 指定撤销的卡 ID 列表；为 None 时撤销该用户全部卡。
        """
        removed = 0
        demote_ok = True
        targets = self.user_cards(group_id, user_id)
        if card_ids is not None:
            targets = [c for c in targets if c["id"] in card_ids]
        for card in targets:
            if card["status"] == STATUS_UNUSED:
                card["status"] = STATUS_CANCELLED
                self.remove_expiry_job(card["id"])
                removed += 1
            elif card["status"] == STATUS_ACTIVE:
                ok = await self.plugin.api.set_group_admin(
                    group_id, user_id, False, platform_id=card.get("platform_id")
                )
                if ok:
                    card["status"] = STATUS_CANCELLED
                    self.remove_expiry_job(card["id"])
                    removed += 1
                else:
                    demote_ok = False
        if removed:
            self.save()
        return {"removed": removed, "demote_ok": demote_ok}

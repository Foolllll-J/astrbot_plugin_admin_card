"""OneBot 11 客户端绑定与动作封装。

客户端在启动流程中从 context.platform_manager 获取并绑定到 self.bot，
绑定成功后立即用 get_login_info 获取Bot自身 QQ 号；定时任务与命令
均使用绑定实例。
"""

import asyncio
from typing import Any, Optional

from astrbot.api import logger


def is_supported_bot_client(client: Any) -> bool:
    """判断对象是否为带 api.call_action 的 OneBot 客户端。"""
    return bool(
        client and hasattr(client, "api") and hasattr(client.api, "call_action")
    )


def is_permission_error(e: Exception) -> bool:
    """判断是否为"Bot不是群主"等权限不足错误（retcode 1200/1401）。"""
    retcode = getattr(e, "retcode", None)
    if retcode is not None:
        try:
            if int(retcode) in (1200, 1401):
                return True
        except (TypeError, ValueError):
            pass
    message = str(getattr(e, "message", "") or "")
    return "NO_PERMISSION" in message.upper() or "权限不足" in message


class ApiClient:
    """平台客户端绑定、self_id 判定与 OneBot 动作封装。"""

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        self.bot: Any = None
        self.self_id: Optional[str] = None
        self.last_perm_error = False
        """最近一次上/下管失败是否因权限不足（Bot不是群主）。"""

    async def bind(self) -> bool:
        """从平台管理器获取并绑定 aiocqhttp 客户端，随后获取Bot自身 QQ 号。"""
        client = self._find_client()
        if client is None:
            return False
        self.bot = client
        try:
            info = await client.api.call_action("get_login_info")
            if isinstance(info, dict) and info.get("user_id") is not None:
                self.self_id = str(info["user_id"])
        except Exception as e:
            logger.warning(f"获取登录信息失败: {e}")
        return True

    def _find_client(self) -> Any:
        """从平台管理器查找可用的 aiocqhttp 客户端实例。"""
        try:
            manager = getattr(self.plugin.context, "platform_manager", None)
            if manager is not None:
                insts = getattr(manager, "platform_insts", None)
                if insts is None:
                    insts = manager.get_insts()
                for platform in insts or []:
                    try:
                        meta = platform.meta()
                    except Exception:
                        continue
                    if str(meta.name).lower() != "aiocqhttp":
                        continue
                    client = platform.get_client()
                    if is_supported_bot_client(client):
                        return client
        except Exception as e:
            logger.warning(f"从平台管理器获取客户端失败: {e}")
        return None

    def get_client(self, platform_id: Optional[str] = None) -> Any:
        """返回绑定的客户端实例；未绑定时惰性从平台管理器现查兜底。"""
        if self.bot is not None:
            return self.bot
        return self._find_client()

    async def get_self_id(self, platform_id: Optional[str] = None) -> Optional[str]:
        """返回Bot自身 QQ 号（bind 时通过 get_login_info 获取）。

        不能从客户端 self_id 属性读取（aiocqhttp 的 __getattr__ 会返回
        functools.partial(call_action, 'self_id') 垃圾对象）。
        """
        return self.self_id

    async def call_action(
        self,
        action: str,
        group_id,
        platform_id: Optional[str] = None,
        **params: Any,
    ) -> Any:
        """调用 OneBot 动作，参数自动补充 group_id。"""
        client = self.get_client(platform_id)
        if not client:
            logger.warning(f"无可用的平台客户端，动作 {action} 未执行")
            return None
        params["group_id"] = group_id
        return await client.api.call_action(action, **params)

    async def set_group_admin(
        self,
        group_id,
        user_id,
        enable: bool,
        platform_id: Optional[str] = None,
    ) -> bool:
        """设置/取消群管理员，失败重试一次，仍失败仅日志。"""
        params = {"user_id": user_id, "enable": enable}
        last_error: Exception | None = None
        self.last_perm_error = False
        for attempt in range(2):
            try:
                await self.call_action(
                    "set_group_admin", group_id, platform_id=platform_id, **params
                )
                logger.info(
                    f"群 {group_id} 成员 {user_id} "
                    f"{'设置为' if enable else '取消'}管理员成功"
                )
                return True
            except Exception as e:
                last_error = e
                if attempt == 0:
                    await asyncio.sleep(5)
        if is_permission_error(last_error):
            self.last_perm_error = True
            logger.error(
                f"群 {group_id} 成员 {user_id} "
                f"{'设置为' if enable else '取消'}管理员失败："
                "Bot不是群主，无权限操作"
            )
        else:
            logger.error(
                f"群 {group_id} 成员 {user_id} "
                f"{'设置为' if enable else '取消'}管理员失败: {last_error}"
            )
        return False

    async def get_group_member_list(
        self, group_id, platform_id: Optional[str] = None
    ) -> list:
        """获取群成员列表（含等级/角色字段），不使用缓存。"""
        try:
            result = await self.call_action(
                "get_group_member_list",
                group_id,
                platform_id=platform_id,
                no_cache=True,
            )
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.error(f"群 {group_id} 获取成员列表失败: {e}")
            return []

    async def get_group_member_info(
        self, group_id, user_id, platform_id: Optional[str] = None
    ) -> dict:
        """获取单个群成员信息。"""
        try:
            result = await self.call_action(
                "get_group_member_info",
                group_id,
                platform_id=platform_id,
                user_id=user_id,
                no_cache=True,
            )
            return result if isinstance(result, dict) else {}
        except Exception as e:
            logger.error(f"群 {group_id} 获取成员 {user_id} 信息失败: {e}")
            return {}

    async def send_group_msg(
        self,
        group_id,
        text: str,
        at_qqs=None,
        platform_id: Optional[str] = None,
    ) -> bool:
        """主动向群发送文本消息，@ 段（如有）追加在文本末尾。

        Args:
            group_id: 目标群号。
            text: 消息文本。
            at_qqs: 单个 QQ 号、QQ 号列表或 None。
            platform_id: 平台实例 ID，为空时自动兜底。
        """
        if at_qqs is None:
            message = text
        else:
            if isinstance(at_qqs, (str, int)):
                at_qqs = [at_qqs]
            segments = [{"type": "text", "data": {"text": text}}]
            segments.extend({"type": "at", "data": {"qq": str(qq)}} for qq in at_qqs)
            message = segments
        return await self.send_group_segments(group_id, message, platform_id)

    async def send_group_segments(
        self,
        group_id,
        segments,
        platform_id: Optional[str] = None,
    ) -> bool:
        """按给定顺序发送 OneBot 消息段列表。

        text 与 at 段交替排列，实现 @ 插入文案中间的效果。段格式：
        {"type": "text", "data": {...}} 或 {"type": "at", "data": {"qq": ...}}。

        Args:
            group_id: 目标群号。
            segments: 消息段列表或纯文本字符串。
            platform_id: 平台实例 ID，为空时自动兜底。
        """
        try:
            await self.call_action(
                "send_group_msg", group_id, platform_id=platform_id, message=segments
            )
            return True
        except Exception as e:
            logger.error(f"群 {group_id} 发送消息失败: {e}")
            return False

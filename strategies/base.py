"""Strategy interfaces for modular trading modes."""

import abc
from typing import Any, Optional


class BaseStrategy(abc.ABC):
	def __init__(self, services: Any):
		self.services = services

	@abc.abstractmethod
	async def on_tick(self):
		"""Run one strategy tick (called by orchestrator or bot loop)."""

	async def on_order_update(self, order_update: Optional[Any] = None):
		"""Optional hook for order updates; default no-op."""
		return None


__all__ = ["BaseStrategy"]

from collections.abc import Callable
from queue import Queue

class Request[_T]:

	def __init__(self, send_to: Callable):
		self.send_to = send_to
		self.fulfilled = False
		self.fulfilled_funcs = Queue()

	def add_fulfilled_func(self, func: Callable[tuple[str, _T]]):
		self.fulfilled_funcs.put(func)

	def fulfill(self, token: tuple[str, _T]):
		self.send_to(token)
		self.fulfilled = True
		n = self.fulfilled_funcs.qsize()
		for _ in range(n):
			self.fulfilled_funcs.get()()
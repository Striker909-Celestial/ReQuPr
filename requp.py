import asyncio
import multiprocessing

from multiprocessing import Queue

from processor import Processor
from request import Request
from buffer import BufferGroup

class ReQup:

	def __init__(self, processors: dict[str, Processor], processor_counts: dict[str, int],
				 output_targets: dict[str, int],
				 max_process_wait=1.0, idle_interval: float = 1.0):
		self.processors = processors
		self.processor_counts = processor_counts
		self.max_process_wait = max_process_wait
		self.idle_interval = idle_interval

		self.buffer_groups = {name: BufferGroup(processor, self.processor_counts[name], self.send_request, self.max_process_wait, self.idle_interval) for name, processor in processors.items()}

		self.active = False
		self.idling = False
		self.daemons = {}

		self.output_targets = output_targets
		self.incomplete_outputs = set([name for name in self.output_targets])
		self.output_data = {name : Queue() for name in output_targets}
		self.output_active_requests = {name : 0 for name in output_targets}

		self.saved_data = None

	def start_daemons(self):
		if self.active:
			return
		self.active = True
		for name, buffer_group in self.buffer_groups.items():
			buffer_group.start_daemons()
			ctx = multiprocessing.get_context('fork')
			proc = ctx.Process(target=buffer_group.working_loop)
			proc.start()
			self.daemons[name] = proc

	def terminate_daemons(self, max_join_wait: float = 1.0):
		"""
		Terminates all daemons running loops for buffer groups.
		:param max_join_wait: The maximum amount of time to wait for a daemon to join and terminate.
		"""
		self.active = False
		for name, proc in self.daemons.items():
			self.buffer_groups[name].terminate_daemons()
			proc.join(timeout=max_join_wait)
			proc.terminate()

	def send_request(self, buffer_group_name: str, request: Request):
		self.buffer_groups[buffer_group_name].receive_request(request)

	def send_output_request(self, output_name: str):
		request = Request(self.output_data[output_name].put)
		self.output_active_requests[output_name] += 1

		def remove_active_request():
			self.output_active_requests[output_name] -= 1
			self.idling = False
		request.add_fulfilled_func(remove_active_request)

		self.send_request(output_name, request)

	def send_output_request_batch(self, n: int):
		"""
		Sends n requests for each output target that hasn't been reached.
		:param n: The number of requests to send for each output target.
		"""
		for name in self.incomplete_outputs:
			if self.output_data[name].qsize() + self.output_active_requests[name] >= self.output_targets[name]:
				continue
			for _ in range(n):
				self.send_output_request(name)

	def output_completion(self) -> bool:
		"""
		Checks if all output targets are met.
		:return: If all output targets are met.
		"""
		if len(self.incomplete_outputs) == 0:
			return True

		for name in self.output_targets:
			if name in self.incomplete_outputs and self.output_data[name].qsize() >= self.output_targets[name]:
				self.incomplete_outputs.remove(name)
		return len(self.incomplete_outputs) == 0

	def save_data(self) -> bool | dict:
		if not self.output_completion():
			return False

		if self.saved_data is not None:
			return self.saved_data

		self.saved_data = {name : [] for name in self.output_targets}
		for name, queue in self.output_data.items():
			while queue.qsize() > 0:
				item = queue.get()
				self.saved_data[name].append(item)
		return self.saved_data

	async def idle(self, idle_interval: float = 1.0):
		self.idling = True
		while self.idling:
			if not self.active:
				return
			await asyncio.sleep(idle_interval)

	async def working_loop(self, output_request_batch_size: int = 5, max_join_wait: float = 1.0):
		while self.active:
			if self.output_completion():
				self.active = False
				break

			self.send_output_request_batch(output_request_batch_size)

			await self.idle(self.idle_interval)
		self.terminate_daemons(max_join_wait=max_join_wait)
		self.save_data()
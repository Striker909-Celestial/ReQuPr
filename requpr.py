import asyncio
import multiprocessing

from multiprocessing import Queue

from processor import Processor
from buffer import BufferGroup
from data_signal import Request, Response, Token


class ReQuPr:

	def __init__(self, processors: dict[str, Processor], processor_counts: dict[str, int],
				 output_targets: dict[str, int],
				 max_process_wait=1.0, idle_interval: float = 1.0):
		self.processors = processors
		self.processor_counts = processor_counts
		self.max_process_wait = max_process_wait
		self.idle_interval = idle_interval

		self.request_queue = Queue()
		self.response_queue = Queue()

		self.buffer_groups = {name: BufferGroup(processor, self.processor_counts[name], self.request_queue, self.response_queue, self.max_process_wait, self.idle_interval) for name, processor in processors.items()}

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
			proc = ctx.Process(target=BufferGroup.run_working_loop, args=[buffer_group])
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

	def push_output_token(self, token: Token) -> bool:
		"""
		Attempts to push a token into the appropriate output queue.
		:param token: The token to be pushed.
		:return: If the token was successfully pushed.
		"""
		if token.source not in self.incomplete_outputs:
			return False
		self.output_data[token.source].put(token.data)
		print(f"ReQuPr: Pushed token to output from {token.source}, {self.output_data[token.source].qsize()}/{self.output_targets[token.source]}")
		return True

	def send_request(self, request: Request):
		"""
		Sends a request to all of its sources.
		:param request: The request to send.
		"""
		for source in request.sources:
			self.buffer_groups[source].receive_request(request)

	def send_output_request(self, output_name: str) -> bool:
		"""
		Sends a request with the target of output.
		:param output_name: The source of the request.
		:return: If the request was sent, or instead canceled for being unnecessary.
		"""
		if output_name not in self.incomplete_outputs:
			return False
		request = Request.new({output_name}, "output")
		self.output_active_requests[output_name] += 1
		self.send_request(request)
		return True

	def send_queued_requests(self):
		"""
		Sends all requests from the request queue.
		"""
		while not self.request_queue.empty():
			request = self.request_queue.get()
			self.send_request(request)

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

	def send_queued_responses(self):
		"""
		Sends all responses from the response queue.
		"""
		while not self.response_queue.empty():
			response = self.response_queue.get()
			if response.target == "output":
				self.push_output_token(response.token)
				continue
			self.buffer_groups[response.target.split("~")[0]].receive_response(response)

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
		while (self.active
				and self.idling
				and self.request_queue.empty()
				and self.response_queue.empty()):
			analytics = {
				"request_queue": self.request_queue.qsize(),
				"response_queue": self.response_queue.qsize(),
				"output": {
					name: output.qsize() for name, output in self.output_data.items()
				}
			}
			# print(f"requpr: {analytics}")
			# for name, buffer_group in self.buffer_groups.items():
			# 	print(name + ": " + str(buffer_group.get_analytics()))
			# print()
			await asyncio.sleep(idle_interval)

	async def working_loop(self, output_request_batch_size: int = 5, max_join_wait: float = 1.0):
		while self.active:
			self.send_queued_requests()
			self.send_queued_responses()
			if self.output_completion():
				self.active = False
				break
			self.send_output_request_batch(output_request_batch_size)

			await self.idle(self.idle_interval)
		self.terminate_daemons(max_join_wait=max_join_wait)
		self.save_data()
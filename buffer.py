import asyncio
import multiprocessing
import datetime

from collections.abc import Callable
from multiprocessing import Queue
from func_timeout import func_timeout

from processor import Processor
from request import Request

class Buffer:
	"""
	A wrapper for a Processor that contains buffers to store inputs for the processor and uses the outputs of the Processor to
	fulfill requests.
	"""

	def __init__(self, processor: Processor, n: int, fallback_send: Callable[tuple], max_process_wait=1.0):
		"""
		A wrapper for a Processor that contains buffers to store inputs for the processor and uses the outputs of the Processor to fulfill requests.
		:param processor: The Processor to use as the underlying processor for this buffer.
		:param n: An integer to identify this buffer.
		:param fallback_send: A function that the output of the processor will be fed into if there is no request to fulfill. This function should send the output into an external storage buffer to be used later.
		:param max_process_wait: The maximum amount of time in seconds the buffer should allow the processor to process for before timing out.
		"""
		self.processor = processor
		self.n = n
		self.name = self.processor.name + "~" + str(n)

		self.active = True

		self.request: Request | None = None
		self.requested = False
		self.fallback_send = fallback_send

		self.max_process_wait = max_process_wait

		self.input_queue = Queue()
		self.input_storage: Queue[tuple] = Queue()

		self.current_dependencies = self.processor.dependencies.copy()
		self.current_dependencies_map = {key: val.copy() for key, val in self.processor.dependencies_map.items()}
		self.input_buffer = {}
		self.request_sent = False
		self.total_time = 0.0
		self.num_calls = 0

	def size(self):
		"""
		The number of slots in the input buffer.
		:return: The number of slots in the buffer
		"""
		return self.processor.num_dependant_args

	def count(self):
		"""
		The number of slots in the input buffer that are filled.
		:return: The number of filled slots in the buffer
		"""
		return len(self.input_buffer)

	def push_request(self, request: Request) -> bool:
		"""
		Provides a request to this buffer to be fulfilled.
		:param request: The request for this buffer to fulfill.
		:return: False if this buffer is already fulfilling a request, true otherwise.
		"""
		if self.requested:
			return False
		self.requested = True
		self.request = request
		self.request.add_fulfilled_func(self.remove_request)
		self.process_buffer()
		return True

	def remove_request(self):
		"""
		Stops this buffer from fulfilling the current request if there is a request the buffer is fulfilling.
		"""
		self.requested = False
		self.request = None

	def process_buffer(self) -> bool | None:
		"""
		Attempts to run the processor on the buffer's contents.
		:return: If the buffer was full and could be processed, or none if the processor timed out.
		"""
		if self.count() < self.size():
			# Returns false if the buffer is not full
		    return False
		start = datetime.datetime.now()
		self.current_dependencies = self.processor.dependencies.copy()
		self.current_dependencies_map = {key: val.copy() for key, val in self.processor.dependencies_map.items()}
		try:
			output = func_timeout(self.max_process_wait, self.processor.process, self.input_buffer)
		except TimeoutError:
			self.total_time += (datetime.datetime.now() - start).total_seconds()
			return None
		self.num_calls += 1
		if self.requested and self.request is not None:
			self.request.fulfill(output)
			self.requested = False
			self.request = None
		else:
			self.fallback_send(output)
		self.input_buffer = {}
		self.push_storage()
		self.total_time += (datetime.datetime.now() - start).total_seconds()
		if self.count() == self.size():
			self.process_buffer()
		return True

	def receive_token(self, token: tuple) -> bool:
		"""

		:return: If the token was successfully received.
		"""
		if token[1] is None:
			# Returns false if the token's value is none
		    return False
		self.input_queue.put(token)
		return True

	def push_token(self, token: tuple):
		"""
		Attempts to push a token into the input buffer for the post processor. If the token is not needed immediately, it will instead
		be moved to storage to be used in a future run of the processor.
		:param token: The token to be pushed.
		:return: If the token was successfully pushed to the input buffer or storage.
		"""
		token_source = token[0]
		if token_source not in self.current_dependencies:
			# Puts the token into storage for a future operation if it is not currently needed
			self.input_storage.put(token)
			return True
		start = datetime.datetime.now()
		kw = list(self.current_dependencies_map[token_source])[0]
		for dep in self.current_dependencies_map.keys():
			if len(self.current_dependencies_map[dep]) == 0:
				continue
			self.current_dependencies_map[dep].remove(kw)
			if len(self.current_dependencies_map[dep]) == 0:
				self.current_dependencies.remove(dep)
		# Places the token's data in the buffer
		self.input_buffer[kw] = token[1]
		self.total_time += (datetime.datetime.now() - start).total_seconds()

		return True

	def push_input_queue(self):
		"""
		Attempts to push all tokens in the input queue into the input buffer.
		"""
		n = self.input_queue.qsize()
		for _ in range(n):
			self.push_token(self.input_queue.get())

	def push_storage(self):
		"""
		Attempts to push all tokens in storage into the input buffer.
		"""
		n = self.input_storage.qsize()
		for _ in range(n):
			self.push_token(self.input_storage.get())

	def get_process_rate(self) -> float:
		"""
		Returns the average
		"""
		if self.total_time == 0.0:
			return -0.0
		return self.num_calls / self.total_time

	def deactivate(self):
		self.active = False

	async def idle(self, idle_interval: float):
		while self.count() < self.size():
			if not self.active:
				return

			await asyncio.sleep(idle_interval)

			if self.input_queue.qsize() != 0:
				return
		return

	async def working_loop(self, idle_interval: float):
		while self.active:
			self.push_input_queue()
			self.process_buffer()

			await self.idle(idle_interval)
		self.push_input_queue()
		self.process_buffer()

class BufferGroup:
	"""
	A group of Buffers that all use the same processor and thus produce the same outputs from the same inputs.

	Delegates requests for the outputs of the processor to the different buffers in the group. Contains storage space for any overflow
	outputs from the buffers.

	Each buffer runs via a separate daemon that loops at a set frequency.
	"""

	def __init__(self, processor: Processor, n: int, send_request: Callable[str, Request], max_process_wait=1.0, idle_interval: float = 0.5):
		"""
		A group of Buffers that all use the same processor and thus produce the same outputs from the same inputs.

		Delegates requests for the outputs of the processor to the different buffers in the group. Contains storage space for any overflow
		outputs from the buffers.

		Each buffer runs via a separate daemon that loops at a set frequency.
		:param processor: The Processor to be used by all buffers in his group.
		:param n: The number of buffers in this group.
		:send_requests: A function which will accept requests created by the buffers in this group.
		:param max_process_wait: The maximum amount of time to wait for a request to be fulfilled.
		:param idle_interval: The time in seconds to wait per loop for a buffer's daemon.
		"""
		self.start_time = datetime.datetime.now()
		self.processor = processor
		self.n = n
		self.name = self.processor.name
		self.active = False

		self.max_process_wait = max_process_wait
		self.idle_interval = idle_interval

		self.send_request = send_request

		self.request_queue = Queue()
		self.total_num_requests = 0
		self.output_storage = Queue()

		self.buffers = {self.name + "~" + str(i): Buffer(self.processor, i, self.output_storage.put, max_process_wait) for i in range(n)}
		self.unrequested_buffers = Queue()
		for name in self.buffers.keys():
			self.unrequested_buffers.put(name)
		self.requested_buffers = set()

		self.daemons: dict[str, multiprocessing.Process] = {}

	def add_buffers(self, n: int):
		"""
		Adds n buffers to the group.
		:param n: The number of buffers to add.
		"""
		n0 = self.n
		self.n = n0 + n
		for i in range(n):
			name = self.name + "~" + str(i + n0)
			buffer =  Buffer(self.processor, n0 + i, self.output_storage.put, self.max_process_wait)
			self.buffers[name] = buffer
			self.unrequested_buffers.put(name)
			if not self.active:
				continue
			proc = multiprocessing.Process(target=buffer.working_loop, args = [self.idle_interval])
			proc.start()
			self.daemons[name] = proc

	def remove_buffers(self, n: int, max_join_wait: float=1.0) -> bool:
		"""
		Attempts to remove n buffers from the group. Will only remove buffers that aren't currently fulfilling a request.
		If there are less than n such buffers, will fail and return False.
		:param n: The number of buffers to remove.
		:param max_join_wait: The maximum amount of time to wait for a daemon to join and terminate.
		:return: If the buffers were successfully removed.
		"""
		if self.unrequested_buffers.qsize() < n:
			return False

		for _ in range(n):
			if self.request_queue.empty():
				return False
			self.n -= 1
			buffer_name = self.unrequested_buffers.get()
			self.buffers.pop(buffer_name)
			if not self.active:
				continue
			self.buffers[buffer_name].deactivate()
			self.daemons[buffer_name].join(timeout=max_join_wait)
			self.daemons[buffer_name].terminate()
		return True

	def start_daemons(self):
		"""
		Starts a daemon process for each buffer to handle its processing.
		"""
		if self.active:
			return
		self.active = True
		for name, buffer in self.buffers.items():
			ctx = multiprocessing.get_context('fork')
			proc = ctx.Process(target=buffer.working_loop, args=[self.idle_interval])
			proc.start()
			self.daemons[name] = proc

	def terminate_daemons(self, max_join_wait: float=1.0):
		"""
		Terminates all daemons running loops for buffers in this group.
		:param max_join_wait: The maximum amount of time to wait for a daemon to join and terminate.
		"""
		self.active = False
		for name, proc in self.daemons.items():
			self.buffers[name].deactivate()
			proc.join(timeout=max_join_wait)
			proc.terminate()


	def receive_request(self, request: Request):
		"""
		Receives an external request for the output of this group's processor. If there are overflow tokens in storage, will fulfill the
		request with one. Otherwise, adds it to the requests queue.
		:param request: The request to be added to the requests queue.
		"""
		self.total_num_requests += 1
		if not self.output_storage.empty():
			request.fulfill(self.output_storage.get())
			return
		self.request_queue.put(request)

	def assign_request(self) -> bool:
		"""
		Assigns the first request in the request queue to the first available buffer.
		:return: False if there are no requests in the requests queue or no available buffers.
		"""
		if self.request_queue.empty() or self.unrequested_buffers.empty():
			return False
		request = self.request_queue.get()
		name = self.unrequested_buffers.get()
		def remove_requested():
			return self.remove_from_requested(name)
		request.add_fulfilled_func(remove_requested)
		self.requested_buffers.add(name)
		self.buffers[name].push_request(request)
		self.send_requests(name)
		return True

	def remove_from_requested(self, buffer_name: str) -> bool:
		"""
		Removes the buffer with the given name from the being requested.
		:param buffer_name: The buffer name to remove from being requested.
		:return: If the buffer with the given name could be removed.
		"""
		if buffer_name not in self.requested_buffers:
			return False
		self.requested_buffers.remove(buffer_name)
		self.unrequested_buffers.put(buffer_name)
		return True

	def send_requests(self, buffer_name: str):
		"""
		Sends a request for all missing inputs for the buffer with the given name.
		:param buffer_name: The buffer name to send requests for.
		"""
		for arg in self.processor.dependant_args_map:
			if arg in self.buffers[buffer_name].input_buffer:
				continue
			request = Request(self.buffers[buffer_name].receive_token)
			for dep in self.processor.dependant_args_map[arg]:
				self.send_request(dep, request)

	async def idle(self, idle_interval: float):
		while self.request_queue.empty() or self.unrequested_buffers.empty():
			if not self.active:
				return
			await asyncio.sleep(idle_interval)
		return

	async def working_loop(self):
		while self.active:
			while not self.request_queue.empty() and not self.unrequested_buffers.empty():
				self.assign_request()

			await self.idle(self.idle_interval)

	def get_analytics(self) -> dict:
		"""
		Returns debug analytics on this group and all buffers within in a json/dict format.
		:return: A dict with debug analytics.
		"""
		runtime = datetime.datetime.now() - self.start_time
		return {
			"runtime": runtime.total_seconds(),
			"requests": {
				"qsize": self.request_queue.qsize(),
				"rate": self.total_num_requests / runtime.total_seconds(),
			},
			"storage": {
				"size": self.output_storage.qsize(),
			},
			"buffers": [
				{
					"count": buffer.count(),
					"fill": float(buffer.count()) / buffer.size(),
					"storage": buffer.input_storage.qsize(),
					"rate": buffer.get_process_rate()
				}
				for buffer in self.buffers.values()
			]
		}
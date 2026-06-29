import asyncio
import multiprocessing
import datetime

from multiprocessing import Queue, Manager
from multiprocessing.managers import SyncManager
from func_timeout import func_timeout

from processor import Processor
from data_signal import Token, Request, Response

class Buffer:
	"""
	A wrapper for a Processor that contains buffers to store inputs for the processor and uses the outputs of the Processor to
	fulfill requests.
	"""

	def __init__(self, processor: Processor, n: int, output_queue: Queue[Token], available_buffers_queue: Queue[str], working_buffers_set, max_process_wait=1.0, idle_interval: float = 0.5):
		"""
		A wrapper for a Processor that contains buffers to store inputs for the processor and uses the outputs of the Processor to fulfill requests.
		:param processor: The Processor to use as the underlying processor for this buffer.
		:param n: An integer to identify this buffer.
		:param output_queue: The queue for this buffer to output to.
		:param max_process_wait: The maximum amount of time in seconds the buffer should allow the processor to process for before timing out.
		"""
		self.processor = processor
		self.n = n
		self.name = self.processor.name + "~" + str(n)

		self.max_process_wait = max_process_wait
		self.idle_interval = idle_interval

		self.active = True
		self.requested = False

		self.output_queue = output_queue
		self.available_buffers_queue = available_buffers_queue
		self.available_buffers_queue.put(self.name)
		self.working_buffers_set = working_buffers_set

		self.input_queue: Queue[Token] = Queue()
		self.input_storage: Queue[Token] = Queue()

		self.current_dependencies = self.processor.dependencies.copy()
		self.current_dependencies_map = {key: val.copy() for key, val in self.processor.dependencies_map.items()}
		self.input_buffer = {}
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
		output = Token(self.processor.name, None)
		try:
			output = func_timeout(self.max_process_wait, self.processor.process, kwargs={"dependency_inputs": self.input_buffer})
		except TimeoutError:
			pass
		self.num_calls += 1
		self.output_queue.put(output)
		self.available_buffers_queue.put(self.name)
		self.working_buffers_set.remove(self.name)
		self.input_buffer = {}
		print(f"Buffer {self.name}: Finished processing")
		self.push_storage()
		self.total_time += (datetime.datetime.now() - start).total_seconds()
		return True

	def receive_token(self, token: Token):
		"""
		Adds a token to this buffer's input queue.
		:param token: The token to add to the input queue.
		"""
		self.input_queue.put(token)

	def push_token(self, token: Token):
		"""
		Attempts to push a token into the input buffer for the processor. If the token is not needed immediately, it will instead
		be moved to storage to be used in a future run of the processor.
		:param token: The token to be pushed.
		:return: If the token was successfully pushed to the input buffer or storage.
		"""
		token_source = token.source
		print(f"Buffer {self.name}: Pushing token from {token_source} into buffer")
		if token_source not in self.current_dependencies:
			# Puts the token into storage for a future operation if it is not currently necessary
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
		self.input_buffer[kw] = token.data
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
		while (self.active
				and ((self.count() < self.size()
				and self.input_queue.qsize() == 0)
				or self.name not in self.working_buffers_set)):
			await asyncio.sleep(idle_interval)

	async def working_loop(self):
		while self.active:
			self.push_input_queue()
			if self.name in self.working_buffers_set:
				self.process_buffer()

			await self.idle(self.idle_interval)
		self.push_input_queue()
		self.process_buffer()

	@staticmethod
	def run_working_loop(buffer: Buffer):
		asyncio.run(buffer.working_loop())

class BufferGroup:
	"""
	A group of Buffers that all use the same processor and thus produce the same outputs from the same inputs.

	Delegates requests for the outputs of the processor to the different buffers in the group. Contains storage space for any overflow
	outputs from the buffers.

	Each buffer runs via a separate daemon that loops at a set frequency.
	"""

	def __init__(self, processor: Processor, n: int, request_queue: Queue[Request], response_queue: Queue[Response], max_process_wait=1.0, idle_interval: float = 0.5):
		"""
		A group of Buffers that all use the same processor and thus produce the same outputs from the same inputs.

		Delegates requests for the outputs of the processor to the different buffers in the group. Contains storage space for any overflow
		outputs from the buffers.

		Each buffer runs via a separate daemon that loops at a set frequency.
		:param processor: The Processor to be used by all buffers in his group.
		:param n: The number of buffers in this group.
		:param request_queue: A queue to send requests.
		:param response_queue: A queue to send responses to requests.
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

		self.external_request_queue = request_queue
		self.external_response_queue = response_queue

		self.request_queue = Queue()
		self.active_requests = {}
		self.total_num_requests = 0

		self.output_storage = Queue()
		self.response_queue = Queue()

		self.manager = multiprocessing.get_context('fork').Manager()

		self.available_buffers = Queue()
		self.working_buffers = self.manager.set()
		self.buffers = {self.name + "~" + str(i): Buffer(self.processor, i, self.output_storage, self.available_buffers, self.working_buffers, self.max_process_wait, self.idle_interval) for i in range(n)}

		self.daemons: dict[str, multiprocessing.Process] = {}

	def add_buffers(self, n: int):
		"""
		Adds n buffers to the group.
		:param n: The number of buffers to add.
		"""
		n0 = self.n
		self.n = n0 + n
		ctx = multiprocessing.get_context('fork')
		for i in range(n):
			name = self.name + "~" + str(i + n0)
			buffer =  Buffer(self.processor, n0 + i, self.output_storage, self.available_buffers, self.working_buffers, self.max_process_wait, self.idle_interval)
			self.buffers[name] = buffer
			self.available_buffers.put(name)
			if not self.active:
				continue
			proc = ctx.Process(target=Buffer.run_working_loop, args = [buffer])
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
		if self.available_buffers.qsize() < n:
			return False

		for _ in range(n):
			if self.request_queue.empty():
				return False
			self.n -= 1
			buffer_name = self.available_buffers.get()
			self.buffers[buffer_name].deactivate()
			self.buffers.pop(buffer_name)
			if not self.active:
				continue
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
		ctx = multiprocessing.get_context('fork')
		for name, buffer in self.buffers.items():
			proc = ctx.Process(target=Buffer.run_working_loop, args=[buffer])
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


	def send_request(self, request: Request):
		"""
		Sends a request to the external request queue.
		:param request: The request to be sent.
		"""
		self.external_request_queue.put(request)

	def send_response(self, request: Request, token: Token):
		"""
		Fulfills a request with a token and sends both the request and the response to their respective external queues.
		:param request: The request to fulfill.
		:param token: The token to fulfill the request with.
		"""
		response = request.fulfill(token)
		self.active_requests.pop(request.id)
		self.external_request_queue.put(request)
		self.external_response_queue.put(response)

	def fulfill_active_requests(self) -> bool:
		"""
		Fulfills requests from the active requests queue until either the output storage or active requests queue is empty.
		:return: False if there are no active requests or no tokens in output storage.
		"""
		if self.output_storage.empty() and len(self.active_requests) == 0:
			return False
		while not self.output_storage.empty() and len(self.active_requests) > 0:
			token = self.output_storage.get()
			if token.data is None:
				if not self.available_buffers.empty():
					self.send_buffer_requests(self.available_buffers.get())
				else:
					self.request_queue.put(Request.ghost())
				continue
			request = list(self.active_requests.values())[0]
			self.send_response(request, token)
		return True

	def receive_request(self, request: Request):
		"""
		Receives an external request for the output of this group's processor. If there are overflow tokens in storage, will fulfill the
		request with one. Otherwise, adds it to the requests queue.
		:param request: The request to be added to the requests queue.
		"""
		self.total_num_requests += 1
		print(f"Buffer Group {self.name}: Received request from {request.target}")
		if request.fulfilled:
			if request.id in self.active_requests:
				self.active_requests.pop(request.id)
			return
		if not self.output_storage.empty():
			response = request.fulfill(self.output_storage.get())
			if response is not None:
				self.external_request_queue.put(request)
				self.external_response_queue.put(response)
			return
		self.request_queue.put(request)

	def assign_requests(self) -> bool:
		"""
		Assigns as many requests from the request queue as possible to available buffers.
		:return: False if there are no requests in the request queue or no available buffers.
		"""
		if self.request_queue.empty() or self.available_buffers.empty():
			return False
		while not self.request_queue.empty() and not self.available_buffers.empty():
			request = self.request_queue.get()
			if request.id != -1:
				self.active_requests[request.id] = request
			name = self.available_buffers.get()
			self.working_buffers.add(name)
			self.send_buffer_requests(name)
		return True

	def send_buffer_requests(self, buffer_name: str):
		"""
		Sends a request for all missing inputs for the buffer with the given name.
		:param buffer_name: The buffer name to send requests for.
		"""
		for arg in self.processor.dependant_args_map:
			if arg in self.buffers[buffer_name].input_buffer:
				continue
			request = Request.new(self.processor.dependant_args_map[arg], buffer_name)
			self.send_request(request)

	def receive_response(self, response: Response):
		self.response_queue.put(response)
		print(f"Buffer Group {self.name}: Received response from {response.token.source}")

	def push_responses(self) -> bool:
		"""
		Pushes all responses from the response queue to the appropriate buffers.
		:return: If there were any responses in the response queue.
		"""
		if self.response_queue.empty():
			return False
		while not self.response_queue.empty():
			response = self.response_queue.get()
			self.buffers[response.target].receive_token(response.token)
		return True


	async def idle(self, idle_interval: float):
		while (self.active
				and (self.request_queue.empty() or self.available_buffers.empty())
				and (self.output_storage.empty() or len(self.active_requests) == 0)
				and self.response_queue.empty()):
			await asyncio.sleep(idle_interval)

	async def working_loop(self):
		while self.active:
			self.assign_requests()
			self.push_responses()
			self.fulfill_active_requests()

			await self.idle(self.idle_interval)

	@staticmethod
	def run_working_loop(buffer_group: BufferGroup):
		asyncio.run(buffer_group.working_loop())

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
					"available": buffer.name not in self.working_buffers,
					"count": buffer.count(),
					"fill": 0 if buffer.size() == 0 else float(buffer.count()) / buffer.size(),
					"storage": buffer.input_storage.qsize(),
					"rate": buffer.get_process_rate()
				}
				for buffer in self.buffers.values()
			]
		}
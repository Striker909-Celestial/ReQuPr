from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class Token[_T]:
	"""
	A dataclass to hold a single output of a processor.

	``source``: The name of the processor that produced this token

	``data`` The data produced by the processor.
	"""
	source: str
	data: _T

@dataclass
class Request:
	"""
	A dataclass representing a request from an output or a buffer for a token produced by a certain processor or one of several different producers.

	``id``: A unique integer ID for this request.

	``sources``: A set of processor names whose output would satisfy this request.

	``target``: The name of the buffer or output to send the requested token to.

	``fulfilled``: A flag to indicate if this request has been fulfilled.
	"""
	current_id: ClassVar[int] = -1

	id: int
	sources: set[str]
	target: str
	fulfilled: bool

	@classmethod
	def new(cls, sources: set[str], target: str):
		"""
		Creates a new request with the given sources and target.
		:param sources: A set of processor names whose output would satisfy this request.
		:param target: The name of the buffer or output to send the requested token to.
		:return: A new request.
		"""
		cls.current_id += 1
		print(f"Request {cls.current_id}: Created by {target}")
		return cls(
			id=Request.current_id,
			sources=sources,
			target=target,
			fulfilled=False
		)

	@classmethod
	def ghost(cls):
		"""
		Creates a "ghost" request that cannot generate a response and has the ID -1.
		:return: A "ghost" request.
		"""
		print("Created ghost request")
		return cls(id=-1, sources=set(), target="None", fulfilled=True)

	def fulfill[_T](self, token: Token[_T]) -> None | Response[_T]:
		"""
		Sets the status of this request to fulfilled and returns a response to this request with the token.
		:param token: The token to fulfill this request with.
		:return: A response to this request with the token, or none if this request has already been fulfilled
		"""
		if self.fulfilled:
			return None
		print(f"Request {self.id} from {self.target}: Fulfilled by {token.source}")
		self.fulfilled = True
		return Response(self.id, self.target, token)

@dataclass(frozen=True)
class Response[_T]:
	"""
	A dataclass representing a response to a request with a token attached.

	``id``: The ID for the request being responded to.

	``target``: The buffer or output this response should be sent to.

	``token``: The token attached to this response.
	"""
	id: int
	target: str
	token: Token[_T]
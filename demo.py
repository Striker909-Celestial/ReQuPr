import asyncio

from requpr import ReQuPr
from processor import Processor
import numpy

def rand(min, max):
	return numpy.random.randint(min, max)
def add(a, b):
	return a+b
def mul(a, b):
	return a*b

rand_processor = Processor("rand", rand, {"min": -10, "max": 11})
add_processor = Processor("add", add, {"a": "@rand|add|mul", "b": "@rand|add|mul"})
mul_processor = Processor("mul", mul, {"a": "@rand|add|mul", "b": "@rand|add|mul"})

requp = ReQuPr({"rand": rand_processor, "add": add_processor, "mul": mul_processor},
               {"rand": 4, "add": 2, "mul": 2}, {"add": 1000, "mul": 1000})

requp.start_daemons()
asyncio.run(requp.working_loop(), debug=True)
print(requp.save_data())
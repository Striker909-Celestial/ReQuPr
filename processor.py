from collections.abc import Callable

from data_signal import Token


class Processor:
    """
    Represents a single processor.

    A processor consists of a function and keyword arguments that are passed to that function.
    The keyword arguments may reference the output of another processor through a pair in the form:
    ``"ARG_NAME": "@OTHER_PROCESSOR_NAME"``. The other processor therefore becomes a dependency of this processor.

    A generator is a subtype of processor. To be a generator, a processor must have no dependencies. However,
    not all processors without dependencies are generators. Generators are called anytime the queue is empty,
    not just when requested.

    **Attributes:**

    ``name``: The name of the processor.

    ``function``: The function run by the processor.

    ``function_kwargs``: A dict of keyword arguments passed to the function.
    References to other processors will be replaced with an output from those processors.

    ``is_generator``: If the processor is considered a generator.

    ``dependencies``: A set of dependencies on other processors.

    ``dependencies_map``: A dict mapping dependencies to sets of all keywords
    """
    def __init__(self, name: str, function: Callable, function_kwargs: dict, is_generator: bool | None=None):
        self.name = name
        self.function = function
        self.function_kwargs = function_kwargs
        self.is_generator = is_generator

        self.dependencies = set()
        self.dependencies_map = {}
        self.dependant_args = set()
        self.dependant_args_map = {}
        for kw, value in self.function_kwargs.items():
            if type(value) == str and value[0] == '@':
                self.dependant_args.add(kw)
                self.dependant_args_map[kw] = set()
                for s in value[1:].split('|'):
                    if s not in self.dependencies:
                        self.dependencies.add(s)
                        self.dependencies_map[s] = set()
                    self.dependencies_map[s].add(kw)
                    self.dependant_args_map[kw].add(s)
        self.num_dependencies = len(self.dependencies)
        self.num_dependant_args = len(self.dependant_args)

        if self.is_generator is None or self.is_generator:
            self.is_generator = self.num_dependant_args == 0


    def process(self, dependency_inputs: dict | None=None):
        """
        Runs the processor's function with the processor's keyword arguments.

        If the processor has no dependencies, will disregard ``dependency_inputs``.

        If the processor has dependencies but ``dependency_inputs`` is not provided, will return None.
        If the processor has dependencies and ``dependency_inputs`` is provided, will replace the correct keyword
        arguments with the ``dependency_inputs``.
        :param dependency_inputs: A dict mapping keyword arguments for the processor's function to outputs from the
        corresponding processors.
        :return: The output from the processor's function.
        """
        if len(self.dependencies) == 0:
            return self.name, self.function(**self.function_kwargs)
        if dependency_inputs is None:
            return self.name, None
        kwargs = self.function_kwargs.copy()
        kwargs.update(dependency_inputs)
        return Token(self.name, self.function(**kwargs))
import os
from collections import defaultdict
from typing import List, Dict, Any, Set, Optional, Union
from itertools import chain

class Dependency:
	def dependencies(self) -> Set['Dependency']:
		return set()


all_jobs = []

all_files = {}


class Pipe(Dependency):
	name: Optional[str]
	origins: Set[Dependency]

	def __init__(self, name:Optional[str]=None, *, origin:Optional[Dependency]=None):
		self.name = name
		self.origins = {origin} if origin else set()

	def dependencies(self) -> Set[Dependency]:
		return self.origins


class RootDependency(Dependency):
	name: str
	def __init__(self, name:str):
		super().__init__()
		self.name = name


class File(Pipe):
	def __init__(self, name:str):
		super().__init__(name)


FS = RootDependency('<fs>')


def Input(name:str):
	"""File I expect to exist at this point"""
	if not os.path.exists(name) and name not in all_files:
		raise ValueError(f'File does not exist: {name}')

	if name not in all_files:
		all_files[name] = File(name)
		all_files[name].origins.add(FS)

	return all_files[name]





def Output(name:str):
	"""File I don't expect to exist yet"""
	if os.path.exists(name) or name in all_files:
		raise ValueError(f'File already exists: {name}')

	if name not in all_files:
		all_files[name] = File(name)

	return all_files[name]

	
class Process(Dependency):
	cmd: str
	args: List[Union[str,Dependency]]
	_stdin: Optional[Pipe]
	_stdout: Optional[Pipe]

	def __init__(self, cmd:str, args:List[Union[str,Dependency]], stdin:Optional[Pipe]=None, stdout:Optional[Pipe]=None):
		self.cmd = cmd
		self.args = args
		self._stdin = stdin
		self._stdout = stdout

		for arg in args:
			if isinstance(arg, File):
				arg.origins.add(self)

		if stdout:
			stdout.origins.add(self)

	@property
	def stdin(self):
		if self._stdin is not None:
			raise RuntimeError('stdin already used')
		self._stdin = Pipe(name=f'/proc/{self.cmd}/stdin', origin=self)
		return self._stdin

	@property
	def stdout(self):
		if self._stdout is not None:
			raise RuntimeError('stdin already used')
		self._stdout = Pipe(name=f'/proc/{self.cmd}/stdout', origin=self)
		return self._stdout

	def dependencies(self) -> Set[Dependency]:
		return set(
			dependency
			for dependency in chain(self.args, [self.stdin] if self.stdin else [])
			if isinstance(dependency, Dependency)
		)


def call(cmd, *args:Union[str,Dependency], env:Dict[str,Any]={}, stdin:Optional[Pipe]=None, stdout:Optional[Pipe]=None) -> Process:
	proc = Process(cmd, list(args), stdin, stdout)
	all_jobs.append(proc)
	return proc


def name_generator():
	letters = [chr(i) for i in range(ord('a'), ord('z') + 1)]
	i = 0
	while True:
		out = []
		n = i
		i += 1
		while True:
			b = n % len(letters)
			out.append(letters[b])
			n = n // len(letters)
			if n == 0:
				break
			else:
				n -= 1 # because letters[0] == a
		yield ''.join(reversed(out))


def print_dot(jobs:List[Dependency]=all_jobs, *, file=None):
	print('digraph graphname {', file=file)
	print('  rankdir=LR;', file=file)
	
	serial = iter(name_generator())
	names = defaultdict(lambda: next(serial))

	for job in jobs:
		if job not in names:
			print(f'  {names[job]} [label="{job.cmd}",shape=box];', file=file)

		for dependency in job.dependencies():
			if dependency not in names:
				print(f'  {names[dependency]} [label="{dependency.name}"];', file=file)
			print(f'  {names[dependency]} -> {names[job]};', file=file)

	print('}', file=file)

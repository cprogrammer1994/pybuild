import os, sys, queue, threading, subprocess, shlex, textwrap

def get_default(d, k, v):
	return d[k] if k in d else v

class Pipe:
	def __init__(self):
		self.data = bytearray()

	def write(self, text):
		self.data.extend(text.encode())

	def flush(self):
		pass

class Context:
	def __init__(self, content):
		self.content = {}
		for key in content:
			self.content[key] = str(content[key])

	def __getitem__(self, key):
		if key in self.content:
			return self.content[key].format_map(self)
		return ''

class BuildNode:
	def __init__(self, name : str, counter : int, children : set, builds : list):
		self.counter = counter + 1
		self.children = children
		self.builds = builds
		self.name = name

		self.exists = os.path.exists(name)
		if self.exists:
			self.mtime = os.path.getmtime(name)

	def should_build(self):
		if self.exists:
			for child in self.children:
				if child.exists and child.mtime < self.mtime:
					child.exists = False
		else:
			for child in self.children:
				child.exists = False

		return not self.exists

class BuildQueue:
	def __init__(self, nodes : list, num_threads : int):
		self.num_threads = num_threads
		self.lock = threading.Lock()
		self.queue = queue.Queue()
		self.left = set(nodes)
		self.having = 0

		self.decrement(nodes)

	def decrement(self, nodes : list):
		for node in nodes:
			node.counter -= 1
			if not node.counter:
				self.queue.put(node)
				self.having += 1

	def get(self):
		with self.lock:
			self.having -= 1
			if self.having == -self.num_threads:
				for i in range(self.num_threads):
					self.queue.put(None)

		return self.queue.get()

	def feedback(self, node : BuildNode):
		self.left.discard(node)
		self.decrement(node.children)

def execute_task(task : BuildNode, context : Context):
	if not task.should_build():
		return True

	for build in task.builds:
		if hasattr(build, '__call__'):
			pipe, sys.stdout = sys.stdout, Pipe()
			returncode = build()
			pipe, sys.stdout = sys.stdout, pipe
			report_task(task.name, build.__name__, returncode, bytes(pipe.data))
			if returncode:
				return False

		elif type(build) is tuple and hasattr(build[0], '__call__'):
			pipe, sys.stdout = sys.stdout, Pipe()
			returncode = build[0](*build[1:])
			pipe, sys.stdout = sys.stdout, pipe
			report_task(task.name, build[0].__name__, returncode, bytes(pipe.data))
			if returncode:
				return False

		elif type(build) is str:
			build = build.format_map(context)
			args = shlex.split(build)
			proc = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, shell = True)
			proc.wait()

			report_task(task.name, build, proc.returncode, proc.stdout.read())
			if proc.returncode:
				return False

		else:
			report_task(task.name, build, 'Invalid build type', b'')
			return False

	return True

def worker_thread(build_queue : BuildQueue, context : Context):
	node = build_queue.get()
	while node:
		if execute_task(node, context) and (node.name.startswith('!') or os.path.exists(node.name)):
			build_queue.feedback(node)
		
		else:
			report_missing(node.name)

		node = build_queue.get()

def build(context : dict, depends : dict, builds : dict, first : str, num_threads : int = 1):
	if first not in depends and first not in builds:
		report_missing(first)
		report_end(False, [first])
		return
	
	def walkdep(node, result):
		if node in depends:
			for dep in depends[node]:
				if dep in result:
					result[dep].children.add(result[node])
				else:
					result[dep] = BuildNode(dep, len(get_default(depends, dep, [])), {result[node]}, get_default(builds, dep, []))
					walkdep(dep, result)
		return result

	nodes = walkdep(first, {first : BuildNode(first, len(get_default(depends, first, [])), set(), get_default(builds, first, []))})
	report_start(list(nodes))
	
	build_queue = BuildQueue(nodes.values(), num_threads)
	pool = [threading.Thread(target = worker_thread, args = (build_queue, Context(context))) for i in range(num_threads)]

	for worker in pool:
		worker.start()

	for worker in pool:
		worker.join()

	report_end(not build_queue.left, [node.name for node in build_queue.left])

def wrap(text, indent):
	return textwrap.indent(textwrap.fill(text, 79 - indent), ' ' * indent)

def ewrap(text, indent):
	result, *lines = textwrap.wrap(text, 79 - indent)
	if lines:
		result += '\n' + textwrap.indent('\n'.join(lines), ' ' * indent)
	return result

def report_task(task, build, ret, stdout):
	print(stdout.decode())
	if ret:
		print('[ERROR]: %s\n%s' % (task, wrap(build, 9)), end = '\n\n')
	else:
		print('[OK]: %s\n%s' % (task, wrap(build, 6)), end = '\n\n')

def report_missing(name):
	print('[MISSING]:', name, end = '\n\n')

def report_start(deps):
	print('[START]:', ewrap(' '.join(sorted(deps)), 9), end = '\n\n')

def report_end(success, rest):
	if not success:
		print('[FAIL]:', ewrap(' '.join(sorted(rest)), 8), end = '\n\n')
	else:
		print('[DONE]', end = '\n\n')

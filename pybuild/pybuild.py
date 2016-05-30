import os, queue, threading, subprocess, shlex

def get_default(d, k, v):
	return d[k] if k in d else v

class Context:
	def __init__(self, content):
		self.content = content

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
			returncode = build()
			report_task(task.name, build.__name__, returncode, b'')
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

def report_task(task, build, ret, stdout):
	if ret:
		print('[ERROR]:', task, build)
	else:
		print('[OK]:', task, build)

def report_missing(name):
	print('[MISSING]:', name)

def report_start(deps):
	print('[START]:', *sorted(deps))

def report_end(success, rest):
	if not success:
		print('[FAIL]:', *sorted(rest))
	else:
		print('[DONE]')

#encoding=utf8
import threading,time,os,sys
import redis

class Commander(threading.Thread):
    def __init__(self, command_from = 'file',file_path='/dev/null',redis_conf=None, sleep_time=2,threadName='Commander:Default'):
        threading.Thread.__init__(self)
        self.command = ''
        self.sleep_time = sleep_time
        self.command_from = command_from
        if command_from == 'file':
            self.stdin = file_path
        if command_from == 'redis':
            self.redis_conf = redis_conf
            self.redis = redis.Redis(**self.redis_conf)
        # self.ThreadName = 'Commander:' + threadName if threadName else 'Default'
        self.ThreadName = threadName

    def file_command(self):
        try:
            a = raw_input().strip()
            if a:
                print 'file command', a
                self.command = a
        except:
            pass

    def register(self,ThreadName):
        if self.command_from == 'redis':
            self.redis.delete(ThreadName)
        elif self.command_from == 'file':
            os.system('rm -f %s' % self.stdin)
            os.system('touch  %s' % self.stdin)
            si = file(self.stdin, 'r')
            os.dup2(si.fileno(), sys.stdin.fileno())

    def redis_command(self):
        try:
            if self.redis_conf.get('block_pop', False):
                command = self.redis.brpop(self.ThreadName, timeout=self.redis_conf.get('pop_timeout', self.sleep_time))
            else:
                command = self.redis.rpop(self.ThreadName)
            if command:
                if isinstance(command, tuple):
                    self.command = command[1]
                else:
                    self.command = command
                print 'redis command', self.command
        except Exception as e:
            print >> sys.stderr, 'get redis command err', str(e)

    def run(self):
        self.register(self.ThreadName)
        if self.command_from == 'file':
            while 1:
                self.file_command()
                time.sleep(self.sleep_time)
        elif self.command_from == 'redis':
            while 1:
                self.redis_command()
                time.sleep(self.sleep_time)

    def get_command(self):
        return self.command


class Receiver(threading.Thread):
    def __init__(self,commander,sleep_time=5,name=None):
        threading.Thread.__init__(self,name=name)
        self.is_started = False
        self.sleep_time = sleep_time
        self.commander = commander
        self.to_stop = False
        self.non_pause = False

    def executor(self):
        command = self.commander.get_command()
        if command == 'stop':
            self.to_stop = True
    def do(self):
        # if you want to terminate this thread, set self.to_stop True
        pass

    def run(self):
        self.is_started = True
        while 1:
            self.executor()
            if self.to_stop: return
            self.do()
            self.executor()
            if self.to_stop:return
            if not self.non_pause:
                time.sleep(self.sleep_time)


class MyClass(Receiver):
    def __init__(self,commander):
        Receiver.__init__(self,commander=commander)

    def do(self):
        # here is my code
        pass
#
# commander = Commander('/home/filepath')
# commander.start()
# MyClassObject = MyClass(commander)
# MyClassObject.start()
# MyClassObject.join()
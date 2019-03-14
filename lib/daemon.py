#!/usr/bin/env python
#coding=utf-8
import sys, os, time, atexit
from signal import SIGTERM,SIGKILL

class Daemon:
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
    
    def daemonize(self):
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
    
        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    
    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        print "start[ok]"
        # Start the daemon
        self.daemonize()
        self.run()
        

        
        

    def stop(self, forcible = False):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process    
        try:
            print 'pls wait...'
            while 1:
                if forcible:
                    os.kill(pid, SIGKILL)
                else:
                    os.kill(pid, SIGTERM)
                time.sleep(2)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
                print 'stoped',
            else:
                print str(err)
                sys.exit(1)
    def forcestop(self, forcible = True):
        """
        force stop the daemon
        """
        self.stop(forcible)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()
    def status(self):
        if os.path.exists(self.pidfile):
            print "alread run"
        else:
            print "not run"

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """
        pass


def help():
    print "usage: %s start|stop|restart|status"%sys.argv[0]
    
def run_daemon(daemon):
    if len(sys.argv)==2:
        cmd=sys.argv[1]
        if cmd=='start':
            daemon.start()
        elif cmd=='stop':
            daemon.stop()
        elif cmd=='forcestop':
            daemon.forcestop()
        elif cmd=='status':
            daemon.status()
        elif cmd=='restart':
            daemon.restart()
        else:
            print "Unknown command"
            help()
    else:
        help()

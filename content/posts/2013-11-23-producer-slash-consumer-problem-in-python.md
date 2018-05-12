---
layout: post
title: "Producer/Consumer problem in python"
date: 2013-11-23T18:01:00+03:00
comments: true
categories:
    - python
    - producer consumer
    - multithreading
    - programming
draft: true
---

Recently I stumbled upon an almost classical [producer-consumer
problem][producer-consumer] in my script. The script is very similar to the
popular [s3cmd][s3cmd] -- it syncronises contents of the local and remote
folder. Direction doesn't matter but for the clarity let's pretend it downloads
data from the remote side.

<!--
Recently I stumbled upon an almost classical [producer-consumer
problem][producer-consumer] in my script. The script is very similar to the
popular [s3cmd][s3cmd] - it syncronises contents of the local and [S3][s3]
folder. Direction doesn't matter but for the clarity let's pretend it downloads
data from S3. Of course I could take s3cmd from an [alternative][s3cmdparallel1]
[github][s3cmdparallel2] repo where parallel workers feature is already
implemented (there is an [issue][issue] in s3cmd to implement that). However,
for some reason, I needed my very own version of such parallel downloader.
-->

<!-- more -->

In this post I would like to tell a story of my progress implementing this
feature, share some bugs I found and workarounds I had to use. First, let me
describe the directory structure. It is rather strict and looks like this:

```
$ tree /home/bz/source 
/home/bz/source
├── apple
│   ├── 2013-11-22
│   │   ├── apple-2013-11-22-0.tsv.gz
│   │   └── apple-2013-11-22-1.tsv.gz
│   ├── 2013-11-23
│   │   ├── apple-2013-11-23-0.tsv.gz
│   │   └── apple-2013-11-23-1.tsv.gz
│   ├── _description
│   └── _header
├── banana
│   ├── 2013-11-22
│   │   ├── banana-2013-11-22-0.tsv.gz
│   │   └── banana-2013-11-22-1.tsv.gz
│   ├── 2013-11-23
│   │   ├── banana-2013-11-23-0.tsv.gz
│   │   └── banana-2013-11-23-1.tsv.gz
│   ├── _description
│   └── _header
└── orange
    ├── 2013-11-22
    │   ├── orange-2013-11-22-0.tsv.gz
    │   └── orange-2013-11-22-1.tsv.gz
    ├── 2013-11-23
    │   ├── orange-2013-11-23-0.tsv.gz
    │   └── orange-2013-11-23-1.tsv.gz
    ├── _description
    └── _header

9 directories, 18 files
```

### First try

Let's start with a very basic non-parallel sketch of such downloader. I will
use python2 because of the dependencies I have in my original script.

``` python
import os
import sys
import time
import random
from os.path import join as J


def delay(ms, delta=500):
    wait = ms + random.randrange(delta) / 1000.0
    time.sleep(wait)


class RemoteFilesystem(object):
    class File(object):
        def __init__(self, name, filetype):
            self.name = name
            self.filetype = filetype

        def __repr__(self):
            return '{0}_{1}'.format(self.name, self.filetype)

    def list(self, path):
        delay(1, 500)
        items = []
        for x in os.listdir(path):
            typ = 'file' if os.path.isfile(os.path.join(path, x)) else 'dir'
            items.append(RemoteFilesystem.File(x, typ))
        return items

    def download_to(self, source, dest):
        print('Downloading {0}'.format(source))
        delay(0, 500)
        try:
            os.makedirs(os.path.dirname(dest))
        except OSError:
            pass
        with open(dest, 'w') as fdest:
            fdest.write(open(source).read())


def sync(source, dest):
    print('Scanning dir {0}'.format(source))
    remote = RemoteFilesystem()
    files = remote.list(source)
    for x in files:
        snext, dnext = J(source, x.name), J(dest, x.name)
        if x.filetype == 'file':
            remote.download_to(snext, dnext)
        else:
            sync(snext, dnext)


def main():
    source, dest = sys.argv[1:3]
    sync(source, dest)

if __name__ == '__main__':
    random.seed()
    main()
```

**Link to github.**

I intentionally keep the number of error checks at minimum to not to distract
you with non-related details. To simulate delays (connection establishment, data
transfer) in the original remote system I added artificial random delays. Let's
run our script and see how fast it works:

```
$ time python2 sync.py /home/bz/source dest
Scanning dir /home/bz/source
Scanning dir /home/bz/source/banana
Downloading /home/bz/source/banana/_header
Downloading /home/bz/source/banana/_description
Scanning dir /home/bz/source/banana/2013-11-23
Downloading /home/bz/source/banana/2013-11-23/banana-2013-11-23-1.tsv.gz
Downloading /home/bz/source/banana/2013-11-23/banana-2013-11-23-0.tsv.gz
Scanning dir /home/bz/source/banana/2013-11-22
Downloading /home/bz/source/banana/2013-11-22/banana-2013-11-22-0.tsv.gz
Downloading /home/bz/source/banana/2013-11-22/banana-2013-11-22-1.tsv.gz
Scanning dir /home/bz/source/orange
Downloading /home/bz/source/orange/_header
Downloading /home/bz/source/orange/_description
Scanning dir /home/bz/source/orange/2013-11-23
Downloading /home/bz/source/orange/2013-11-23/orange-2013-11-23-1.tsv.gz
Downloading /home/bz/source/orange/2013-11-23/orange-2013-11-23-0.tsv.gz
Scanning dir /home/bz/source/orange/2013-11-22
Downloading /home/bz/source/orange/2013-11-22/orange-2013-11-22-1.tsv.gz
Downloading /home/bz/source/orange/2013-11-22/orange-2013-11-22-0.tsv.gz
Scanning dir /home/bz/source/apple
Downloading /home/bz/source/apple/_header
Downloading /home/bz/source/apple/_description
Scanning dir /home/bz/source/apple/2013-11-23
Downloading /home/bz/source/apple/2013-11-23/apple-2013-11-23-1.tsv.gz
Downloading /home/bz/source/apple/2013-11-23/apple-2013-11-23-0.tsv.gz
Scanning dir /home/bz/source/apple/2013-11-22
Downloading /home/bz/source/apple/2013-11-22/apple-2013-11-22-0.tsv.gz
Downloading /home/bz/source/apple/2013-11-22/apple-2013-11-22-1.tsv.gz
python2 sync.py /home/bz/source dest  0.02s user 0.02s system 0% cpu 16.172 total
```

Not very fast indeed. We definitely can reduce total running time if we run the
sync in parallel. In my first try I decided to use thread-safe Queue module.  My
idea was to run directory listing in workers instead of the main thread. To do
this, instead of listing the directory immediately I put listing request into
the queue. I quickly realized that this model is not quite a traditionally
explained producer-consumer model. In a classic producer-consumer model
producers and consumers are different entities (even though there might be many
of each). This means that producer will never consume an item and vise versa.
In my case, however, it was not true.

When another directory is listed, it might return a list of files which will be
downloaded in the same thread. However it might also return a list of
subdirectories which will be added to the queue to scan later, possibly in a
different worker. Thus, the very first method which starts the syncronization
doesn't really know in advance what files and directories there will be and it
doesn't know when to stop processing. In my case consumer can produce more work
and we don't know if it happens in advance.

### Parallel try

Here is a new parallel version startup method. It creates a `queue`,
`long_running_items_left` counter of the currently running directory listings and a
global signal `transfer_done`:

``` python
def sync(self, source, dest):
    self.queue = Queue.Queue()
    self.transfer_done = threading.Event()
    self.long_running_items_left = Counter(1)
    N = 5

    for i in range(N):
        t = threading.Thread(target=self.sync_worker)
        t.daemon = True
        t.start()

    self.sync_dir(source, dest)

    self.transfer_done.set()
    while threading.active_count() > 1:
        time.sleep(0.1)

    self.queue.join()
```

As you can see, `join()` is not used to wait for the queue to drain. Instead,
`threading.active_count()` is used to check the number currently running
threads. First time I saw this hack in s3cmd [parallel][active-count-hack]
patch. It says:

``` python
    #Necessary to ensure KeyboardInterrupt can actually kill
    #Otherwise Queue.join() blocks until all queue elements have completed
    while threading.active_count() > 1:
        time.sleep(.1)
```

I didn't know at the moment why it was necessary and why would `join()` ignore
`KeyboardInterrupt` so I just copy-pasted it. Because of this hacky way to join
we now need to make sure that workers know when to die. In my case they need
to busy-loop waiting for the queue to fill with something and when they know
for sure that there will be no more work, worker should terminate.

This appeared to be tricky. How does a worker know when to terminate? To give
worker this knowledge, I introduced two new variables:

``` python
self.transfer_done = threading.Event()
self.long_running_items_left = Counter(1)
```

`transfer_done` is a global event. It is cleared before the first call to
`sync_dir` and it is set before waiting for the number of active threads to go
down to one.

`long_running_items_left` is a thread-safe counter. It is incemented
before `list_dir` and is decremented when `list_dir` is done. This counter
represents the number currently listing directories. The need for such counter
can be demonstrated in an example: suppose we're running 10 workers. We list
the first directory and a list of three subdirectories is returned. Three items
are added to the queue and they all are soon taken by three different workers.
However, the rest of the workers, seven of them, they try to get work from the
queue but it's empty. The worker can't tell if it's OK to die only by observing
the queue. We know that at the very moment there are three other workers
listing some directories and we need to provide this information to other
workers so that they don't terminate prematurely.

Also, because `sync_dir` is called in the startup method, the counter is
initialized with value 1:

``` python
self.long_running_items_left = Counter(1)
```

`Counter` is a simple thread-safe counter guarded with `threading.Lock`. I
couldn't find anything similar in the `threading` module so I invented my own:

``` python
class Counter(object):
    def __init__(self, initval=0):
        self.val = initval
        self.lock = threading.Lock()

    def inc(self, n=1):
        with self.lock:
            self.val += n
            return self.val

    def dec(self, n=1):
        with self.lock:
            self.val -= n
            return self.val

    def value(self):
        with self.lock:
            return self.val
```

Now when `sync_dir` sees another directory it will enqueue it instead of
immediate listing:

``` python
def sync_dir(self, source, dest):
    print('Scanning dir {0}'.format(source))
    remote = RemoteFilesystem()
    files = remote.list(source)
    for x in files:
        snext, dnext = J(source, x.name), J(dest, x.name)
        if x.filetype == 'file':
            self.download(snext, dnext)
        else:
            self.long_running_items_left.inc()
            self.queue.put((self.sync_dir, [snext, dnext]))
    self.long_running_items_left.dec()
```

`sync_worker` is executed by every worker. In this method we request another
piece of work from the queue and try to process it. As I mentioned before,
empty queue doesn't necessarily mean we can terminate. That's why we also
check `transfer_done` and `long_running_items_left`. If we extracted and successfully
processed a work, we mark task as done.

``` python
def sync_worker(self):
    while True:
        try:
            handler, args = self.queue.get_nowait()
            handler(*args)
        except Queue.Empty:
            if self.transfer_done.is_set() and (self.long_running_items_left.value() == 0):
                print('QUEUE IS EMPTY AND WE ARE DONE')
                return
            else:
                print('QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING')
                time.sleep(1.0)
                continue
        except Exception as e:
            print('EXCEPTION IN WORKER: {0}'.format(e))
        self.queue.task_done()
```

Here is a complete code in one piece (I also restructured the code a bit, moved
all syncer methods into the same class):

``` python
class Counter(object):
    def __init__(self, initval=0):
        self.val = initval
        self.lock = threading.Lock()

    def inc(self, n=1):
        with self.lock:
            self.val += n
            return self.val

    def dec(self, n=1):
        with self.lock:
            self.val -= n
            return self.val

    def value(self):
        with self.lock:
            return self.val


class Syncer(object):
    def sync(self, source, dest):
        self.queue = Queue.Queue()
        self.transfer_done = threading.Event()
        self.long_running_items_left = Counter(1)
        N = 5

        for i in range(N):
            t = threading.Thread(target=self.sync_worker)
            t.daemon = True
            t.start()

        self.sync_dir(source, dest)

        self.transfer_done.set()
        while threading.active_count() > 1:
            time.sleep(0.1)

        self.queue.join()

    def sync_worker(self):
        while True:
            try:
                handler, args = self.queue.get_nowait()
                handler(*args)
            except Queue.Empty:
                if self.transfer_done.is_set() and (self.long_running_items_left.value() == 0):
                    print('QUEUE IS EMPTY AND WE ARE DONE')
                    return
                else:
                    print('QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING')
                    time.sleep(1.0)
                    continue
            except Exception as e:
                print('EXCEPTION IN WORKER: {0}'.format(e))
            self.queue.task_done()

    def sync_dir(self, source, dest):
        print('Scanning dir {0}'.format(source))
        remote = RemoteFilesystem()
        files = remote.list(source)
        for x in files:
            snext, dnext = J(source, x.name), J(dest, x.name)
            if x.filetype == 'file':
                self.download(snext, dnext)
            else:
                self.long_running_items_left.inc()
                self.queue.put((self.sync_dir, [snext, dnext]))
        self.long_running_items_left.dec()

    def download(self, source, dest):
        print('Downloading {0}'.format(source))
        remote = RemoteFilesystem()
        remote.download_to(source, dest)
```

**Link to github.**

Let's run this version and see how fast it works with 5 parallel workers:

```
$ time python2 sync.py /home/bz/source dest
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
 Scanning dir /home/bz/source
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
 QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
 QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Scanning dir /home/bz/source/banana
Scanning dir /home/bz/source/orange
Scanning dir /home/bz/source/apple
 QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Downloading /home/bz/source/banana/_header
Downloading /home/bz/source/orange/_header
Downloading /home/bz/source/banana/_description
Scanning dir /home/bz/source/banana/2013-11-23
Downloading /home/bz/source/apple/_header
Downloading /home/bz/source/orange/_description
Downloading /home/bz/source/apple/_description
Scanning dir /home/bz/source/banana/2013-11-22
Scanning dir /home/bz/source/orange/2013-11-23
Scanning dir /home/bz/source/orange/2013-11-22
Scanning dir /home/bz/source/apple/2013-11-23
Downloading /home/bz/source/banana/2013-11-23/banana-2013-11-23-1.tsv.gz
Downloading /home/bz/source/banana/2013-11-22/banana-2013-11-22-0.tsv.gz
Downloading /home/bz/source/orange/2013-11-23/orange-2013-11-23-1.tsv.gz
Downloading /home/bz/source/banana/2013-11-23/banana-2013-11-23-0.tsv.gz
Downloading /home/bz/source/banana/2013-11-22/banana-2013-11-22-1.tsv.gz
Scanning dir /home/bz/source/apple/2013-11-22
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Downloading /home/bz/source/orange/2013-11-23/orange-2013-11-23-0.tsv.gz
Downloading /home/bz/source/apple/2013-11-23/apple-2013-11-23-1.tsv.gz
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Downloading /home/bz/source/orange/2013-11-22/orange-2013-11-22-1.tsv.gz
Downloading /home/bz/source/apple/2013-11-23/apple-2013-11-23-0.tsv.gz
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Downloading /home/bz/source/orange/2013-11-22/orange-2013-11-22-0.tsv.gz
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Downloading /home/bz/source/apple/2013-11-22/apple-2013-11-22-0.tsv.gz
QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING
Downloading /home/bz/source/apple/2013-11-22/apple-2013-11-22-1.tsv.gz
QUEUE IS EMPTY AND WE ARE DONE
QUEUE IS EMPTY AND WE ARE DONE
QUEUE IS EMPTY AND WE ARE DONE
QUEUE IS EMPTY AND WE ARE DONE
QUEUE IS EMPTY AND WE ARE DONE
python2 sync.py /home/bz/source dest  0.03s user 0.01s system 0% cpu 7.527 total
```

### Less variables

The very first thing that brings attention is this `transfer_done` variable. I
don't know what I was thinking but it is obvious that `long_running_items_left`
totally superseds it. Initializing `long_running_items_left` counter with value
1 and decrementing it before `join()` makes `transfer_done` completely useless.
This is the first thing to throw away.

The use of `long_running_items_left` variable does not seem right. It is used in
`sync_dir` method, while it is better to stick such variables to the worker
code. So instead, let's move `long_running_items_left` to the `sync_worker`
method. We will increment the value before the handler and decrement it after
the work is done.

Another thing to mention is explicit `time.sleep(1.0)` in `sync_worker`.
Instead, we can use blocking `Queue.get` and specify wait timeout. After that
we can safely remove `time.sleep` call. Here is a complete code:

``` python
def sync(self, source, dest):
    self.queue = Queue.Queue()
    self.long_running_items_left = Counter(1)
    N = 5

    for i in range(N):
        t = threading.Thread(target=self.sync_worker)
        t.daemon = True
        t.start()

    self.sync_dir(source, dest)
    self.long_running_items_left.dec()

    while threading.active_count() > 1:
        time.sleep(0.1)

    self.queue.join()

def sync_worker(self):
    while True:
        try:
            handler, args = self.queue.get(True, 1.0)
            self.long_running_items_left.inc()
            handler(*args)
        except Queue.Empty:
            if self.long_running_items_left.value() == 0:
                print('QUEUE IS EMPTY AND WE ARE DONE')
                return
            else:
                print('QUEUE IS EMPTY BUT WE ARE NOT DONE, SLEEPING')
                continue
        except Exception as e:
            print('EXCEPTION IN WORKER: {0}'.format(e))
        self.long_running_items_left.dec()
        self.queue.task_done()

def sync_dir(self, source, dest):
    print('Scanning dir {0}'.format(source))
    remote = RemoteFilesystem()
    files = remote.list(source)
    for x in files:
        snext, dnext = J(source, x.name), J(dest, x.name)
        if x.filetype == 'file':
            self.download(snext, dnext)
        else:
            self.queue.put((self.sync_dir, [snext, dnext]))
```

**Link to github.**

### Poison pill

This looks a little better: we narrowed down the use of
`long_running_items_left`, removed unnecessary `transfer_done` and got rid of
explicit `time.sleep`. However, there is a problem with this approach. It's
hidden in this pair of lines:

``` python
handler, args = self.queue.get(True, 1.0)
self.long_running_items_left.inc()
```

The problem with this code is that we use two different synchronization mechanisms
simultaneously and it's not atomic. *JUSTIFY NON-ATOMICITY, GIVE MORE EXAMPLES*.

Can we do better? Yes we can if we recall "poison pill" technique. The trick
is, instead of busy waiting in the worker, to send the worker a special
message. When worker receives such a message, it means there's no more work to
do here and it quits.


[producer-consumer]: http://en.wikipedia.org/wiki/Producer%E2%80%93consumer_problem
[s3cmd]: http://s3tools.org/s3cmd
[s3]: http://aws.amazon.com/s3/
[s3cmdparallel1]: https://github.com/pcorliss/s3cmd-modification
[s3cmdparallel2]: https://github.com/Pearltrees/s3cmd-modification
[issue]: https://github.com/s3tools/s3cmd/issues/2
[active-count-hack]: https://github.com/pcorliss/s3cmd-modification/commit/9a65f78ea9e2a6633558a9462dc285c9ce1d943a#diff-c2214fd9d0677e2abe4e25e9a8950a2cR72

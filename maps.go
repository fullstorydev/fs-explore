package main

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"
)

// Like sync.Locker except that the locking is done at the granularity of a (string) key.
type KeyLocker interface {
	Lock(ctx context.Context, key string)
	Unlock(key string)
}

type countedLock struct {
	refcount int64 // should only be accessed while the outer _map_ lock is held (not this key lock)
	lock     chan interface{}
}

func newCountedLock() *countedLock {
	l := &countedLock{
		lock: make(chan interface{}, 1),
	}
	l.lock <- 1 // value doesn't matter
	return l
}

// TransientLockMap is a map of mutexes that is safe for concurrent access.  It does not bother to save mutexes after
// they have been unlocked, and thus this data structure is best for situations where the space of keys is very large.
// If the space of keys is small then it may be inefficient to constantly recreate mutexes whenever they are needed.
type TransientLockMap struct {
	mu    sync.Mutex              // the mutex that locks the map
	locks map[string]*countedLock // all locks that are currently held

	nursery sync.Pool // efficiently creates countedLocks as needed
}

func NewTransientLockMap() *TransientLockMap {
	return &TransientLockMap{
		locks:   make(map[string]*countedLock),
		nursery: sync.Pool{New: func() interface{} { return newCountedLock() }},
	}
}

// Lock acquires the lock for the specified key and returns true, unless the context finishes before the lock could be
// acquired, in which case false is returned.
func (l *TransientLockMap) Lock(ctx context.Context, key string) bool {
	// If there is high lock contention, we could use a readonly lock to check if the lock is already in the map (and
	// thus no map writes are necessary), but this is complicated enough as it is so we skip that optimization for now.
	l.mu.Lock()

	var lock *countedLock
	var ok bool

	// Check if there is already a lock for this key.
	if lock, ok = l.locks[key]; !ok {
		// no lock yet, so make one and add it to the map
		lock = l.nursery.Get().(*countedLock)
		l.locks[key] = lock
	}

	// Order is very important here.  First we have to increment the refcount while we still have the map locked; this
	// will prevent anyone else from evicting this lock after we unlock the map but before we lock the key.  Second we
	// have to unlock the map _before_ we start trying to lock the key (because locking the key could take a long time
	// and we don't want to keep the map locked that whole time).
	lock.refcount++ // incremented while holding _map_ lock
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		return false
	case <-lock.lock:
		return true
	}
}

func (l *TransientLockMap) Unlock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lock, ok := l.locks[key]; ok {
		// common case - lock is present in the map
		lock.refcount-- // decremented while holding _map_ lock
		if lock.refcount < 0 {
			panic(fmt.Sprintf("BUG: somehow the lock.refcount for %q dropped to %d", key, lock.refcount))
		}
		if lock.refcount == 0 {
			delete(l.locks, key)
		}
		lock.lock <- 1 // value doesn't matter
		return
	}

	// else, key not found in locks{} map - boom time
	panic(fmt.Sprintf("lock not held for key %s", key))
}

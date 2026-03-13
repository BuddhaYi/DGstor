package dir

import (
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"dgstor/common"
	"sync"
	"time"
)

type VolumeConnectionPool struct{
	*common.ConnectionPool
	volumeName string

	//A copy of volume
	volume     *Volume

	mu         sync.RWMutex
	dirConn    *common.Connection
	released   bool
}

func MakeVolumeConnectionPool(volumeName string, dirConn *common.Connection) *VolumeConnectionPool{
	pool := &VolumeConnectionPool{ConnectionPool: common.MakeConnectionPool(),volumeName: volumeName,dirConn: dirConn}
	pool.updateVolume()
	pool.released = false
	pool.autoUpdate()
	return pool
}
func (pool *VolumeConnectionPool) GetVolume() *Volume{
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.volume
}
//TODO:加锁
func (pool *VolumeConnectionPool) GetConnectionByBrick(brickId uuid.UUID) *common.Connection{
	pool.mu.RLock()
	brick,ok := pool.volume.Bricks[brickId]
	for !ok || brick == nil {
		pool.mu.RUnlock()
		time.Sleep(time.Second)
		pool.mu.Lock()
		pool.updateVolume()
		pool.mu.Unlock()
		pool.mu.RLock()
		brick,ok = pool.volume.Bricks[brickId]
	}
	pool.mu.RUnlock()
	return pool.GetConnection(brick.Addr())
}

func (pool *VolumeConnectionPool) GetClientConnectionById(id int) *common.Connection{
	pool.mu.RLock()
	server := pool.volume.HttpServers[id]
	for !server.IsRegistered() {
		pool.mu.RUnlock()
		time.Sleep(time.Second)
		pool.updateVolume()
		pool.mu.RLock()
		server = pool.volume.HttpServers[id]
	}
	pool.mu.RUnlock()
	return pool.GetConnection(server.ClientAddr())
}

func (pool *VolumeConnectionPool) GetDirConnection() *common.Connection{
	return pool.dirConn
}
//TODO:加锁
//在 updateVolume 函数的开头和结尾加锁不是一个好的选择。这是因为在调用该函数时，可能会阻塞其他goroutine对连接池的访问，从而降低程序的并发性能。
//如果在该函数的开头和结尾加锁，则会出现以下情况：
//
//当一个goroutine正在执行这个函数时，其他goroutine想要访问连接池时，需要等待该函数执行完成才能进行访问。这会导致其他goroutine被阻塞。
//当该函数执行较长时间时，其他goroutine需要等待相当长的时间才能访问连接池，这会导致程序的响应时间变慢。
//因此，应该只在需要修改连接池的部分加锁，而不是在整个函数的开头和结尾加锁。这样可以最大化地保持程序的并发性能，同时又能保证连接池的操作是线程安全的。
func (pool *VolumeConnectionPool) updateVolume() {
	var volume Volume
	err := pool.dirConn.Call("DirectoryService.GetVolume",&pool.volumeName,&volume)
	if err != nil {
		log.Errorln("Error getting volume when auto updating:",err)
	}
	pool.mu.Lock()

	if pool.volume != nil {
		for id, client := range volume.HttpServers {
			if id < len(pool.volume.HttpServers) && pool.volume.HttpServers[id].IsRegistered() && pool.volume.HttpServers[id].ClientAddr() != client.ClientAddr() {
				pool.ResetAddress(pool.volume.HttpServers[id].ClientAddr(), client.ClientAddr())
			}
		}

		for id, brick := range volume.Bricks {
			oldBrick, ok := pool.volume.Bricks[id]
			if ok && oldBrick.Addr() != brick.Addr() {
				pool.ResetAddress(oldBrick.Addr(), brick.Addr())
			}
		}
	}

	pool.mu.Unlock()
	//在这段代码中，更新操作可能会涉及到多个goroutine同时操作pool.volume，因此需要保证这个操作的原子性。
	//使用单独的锁pool.mu.Lock()和pool.mu.Unlock()来锁定这个操作，可以保证这个操作的原子性。
	//如果不加锁，当多个goroutine同时更新pool.volume时，可能会出现竞态条件，导致数据不一致的情况。
	pool.mu.Lock()
	pool.volume = &volume
	pool.mu.Unlock()
}

func (pool *VolumeConnectionPool) Release() {
	pool.mu.Lock()
	pool.released = true
	pool.ConnectionPool.Release()
	pool.mu.Unlock()
}

func (pool *VolumeConnectionPool) autoUpdate() {
	go func() {
		for {
			time.Sleep(common.HeartBeatInterval)
			pool.mu.Lock()
			released := pool.released
			pool.mu.Unlock()
			if released{
				return
			}
			pool.updateVolume()
		}
	}()
}


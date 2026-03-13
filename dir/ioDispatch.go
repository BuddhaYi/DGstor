package dir

import (
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"dgstor/common"
)

type MoveInstruct struct {
	PG     PlacementGroup
	Broken int
	NewBrick  uuid.UUID
}

type RecoverReply []MoveInstruct

func (volume *Volume) findMin(io map[uuid.UUID]float64, ioServer []int, broken uuid.UUID) (uuid.UUID, float64) {

	var res uuid.UUID
	minS := math.MaxInt32
	minIO := math.MaxFloat64

	//brokenServer := volume.getBrickServerIndex(broken)

	for k, v := range io {
		server := volume.getBrickServerIndex(k)

			if ioServer[server] < minS {
				minS = ioServer[server]
				minIO = v
				res = k
			} else if ioServer[server] == minS && v < minIO {
				minIO = v
				res = k
			} else if ioServer[server] == minS && v == minIO && k.String() < res.String() {
				res = k
			}

	}
	return res, minIO
}

func printIO(io map[uuid.UUID]float64) {
	for k, v := range io {
		log.Infoln(k, "io:", v)
	}
}

func (volume *Volume) getBrickServerIndex(brickId uuid.UUID) int {
	for index,server := range volume.Servers {
		if server.IP == volume.Bricks[brickId].IP{
			return index
		}
	}
	return -1
}

func (volume *Volume) getIthBrick(i int) (int,uuid.UUID) {
	if i < 0 {
		panic("Invalid index")
	}
	index :=0
	for serverId,server := range volume.Servers {
		if i - index < len(server.BrickId) {
			return serverId,server.BrickId[i - index]
		}
		index += len(server.Dir)
	}
	panic("Brick not found")
}

var randGenerator *rand.Rand

func init() {
	randGenerator = rand.New(rand.NewSource(0))
}

func shuffle(bricks []uuid.UUID) {
	n := len(bricks)
	for i := 0; i+1 < n; i++ {
		j := int(randGenerator.Int31n(int32(n-i))) + i
		if j != i {
			bricks[i], bricks[j] = bricks[j], bricks[i]
		}
	}
}

//For each brick,we need to make sure the number of bricks related to it is maximal.
func (volume *Volume) assignPGs() {
	volume.PGs = make([]PlacementGroup, volume.Parameter.NumberPG)
	servers := len(volume.Servers)

	bricks := len(volume.Bricks)

	graph := make([][]int, bricks)
	for i := 0; i < bricks; i++ {
		graph[i] = make([]int, bricks)
		for j := 0; j < bricks; j++ {
			graph[i][j] = 0
		}
	}

	pv_id := make([]uint16, bricks)

	for i := 0; i < volume.Parameter.NumberPG; i++ {
		pg := PlacementGroup{}
		// Allocate K + Redundancy + loc * localParityPerGroup bricks per PG
		pg.Bricks = make([]uuid.UUID, volume.Parameter.TotalBricksPerPG())
		pg.Version = 0

		serUsed := make([]int, servers)
		bUsed := make(map[int]bool)
		for j := 0; j < servers; j++ {
			serUsed[j] = 0
		}

		choosed := make([]int, 0)

		for j := 0; j < len(pg.Bricks); j++ {
			minUsed := common.MinArray(serUsed)

			minConnected := math.MaxInt32
			minPV := uint16(math.MaxInt16)
			toChoose := 0
			for t := 0; t < bricks; t++ {
				if pv_id[t] <= minPV {
					s,_ := volume.getIthBrick(t)
					used, exist := bUsed[t]
					if exist && used {
						continue
					}

					if serUsed[s] == minUsed {
						conn := 0
						for k := 0; k < len(choosed); k++ {
							if graph[t][choosed[k]] > conn {
								conn = graph[t][choosed[k]]
							}
						}

						if (pv_id[t] < minPV) || (conn < minConnected && pv_id[t] == minPV) {
							minConnected = conn
							minPV = pv_id[t]
							toChoose = t
						}
					}
				}
			}
			s,_ := volume.getIthBrick(toChoose)
			serUsed[s]++
			bUsed[toChoose] = true

			choosed = append(choosed, toChoose)
		}

		for j := 0; j < len(choosed); j++ {
			for k := 0; k < len(choosed); k++ {
				if j != k {
					graph[choosed[j]][choosed[k]]++
				}
			}
		}

		for j := 0; j < len(choosed); j++ {
			_,brickId := volume.getIthBrick(choosed[j])
			pg.Bricks[j] = brickId
			pv_id[choosed[j]]++
		}

		shuffle(pg.Bricks)
		//将前三个节点作为存储索引的节点
		for j := 0; j < common.REPLICATION; j++ {
			pg.Index[j] = pg.Bricks[j]
		}

		pg.PGId = uint32(i)

		volume.PGs[i] = pg
	}
}

type RecoveryDispatchArgs struct {
	BrokenBrick uuid.UUID
	VolumeName  string
}

func (volume *Volume) dispatch(args *RecoveryDispatchArgs, reply *RecoverReply) error {
	io := make(map[uuid.UUID]float64)// 创建一个空的 map，用于记录每个存储节点的 IO 情况
	res := make(RecoverReply, 0)// 创建一个空的恢复指令序列，用于记录数据迁移操作
	// 遍历 Volume 的每个存储节点
	for brickID,_ := range volume.Bricks {
		// 将每个存储节点的 IO 设置为 0，但排除坏掉的节点
		if brickID != args.BrokenBrick {
			io[brickID] = 0
		}
	}
	// 遍历 Volume 的每个 PG（placement group）
	for i := 0; i < len(volume.PGs); i++ {
		pg := volume.PGs[i].Bricks // 获取 PG 中的存储节点列表

		broken := -1// 初始化坏掉的存储节点编号为 -1
		// 找到坏掉的存储节点
		for j := 0; j < len(pg); j++ {
			if pg[j]  == args.BrokenBrick {
				broken = j
				break
			}
		}
		// Compute repair cost based on node role.
		// Data nodes use local repair (low cost), parity nodes use global repair (higher cost).
		k := volume.Parameter.K
		r := volume.Parameter.Redundancy
		cost := 1.0
		if broken >= 0 && broken < k {
			cost = 1.0 // data node: local group repair
		} else if broken < k+r {
			cost = 1.4 // global parity node: global repair
		} else {
			cost = 2.0 // local parity node: global repair
		}
		// 如果 PG 中有存储节点坏掉，就将该 PG 中的其他存储节点的 IO 值加上迁移代价
		if broken >= 0 {
			for j := 0; j < len(pg); j++ {
				if pg[j] != args.BrokenBrick {
					currentIo, exist := io[pg[j]]
					if !exist {
						io[pg[j]] = cost
					} else {
						io[pg[j]] = currentIo + cost
					}
				}
			}
		} else {
			continue // 如果 PG 中没有坏掉的存储节点，就继续处理下一个 PG
		}
	}
	// 初始化每个存储节点所在的服务器的 IO 值为 0
	ioServer := make([]int,len(volume.Servers))
	for i, _ := range volume.Servers {
		ioServer[i] = 0
	}
	// 遍历 Volume 的每个 PG
	for i := 0; i < len(volume.PGs); i++ {
		pg := volume.PGs[i].Bricks
		broken := -1
		for j := 0; j < len(pg); j++ {
			if pg[j]  == args.BrokenBrick {    //reply.Servers[0].BrickId[0]
				broken = j
				break
			}
		}
		if broken >= 0 {
			minBrick, cnt := volume.findMin(io, ioServer, args.BrokenBrick)
			io[minBrick] = cnt + 4.0 //??? RS:1, MSR:2 or 3 or 4, LRC:1
			ioServer[volume.getBrickServerIndex(minBrick)]++
			res = append(res, MoveInstruct{PG: volume.PGs[i], Broken: broken, NewBrick: minBrick})
		}
	}

	//printIO(io)

	*reply = res
	return nil
}

func (service *DirectoryService) RecoverDispatch(args *RecoveryDispatchArgs, reply *RecoverReply) error {

	loaded, ok := service.Volumes.Load(args.VolumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	return volume.dispatch(args,reply)

}


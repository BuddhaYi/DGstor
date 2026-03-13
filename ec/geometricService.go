package ec

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"io"
	"dgstor/common"
	"dgstor/dir"
	"dgstor/encoder"
	"dgstor/indexservice"
	"dgstor/storageservice"
	"sync"
	"time"
)

type task struct {
	i       int
	blockID int16
}
type IndexedBuffer struct {
	Index  int
	Buffer common.IOBuffer
}
type GeometricService struct {
	minBlockEncoder *encoder.RSEncoder
	gloencoder      *encoder.MSREncoder
	locMSREncoder   *encoder.MSREncoder // MSR-based local encoder (used when localEncoderType == "MSR")
	locRSEncoder    *encoder.RSEncoder  // RS-based local encoder (used when localEncoderType == "RS")
	pool            *dir.VolumeConnectionPool
	bufPool         *common.IOBufferPool

	partitioner *index.GeometricPartitioner

	k          int
	redundancy int

	loc                 int    // number of local groups
	localParityPerGroup int    // 2 for MSR, 1 for RS
	localEncoderType    string // "MSR" or "RS"
	kLocal              int    // k / loc
	nLocal              int    // kLocal + localParityPerGroup

	timestat common.TimeStats
}

func MakeGeometricService(pool *dir.VolumeConnectionPool) *GeometricService {
	service := &GeometricService{}

	volume := pool.GetVolume()
	parameter := volume.Parameter

	service.k = parameter.K
	service.redundancy = parameter.Redundancy

	// Read loc from config with default
	service.loc = parameter.EffectiveLocalRedundancy()
	service.kLocal = service.k / service.loc

	// Determine local encoder type from config
	service.localEncoderType = parameter.LocalEncoderType
	if service.localEncoderType == "" {
		service.localEncoderType = "MSR"
	}

	service.pool = pool
	service.bufPool = common.MakeIOBufferPool()

	// RS encoder for small blocks (blockId == 0)
	service.minBlockEncoder = encoder.MakeRSEncoder(service.redundancy+service.k, service.k)

	// Global MSR encoder
	service.gloencoder = encoder.MakeMSREncoder(service.k+service.redundancy, service.k, service.loc, service.bufPool)

	// Local encoder: MSR or RS based on config
	if service.localEncoderType == "MSR" {
		service.localParityPerGroup = 2
		service.nLocal = service.kLocal + service.localParityPerGroup
		service.locMSREncoder = encoder.MakeMSREncoder(service.nLocal, service.kLocal, service.loc, service.bufPool)
	} else {
		service.localParityPerGroup = 1
		service.nLocal = service.kLocal + service.localParityPerGroup
		service.locRSEncoder = encoder.MakeRSEncoder(service.nLocal, service.kLocal)
	}

	service.partitioner = &index.GeometricPartitioner{
		MinBlock: parameter.MinBlockSize,
		MaxBlock: parameter.MaxBlockSize,
		Base:     parameter.GeometricBase,
	}

	return service
}

// totalNodes returns k + redundancy (global parity count, excluding local parity).
func (service *GeometricService) totalNodes() int {
	return service.k + service.redundancy
}

// totalBricks returns the total number of bricks per PG including local parity.
func (service *GeometricService) totalBricks() int {
	return service.totalNodes() + service.loc*service.localParityPerGroup
}

// localRepairMSR performs local repair using MSR regeneration within a local group.
func (service *GeometricService) localRepairMSR(blockId int, offset uint64, size uint64, broken int, PG dir.PlacementGroup, indices []int) common.IOBuffer {
	logrus.Warnln("localRepairMSR: PGId:", PG.PGId, "blockId:", blockId, "offset:", offset, "size:", size, "broken:", broken)

	nLocal := service.nLocal
	lrcdata := make([]common.IOBuffer, nLocal)
	groupStart := (broken / service.kLocal) * service.kLocal
	localBroken := broken - groupStart
	// Use the LOCAL MSR encoder's GetRegenerateOffset with the local index (0..nLocal-1)
	// The (nLocal, kLocal) MSR code has different alpha/beta than the (n, k) global code
	lrcoffs := service.locMSREncoder.GetRegenerateOffset(localBroken, offset, size)

	var wg sync.WaitGroup
	wg.Add(len(indices))
	for _, idx := range indices {
		if idx != broken {
			go func(idx int) {
				defer wg.Done()
				var args stor.GetBatchDataArgs
				args.PGId = int(PG.PGId)
				for _, v := range lrcoffs {
					args.BlockId = append(args.BlockId, int16(blockId))
					args.Offset = append(args.Offset, v.Offset)
					args.Size = append(args.Size, uint32(v.Size))
				}
				conn := service.pool.GetConnectionByBrick(PG.Bricks[idx])
				stor.Compress(&args)

				// Map PG index to local encoder index
				localIdx := idx - groupStart
				if idx >= service.totalNodes() {
					// Local parity node: map to positions after kLocal in local array
					localParityStart := service.totalNodes() + (broken/service.kLocal)*service.localParityPerGroup
					localIdx = service.kLocal + (idx - localParityStart)
				}
				lrcdata[localIdx] = getData(&args, conn, service.bufPool)
			}(idx)
		} else {
			wg.Done()
		}
	}
	wg.Wait()

	start := time.Now()
	lrcBroken := service.bufPool.GetBuffer(int(size))
	service.locMSREncoder.Regenerate(lrcdata, lrcBroken)
	for j := 0; j < nLocal; j++ {
		if lrcdata[j].Data != nil && j != (broken-groupStart) {
			lrcdata[j].Unref()
		}
	}
	logrus.Println("Local MSR Regenerate cost:", time.Since(start))
	return lrcBroken
}

// localRepairRS performs local repair using RS decoding within a local group.
func (service *GeometricService) localRepairRS(blockId int, offset uint64, size uint64, broken int, PG dir.PlacementGroup, indices []int) common.IOBuffer {
	logrus.Warnln("localRepairRS: PGId:", PG.PGId, "blockId:", blockId, "offset:", offset, "size:", size, "broken:", broken)

	nLocal := service.nLocal
	data := make([]common.IOBuffer, nLocal)
	groupStart := (broken / service.kLocal) * service.kLocal

	var wg sync.WaitGroup
	wg.Add(len(indices) - 1) // exclude broken

	for _, idx := range indices {
		if idx != broken {
			go func(idx int) {
				defer wg.Done()
				var args stor.GetBatchDataArgs
				args.PGId = int(PG.PGId)
				args.BlockId = append(args.BlockId, int16(blockId))
				args.Offset = append(args.Offset, offset)
				args.Size = append(args.Size, uint32(size))
				conn := service.pool.GetConnectionByBrick(PG.Bricks[idx])

				// Map PG index to local encoder index
				localIdx := idx - groupStart
				if idx >= service.totalNodes() {
					localParityStart := service.totalNodes() + (broken/service.kLocal)*service.localParityPerGroup
					localIdx = service.kLocal + (idx - localParityStart)
				}
				data[localIdx] = getData(&args, conn, service.bufPool)
			}(idx)
		}
	}
	wg.Wait()

	start := time.Now()
	brokenLocalIdx := broken - groupStart
	outputs := make([]common.IOBuffer, nLocal-service.kLocal)
	for i := range outputs {
		outputs[i] = service.bufPool.GetBuffer(int(size))
	}
	service.locRSEncoder.Encode(data, outputs)

	result := data[brokenLocalIdx]
	for j := 0; j < nLocal; j++ {
		if j != brokenLocalIdx && data[j].Data != nil {
			data[j].Unref()
		}
	}
	logrus.Println("Local RS Reconstruct cost:", time.Since(start))
	return result
}

// localRepair dispatches to MSR or RS local repair based on config.
func (service *GeometricService) localRepair(blockId int, offset uint64, size uint64, broken int, PG dir.PlacementGroup, groupIndex int) common.IOBuffer {
	n := service.totalNodes()
	groupStart := groupIndex * service.kLocal

	// Build indices: data nodes in this group + local parity nodes for this group
	indices := make([]int, 0, service.nLocal)
	for i := groupStart; i < groupStart+service.kLocal; i++ {
		indices = append(indices, i)
	}
	localParityStart := n + groupIndex*service.localParityPerGroup
	for i := 0; i < service.localParityPerGroup; i++ {
		indices = append(indices, localParityStart+i)
	}

	if service.localEncoderType == "MSR" {
		return service.localRepairMSR(blockId, offset, size, broken, PG, indices)
	}
	return service.localRepairRS(blockId, offset, size, broken, PG, indices)
}

// globalRepair performs global MSR repair reading from all n nodes.
func (service *GeometricService) globalRepair(blockId int, offset uint64, size uint64, broken int, PG dir.PlacementGroup) common.IOBuffer {
	n := service.totalNodes()
	logrus.Warnln("globalRepair: PGId:", PG.PGId, "blockId:", blockId, "offset:", offset, "size:", size, "broken:", broken)

	var wg sync.WaitGroup
	data := make([]common.IOBuffer, n)
	offs := service.gloencoder.GetRegenerateOffset(broken, offset, size)

	wg.Add(n - 1)
	for j := 0; j < n; j++ {
		if j != broken {
			go func(j int) {
				defer wg.Done()
				var args stor.GetBatchDataArgs
				args.PGId = int(PG.PGId)
				for _, v := range offs {
					args.BlockId = append(args.BlockId, int16(blockId))
					args.Offset = append(args.Offset, v.Offset)
					args.Size = append(args.Size, uint32(v.Size))
				}
				conn := service.pool.GetConnectionByBrick(PG.Bricks[j])
				stor.Compress(&args)
				data[j] = getData(&args, conn, service.bufPool)
			}(j)
		}
	}
	wg.Wait()

	start := time.Now()
	dataBroken := service.bufPool.GetBuffer(int(size))
	service.gloencoder.Regenerate(data, dataBroken)
	for j := 0; j < n; j++ {
		if j != broken && data[j].Data != nil {
			data[j].Unref()
		}
	}
	logrus.Println("Global MSR Regenerate cost:", time.Since(start))
	return dataBroken
}

// repairLocalParity repairs a local parity node by re-encoding from the group's data nodes.
// groupIndex: which local group (0..loc-1), parityIndexInGroup: which parity within the group (0..localParityPerGroup-1).
func (service *GeometricService) repairLocalParity(blockId int, offset uint64, size uint64, PG dir.PlacementGroup, groupIndex int, parityIndexInGroup int) common.IOBuffer {
	kLocal := service.kLocal
	nLocal := service.nLocal
	groupStart := groupIndex * kLocal

	logrus.Warnln("repairLocalParity: PGId:", PG.PGId, "blockId:", blockId, "group:", groupIndex, "parityIdx:", parityIndexInGroup)

	// Read the kLocal data nodes in this group
	localData := make([]common.IOBuffer, nLocal)
	var wg sync.WaitGroup
	wg.Add(kLocal)
	for i := 0; i < kLocal; i++ {
		go func(i int) {
			defer wg.Done()
			pgIdx := groupStart + i
			var args stor.GetBatchDataArgs
			args.PGId = int(PG.PGId)
			args.BlockId = append(args.BlockId, int16(blockId))
			args.Offset = append(args.Offset, offset)
			args.Size = append(args.Size, uint32(size))
			conn := service.pool.GetConnectionByBrick(PG.Bricks[pgIdx])
			localData[i] = getData(&args, conn, service.bufPool)
		}(i)
	}
	wg.Wait()

	// Parity positions are nil (to be generated by encoding)
	for i := kLocal; i < nLocal; i++ {
		localData[i] = common.IOBuffer{}
	}

	localOutputs := make([]common.IOBuffer, service.localParityPerGroup)
	for i := range localOutputs {
		localOutputs[i] = service.bufPool.GetBuffer(int(size))
	}

	if service.localEncoderType == "MSR" {
		service.locMSREncoder.LocEncode(localData, localOutputs, service.localParityPerGroup)
	} else {
		service.locRSEncoder.Encode(localData, localOutputs)
	}

	// Free data node buffers
	for i := 0; i < kLocal; i++ {
		if localData[i].Data != nil {
			localData[i].Unref()
		}
	}
	// Free other parity outputs we don't need
	result := localOutputs[parityIndexInGroup]
	for i := 0; i < service.localParityPerGroup; i++ {
		if i != parityIndexInGroup && localOutputs[i].Data != nil {
			localOutputs[i].Unref()
		}
	}

	logrus.Infoln("Local parity repair completed for group", groupIndex, "parity", parityIndexInGroup)
	return result
}

func (service *GeometricService) WriteResponse(w io.Writer, objectID uint64, offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex, broken int) {

	readBlock := make(chan common.IOBuffer, 20)
	finished := make(chan bool)
	var transferTime time.Duration
	go func() {

		for buff := range readBlock {
			begin := time.Now()
			n, err := w.Write(buff.Data)
			transferTime += time.Since(begin)
			if n != len(buff.Data) || err != nil {
				logrus.Errorln(err)
			}
			buff.Unref()
		}
		logrus.Infoln("Transfer time consumed on object", objectID, transferTime.String())
		finished <- true
	}()

	var blocks chan common.IOBuffer
	// Check if ANY block of this object resides on the broken node
	hasBrokenBlock := false
	for _, blk := range objectIndex.Geometric.Blocks {
		if int(blk.IndexInPG) == broken {
			hasBrokenBlock = true
			break
		}
	}
	if hasBrokenBlock {
		blocks = service.RegenerateObject(PG, objectIndex.Geometric, offset, size, broken)
		logrus.Infoln("begin to recieve blocks")
	} else {
		blocks = service.Get(offset, size, PG, objectIndex)
	}
	var repairTime time.Duration
	blocksList := []common.IOBuffer{}
	for {
		begin := time.Now()
		block, proceeding := <-blocks
		repairTime += time.Since(begin)
		if !proceeding {
			break
		}

		blocksList = append(blocksList, block)
	}
	if len(blocksList) > 0 {
		// Process all the blocks except the last one
		for _, block := range blocksList[:len(blocksList)-1] {
			readBlock <- block
		}

		// Process the last block: strip padding using PaddSize from index
		lastBlock := blocksList[len(blocksList)-1]
		lastIdx := len(objectIndex.Geometric.Blocks) - 1
		paddSize := objectIndex.Geometric.Blocks[lastIdx].PaddSize
		if paddSize > 0 && uint64(lastBlock.Len()) > paddSize {
			trimmed := lastBlock.Slice(0, uint64(lastBlock.Len())-paddSize)
			readBlock <- trimmed
		} else {
			readBlock <- lastBlock
		}
	}
	//readBlock
	close(readBlock)
	logrus.Infoln("Repair time consumed on object", objectID, repairTime.String())

	<-finished

	service.timestat.AddTime(repairTime, transferTime)
	logrus.Printf("total repair time:%d ns, total transfer time:%d ns.\n", repairTime, transferTime)
	rep, trans := service.timestat.Average()
	logrus.Printf("Avg repair time:%d ns, avg transfer time:%d ns.\n", rep, trans)
}

func (service *GeometricService) RepairBlocks(blockId int, offset uint64, size uint64, pg dir.PlacementGroup, broken int, shuffled bool) []common.IOBuffer {
	n := service.totalNodes()
	k := service.k
	loc := service.loc
	kLocal := service.kLocal
	totalBricks := service.totalBricks()

	var wg sync.WaitGroup

	data := make([]common.IOBuffer, totalBricks)
	wg.Add(k)

	order := make([]int, n)
	if shuffled {
		order = common.OrderShuffle(n)
	} else {
		for i := 0; i < n; i++ {
			order[i] = i
		}
	}

	asked := 0
	for i := 0; i < n && asked < k; i++ {
		if order[i] != broken {
			asked++
			go func(brickNum int) {
				defer wg.Done()
				conn := service.pool.GetConnectionByBrick(pg.Bricks[brickNum])
				var args stor.GetBatchDataArgs
				args.BlockId = append(args.BlockId, int16(blockId))
				args.PGId = int(pg.PGId)
				args.Offset = append(args.Offset, offset)
				args.Size = append(args.Size, uint32(size))
				data[brickNum] = getData(&args, conn, service.bufPool)
			}(order[i])
		}
	}
	wg.Wait()

	if blockId != 0 {
		// Global MSR encoding: generate global parity from k data nodes
		globalOutputs := make([]common.IOBuffer, n-k)
		for i := range globalOutputs {
			globalOutputs[i] = service.bufPool.GetBuffer(int(size))
		}
		service.gloencoder.Encode(data[:n], globalOutputs)
		for i := 0; i < n-k; i++ {
			data[k+i] = globalOutputs[i]
		}
		logrus.Infoln("Global parity generated")

		// Local encoding for ALL groups
		for g := 0; g < loc; g++ {
			groupStart := g * kLocal
			nLocal := service.nLocal

			localData := make([]common.IOBuffer, nLocal)
			for i := 0; i < kLocal; i++ {
				localData[i] = data[groupStart+i]
			}
			// Set parity positions to nil for encoding
			for i := kLocal; i < nLocal; i++ {
				localData[i] = common.IOBuffer{}
			}

			localOutputs := make([]common.IOBuffer, service.localParityPerGroup)
			for i := range localOutputs {
				localOutputs[i] = service.bufPool.GetBuffer(int(size))
			}

			if service.localEncoderType == "MSR" {
				service.locMSREncoder.LocEncode(localData, localOutputs, service.localParityPerGroup)
			} else {
				service.locRSEncoder.Encode(localData, localOutputs)
			}

			// Store local parity in the data array
			localParityStart := n + g*service.localParityPerGroup
			for i := 0; i < service.localParityPerGroup; i++ {
				data[localParityStart+i] = localOutputs[i]
			}
			logrus.Infof("Local parity generated for group %d", g)
		}
	} else {
		// Small blocks (blockId == 0): use RS encoding for global parity only
		rsOutputs := make([]common.IOBuffer, n-k)
		for i := range rsOutputs {
			rsOutputs[i] = service.bufPool.GetBuffer(int(size))
		}
		service.minBlockEncoder.Encode(data[:n], rsOutputs)
		logrus.Infoln("RS encoding for small blocks completed")
	}

	return data
}

func (service *GeometricService) RegenerateObject(PG dir.PlacementGroup, geoIndex *index.GeometricIndex, offset, size uint64, broken int) chan common.IOBuffer {
	done := make(chan bool, 1)
	var args stor.GetBatchDataArgs
	//var StripNum = 0
	args.PGId = int(PG.PGId)
	logrus.Infoln("对象的offset,是：", offset)
	ret := make(chan common.IOBuffer, len(geoIndex.Blocks))
	logrus.Infoln("开始dget")

	//stor.Compress(&args)
	indexedBuffers := make([]IndexedBuffer, len(geoIndex.Blocks))
	indexedGoodBuffers := make([]IndexedBuffer, len(geoIndex.Blocks))
	go func() {
		for _, block := range geoIndex.Blocks {
			if block.IndexInPG == int16(broken) {
				//TODO:恢复大对象，按照几何条带布局，每个对象只需要修复损坏磁盘上的一部分即可，也就是说，修复该对象blockId=0那个节点上的头部数据(或者其他数据)
				//对象的每个块都不属于一个条带上***
				args.Offset = append(args.Offset, block.OffsetInBucket)
				args.Size = append(args.Size, uint32(block.Size))
				args.BlockId = append(args.BlockId, block.BlockID)
			}
		}
		if len(args.BlockId) <= 0 {
			close(ret)
			return
		}
		//假如第一个节点上有该对象的2个不同的数据块，或者有10个相同的数据块。
		//对于不同的数据块就需要恢复2个条带，对于10个相同的数据块只需要恢复一个条带
		//所以要先判断该节点上对于损坏的数据块有几个是不属于一个条带的，这个就是 []IOBuffer的长度
		//所以要遍历这个数组，将不同的BlockId算出来
		//for i := 0; i < len(args.BlockId); i++ {
		//	if args.BlockId[i] == args.BlockId[i+1] {
		//		StripNum++
		//	}
		//}
		//var wg sync.WaitGroup

		//indexedBuffers := make([]IndexedBuffer, len(args.BlockId))
		//indexedGoodBuffers := make([]IndexedBuffer, len(geoIndex.Blocks))
		//要知道是哪几个BlockId损坏了，然后调用RegenerateBlock函数
		for i, _ := range geoIndex.Blocks {
			if geoIndex.Blocks[i].IndexInPG == int16(broken) {
				data := service.RegenerateBlock(int(geoIndex.Blocks[i].BlockID), geoIndex.Blocks[i].OffsetInBucket, geoIndex.Blocks[i].Size, broken, PG)
				indexedBuffers[i] = IndexedBuffer{Index: i, Buffer: data}
				//logrus.Warnln("RegenerateBlock time", i, "indexedBadBuffers", indexedBuffers[i].Buffer.Len())
				logrus.Warnln("len.BlockId=", indexedBuffers[i].Buffer.Len())
			}
		}
		logrus.Warnln("len(indexedBuffers)", len(indexedBuffers))
		var Goodblock chan common.IOBuffer
		Goodblock = service.GetGoodBlock(offset, size, PG, geoIndex)
		for i, block := range geoIndex.Blocks {
			indexedGoodBuffers[i] = IndexedBuffer{Index: i, Buffer: <-Goodblock}
			logrus.Warnln("GetGoodBlock time", i, "indexedGoodBuffers[i].Buffer.Len()", indexedGoodBuffers[i].Buffer.Len())
			logrus.Warnln("blockId=", i, "disk", block.IndexInPG)
		}
		//logrus.Warnln("len(indexedGoodBuffers)", len(indexedGoodBuffers))
		//fillReadBlock(Goodblock, indexedGoodBuffers)
		for i, block := range geoIndex.Blocks {
			if block.IndexInPG == int16(broken) {
				indexedGoodBuffers[i].Buffer.Unref()
				logrus.Warnln("被丢弃掉的数据有:", i)
			}
		}
		for i, block := range geoIndex.Blocks {
			if block.IndexInPG == int16(broken) {
				indexedGoodBuffers[i].Buffer.Ref()
				indexedGoodBuffers[i].Buffer = indexedBuffers[i].Buffer
				logrus.Warnln("insert time:", i)
			}
		}
		done <- true
	}()

	<-done
	for i, _ := range geoIndex.Blocks {
		ret <- indexedGoodBuffers[i].Buffer
		logrus.Warnln("transfer time", i)
	}
	logrus.Warnln("close(ret)")
	close(ret)

	logrus.Warnln("len(indexedGoodBuffers)", len(indexedGoodBuffers))
	return ret
}

func fillReadBlock(goodBlock chan common.IOBuffer, readBlock []common.IOBuffer) {
	// 确保切片具有足够的容量
	if cap(readBlock) < len(goodBlock) {
		readBlock = make([]common.IOBuffer, len(goodBlock))
	}

	// 从 goodBlock 通道中读取数据，并将其存储到 readBlock 切片中
	for i := 0; i < len(goodBlock); i++ {
		buffer := <-goodBlock
		readBlock[i] = buffer
	}
}

// RegenerateBlock repairs a single data block using local or global repair.
// For data nodes (broken < k), uses local group repair for reduced I/O.
// For parity nodes, falls back to global MSR repair.
func (service *GeometricService) RegenerateBlock(blockId int, offset uint64, size uint64, broken int, PG dir.PlacementGroup) common.IOBuffer {
	k := service.k
	kLocal := service.kLocal
	logrus.Warnln("RegenerateBlock: PGId:", PG.PGId, "blockId:", blockId, "offset:", offset, "size:", size, "broken:", broken)

	// Small blocks (blockId == 0): use RS encoding
	if blockId == 0 {
		blocks := service.RepairBlocks(blockId, offset, size, PG, broken, true)
		logrus.Infoln("RS repair completed, size:", blocks[broken].Len())
		for i := 0; i < len(blocks); i++ {
			if i != broken && blocks[i].Data != nil {
				blocks[i].Unref()
			}
		}
		return blocks[broken]
	}

	// Determine which local group the broken node belongs to
	groupIndex := broken / kLocal
	if groupIndex < service.loc && broken < k {
		// LOCAL REPAIR: data node within a local group
		logrus.Infof("Local repair: broken=%d in group %d (nodes %d-%d)", broken, groupIndex, groupIndex*kLocal, (groupIndex+1)*kLocal-1)
		return service.localRepair(blockId, offset, size, broken, PG, groupIndex)
	}

	// Check if broken is a local parity node (broken >= n)
	n := service.totalNodes()
	if broken >= n {
		// LOCAL PARITY REPAIR: re-encode from the group's data nodes
		localParityOffset := broken - n
		groupIndex := localParityOffset / service.localParityPerGroup
		parityIndexInGroup := localParityOffset % service.localParityPerGroup
		logrus.Infof("Local parity repair: broken=%d, group=%d, parityIdx=%d", broken, groupIndex, parityIndexInGroup)
		return service.repairLocalParity(blockId, offset, size, PG, groupIndex, parityIndexInGroup)
	}

	// GLOBAL REPAIR: for global parity nodes (k <= broken < n)
	logrus.Infof("Global repair: broken=%d (global parity node)", broken)
	return service.globalRepair(blockId, offset, size, broken, PG)
}
func (service *GeometricService) GetGoodBlock(offset uint64, size uint64, PG dir.PlacementGroup, geoIndex *index.GeometricIndex) chan common.IOBuffer {

	//currentOff := uint64(0)

	var args stor.GetBatchDataArgs
	args.PGId = int(PG.PGId)

	for _, block := range geoIndex.Blocks {

		args.BlockId = append(args.BlockId, block.BlockID)
		args.Offset = append(args.Offset, block.OffsetInBucket)
		args.Size = append(args.Size, uint32(block.Size))

	}

	results := make(chan common.IOBuffer, len(args.BlockId))

	var wg sync.WaitGroup

	type IndexedBuffer struct {
		Index  int
		Buffer common.IOBuffer
	}

	indexedBuffers := make([]IndexedBuffer, len(args.BlockId))

	for j, block := range geoIndex.Blocks {
		wg.Add(1)
		go func(i int, blockID int16) {
			var blockArgs stor.GetBatchDataArgs
			blockArgs.PGId = args.PGId
			blockArgs.BlockId = append(blockArgs.BlockId, blockID)
			blockArgs.Offset = append(blockArgs.Offset, args.Offset[i])
			blockArgs.Size = append(blockArgs.Size, args.Size[i])
			conn := service.pool.GetConnectionByBrick(PG.Bricks[geoIndex.Blocks[i].IndexInPG])
			buff := getData(&blockArgs, conn, service.bufPool)
			indexedBuffers[i] = IndexedBuffer{Index: i, Buffer: buff}
			wg.Done()
		}(j, block.BlockID)
	}

	go func() {
		wg.Wait()
		for _, indexedBuffer := range indexedBuffers {
			results <- indexedBuffer.Buffer
		}
		close(results)
	}()

	return results
}
func (service *GeometricService) Get(offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex) chan common.IOBuffer {

	geoIndex := objectIndex.Geometric
	//currentOff := uint64(0)

	var args stor.GetBatchDataArgs
	args.PGId = int(PG.PGId)

	for _, block := range geoIndex.Blocks {

		args.BlockId = append(args.BlockId, block.BlockID)
		args.Offset = append(args.Offset, block.OffsetInBucket)
		args.Size = append(args.Size, uint32(block.Size))

	}

	results := make(chan common.IOBuffer, len(args.BlockId))

	var wg sync.WaitGroup

	type IndexedBuffer struct {
		Index  int
		Buffer common.IOBuffer
	}

	indexedBuffers := make([]IndexedBuffer, len(args.BlockId))

	for j, block := range geoIndex.Blocks {
		wg.Add(1)
		go func(i int, blockID int16) {
			var blockArgs stor.GetBatchDataArgs
			blockArgs.PGId = args.PGId
			blockArgs.BlockId = append(blockArgs.BlockId, blockID)
			blockArgs.Offset = append(blockArgs.Offset, args.Offset[i])
			blockArgs.Size = append(blockArgs.Size, args.Size[i])
			conn := service.pool.GetConnectionByBrick(PG.Bricks[geoIndex.Blocks[i].IndexInPG])
			buff := getData(&blockArgs, conn, service.bufPool)
			indexedBuffers[i] = IndexedBuffer{Index: i, Buffer: buff}
			wg.Done()
		}(j, block.BlockID)
	}

	go func() {
		wg.Wait()
		for i, indexedBuffer := range indexedBuffers {
			if i == len(indexedBuffers)-1 {
				begin := uint64(0)
				end := indexedBuffer.Buffer.Len() - geoIndex.Blocks[i].PaddSize
				indexedBuffer.Buffer = indexedBuffer.Buffer.Slice(begin, end)
			}
			results <- indexedBuffer.Buffer
		}
		close(results)
	}()

	return results
}

func (service *GeometricService) getMaxSize(blockID int16, pg dir.PlacementGroup) uint64 {
	k := service.k
	sizes := make([]uint64, k)
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			conn := service.pool.GetConnectionByBrick(pg.Bricks[i])
			var args stor.GetBlockSizeArgs
			args.BlockId = blockID
			args.PGId = pg.PGId
			conn.Call("StorageService.GetBlockSize", &args, &sizes[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
	maxSize := uint64(0)
	for i := range sizes {
		if sizes[i] > maxSize {
			maxSize = sizes[i]
		}
	}
	return maxSize
}

func (service *GeometricService) GenerateParity(PG dir.PlacementGroup) {
	k := service.k
	n := k + service.redundancy

	blockSizes := make([]uint64, 0)
	blockSizes = append(blockSizes, 0)
	for blockSize := service.partitioner.MinBlock; blockSize <= service.partitioner.MaxBlock; blockSize = blockSize * uint64(service.partitioner.Base) {
		blockSizes = append(blockSizes, blockSize)
	}

	var wg sync.WaitGroup
	for _, blockSize := range blockSizes {
		blockId := service.partitioner.SizeToBlockId(blockSize)
		maxSize := service.getMaxSize(blockId, PG)
		logrus.Infof("Generating PG%d-block%d, size is %d\n", PG.PGId, blockId, maxSize)

		if blockId == 0 {
			blockSize = service.partitioner.MinBlock
		}

		for offset := uint64(0); offset < maxSize; offset += blockSize {
			size := blockSize
			if maxSize-offset < size {
				size = maxSize - offset
			}
			blocks := service.RepairBlocks(int(blockId), offset, size, PG, -1, false)
			for i := range blocks[:k] {
				blocks[i].Unref()
			}
			for i := k; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					conn := service.pool.GetConnectionByBrick(PG.Bricks[i])
					err := putSlide(blocks[i], 0, uint64(len(blocks[i].Data)), offset, blockId, PG.PGId, conn)
					if err != nil {
						logrus.Errorf("Error putting slide for brick %v: %v", PG.Bricks[i], err)
					}
					blocks[i].Unref()
					wg.Done()
				}(i)
			}
			wg.Wait()
			// Write local parity for all groups (only for blockId != 0;
			// blockId==0 uses RS global parity only, no local parity)
			if blockId != 0 {
				totalLocalParity := service.loc * service.localParityPerGroup
				for i := n; i < n+totalLocalParity; i++ {
					wg.Add(1)
					go func(i int) {
						conn := service.pool.GetConnectionByBrick(PG.Bricks[i])
						err := putSlide(blocks[i], 0, uint64(len(blocks[i].Data)), offset, blockId, PG.PGId, conn)
						if err != nil {
							logrus.Errorf("Error putting slide for brick %v: %v", PG.Bricks[i], err)
						}
						blocks[i].Unref()
						wg.Done()
					}(i)
				}
				wg.Wait()
			}
			logrus.Infoln("putSlide goroutine finished")
		}

	}

}

func (service *GeometricService) Put(objectID uint64, data common.IOBuffer, PG dir.PlacementGroup, objectIndex index.ObjectIndex) {
	geoIndex := objectIndex.Geometric
	var wg sync.WaitGroup
	wg.Add(len(geoIndex.Blocks))

	curOff := make([]uint64, len(geoIndex.Blocks))
	curoff := uint64(0)
	for i, block := range geoIndex.Blocks {
		curOff[i] = curoff
		curoff += block.Size
	}
	for i, block := range geoIndex.Blocks {
		go func(block index.GeometricIndexBlock, offset uint64) {
			conn := service.pool.GetConnectionByBrick(PG.Bricks[block.IndexInPG])
			err := putSlide(data, offset, block.Size, block.OffsetInBucket, block.BlockID, PG.PGId, conn)
			if err != nil {
				logrus.Errorln(err)
			}
			//conn.Close()
			wg.Done()
		}(block, curOff[i])
	}
	wg.Wait()
}

func (service *GeometricService) GetRecoverTasks(instruct dir.MoveInstruct) []dir.RecoveryTask {

	PG := instruct.PG
	broken := instruct.Broken
	toRecover := uint64(0)
	conn := service.pool.GetConnectionByBrick(PG.Bricks[broken])

	res := make([]dir.RecoveryTask, 0)

	blockSizes := make([]uint64, 0)
	for blockSize := service.partitioner.MaxBlock; blockSize >= service.partitioner.MinBlock; blockSize = blockSize / uint64(service.partitioner.Base) {
		blockSizes = append(blockSizes, blockSize)
	}
	blockSizes = append(blockSizes, 0)

	for _, blockSize := range blockSizes {
		blockId := service.partitioner.SizeToBlockId(blockSize)

		var args stor.GetBlockSizeArgs
		args.BlockId = blockId
		args.PGId = PG.PGId
		conn.Call("StorageService.GetBlockSize", &args, &toRecover)

		logrus.Warnf("PG%d-block%d: %u", PG.PGId, blockId, toRecover)

		if blockId == 0 {
			blockSize = service.partitioner.MinBlock
		}

		for offset := uint64(0); offset < toRecover; offset += blockSize {
			size := blockSize
			if blockId == 0 && offset+size > toRecover {
				size = toRecover - offset
			}
			res = append(res, dir.RecoveryTask{PGId: int(instruct.PG.PGId), Broken: instruct.Broken, NewBrick: instruct.NewBrick, BlockId: int(blockId), BlockOffset: offset, Size: size})
		}
	}
	return res
}

func (service *GeometricService) Recovery() {

	conn := service.pool.GetDirConnection()
	volume := service.pool.GetVolume()

	blocks := int(service.partitioner.MaxBlock / service.partitioner.MinBlock)
	maxWrites := make(chan bool, common.RecoveryConcurrentNum*4*blocks)
	for i := 0; i < common.RecoveryConcurrentNum*4*blocks; i++ {
		maxWrites <- true
	}

	maxTasks := make(chan bool, common.RecoveryConcurrentNum*blocks)
	for i := 0; i < common.RecoveryConcurrentNum*blocks; i++ {
		maxTasks <- true
	}

	for {
		var task dir.RecoveryTask
		err := conn.Call("DirectoryService.PullRecoveryTask", &volume.VolumeName, &task)
		if err != nil && err.Error() == common.ErrNoTaskLeft.Error() {
			break
		} else if err != nil {
			logrus.Errorln(err)
			time.Sleep(time.Second)
			continue
		}
		blocks := int(task.Size / service.partitioner.MinBlock)
		for i := 0; i < blocks; i++ {
			<-maxTasks
		}
		go func(task dir.RecoveryTask, blocks int) {
			logrus.Infoln("Handling recovery task", task)
			PGId := task.PGId
			broken := task.Broken
			dest := task.NewBrick

			var args stor.PutDataArgs
			args.BlockId = int16(task.BlockId)
			args.Offset = task.BlockOffset
			args.Len = task.Size

			volume := service.pool.GetVolume()
			pg := volume.PGs[task.PGId]
			logrus.Warnln("PGId:", task.PGId, "blockId:", task.BlockId, "size:", task.Size, " IndexInPG:", broken)
			data := service.RegenerateBlock(task.BlockId, task.BlockOffset, task.Size, broken, pg)
			args.PGId = uint32(PGId)

			//Time consuming, but with little concurrency. Should not block the running of repair.
			go func(brick uuid.UUID, args *stor.PutDataArgs, data common.IOBuffer, blocks int) {
				for i := 0; i < blocks; i++ {
					<-maxWrites
				}
				conn := service.pool.GetConnectionByBrick(brick)

				err := putSlide(data, 0, args.Len, args.Offset, args.BlockId, args.PGId, conn)
				if err != nil {
					logrus.Errorln(err)
				}
				data.Unref()
				for i := 0; i < blocks; i++ {
					maxWrites <- true
				}
			}(dest, &args, data, blocks)
			logrus.Infoln("Finish recovery task", task)
			for i := 0; i < blocks; i++ {
				maxTasks <- true
			}

		}(task, blocks)
	}
	for i := 0; i < common.RecoveryConcurrentNum*blocks; i++ {
		<-maxTasks
	}
	logrus.Infoln("Begin to wait for writing on disk.")
	for i := 0; i < common.RecoveryConcurrentNum*4*blocks; i++ {
		<-maxWrites
	}
}

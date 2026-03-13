package index

import (
	"dgstor/common"
)

type GeometricIndexBlock struct {
	IndexInPG      int16
	BlockID        int16
	OffsetInBucket uint64
	Size           uint64
	PaddSize	   uint64
	OffsetInObject     int
}

type GeometricIndex struct {
	Blocks    []GeometricIndexBlock
}

type GeometricIndexConstructor struct {
	Partitioner GeometricPartitioner
	K           int
	currentDisk   int16
}


//TODO:disk选取
func (constructor *GeometricIndexConstructor) Construct(args *PutIndexArgs,spaceUsed *interface{}) *ObjectIndex {
	sizeList := constructor.Partitioner.Split(args.Size)

	k := constructor.K
	if *spaceUsed == nil {
		*spaceUsed = make([][]uint64, k)
		spaces := (*spaceUsed).([][]uint64)
		for i:=0;i<k;i++{
			spaces[i] = make([]uint64,constructor.Partitioner.SizeToBlockId(constructor.Partitioner.MaxBlock) + 1)
		}
	}
	spaces := (*spaceUsed).([][]uint64)

	index := &GeometricIndex{}
	index.Blocks = make([]GeometricIndexBlock,0)

	//disk := constructor.getOptimalBrick(spaces,constructor.Partitioner.SizeToBlockId(sizeList[len(sizeList) - 1]),isLargeObject)
	for i,size := range sizeList {
		block := &GeometricIndexBlock{}
		block.Size = size
		block.BlockID = constructor.Partitioner.SizeToBlockId(size)
		block.PaddSize = args.PaddSize
		block.IndexInPG = constructor.getMostFreeBrick(spaces,constructor.Partitioner.SizeToBlockId(sizeList[len(sizeList) - 1]))
		block.OffsetInBucket = spaces[block.IndexInPG][block.BlockID]
		block.OffsetInObject = i
		index.Blocks = append(index.Blocks,*block)
		spaces[block.IndexInPG][block.BlockID] += block.Size


		if spaces[block.IndexInPG][block.BlockID]%common.StorageOffsetAlign != 0 {
			spaces[block.IndexInPG][block.BlockID] += common.StorageOffsetAlign - spaces[block.IndexInPG][block.BlockID]%common.StorageOffsetAlign
		}
	}

	return &ObjectIndex{Geometric: index}
}
//使用于大对象,对于大对象来说，使用getMostFreeBrick函数可以更好地利用空间，因为该函数会选择可用空间最多的磁盘，从而最大化可用空间。
func (constructor *GeometricIndexConstructor) getMostFreeBrick(spaceUsed [][]uint64, blockId int16) int16 {
	ret := 0
	for i := 1; i < len(spaceUsed); i++ {
		if spaceUsed[i][blockId] < spaceUsed[ret][blockId] {
			ret = i
		}
	}
	return int16(ret)
}
////适用于小对象,而对于小对象来说，getMostUseBrick函数可能更合适，因为它可以尽可能地利用已经占用的空间，从而最小化空间浪费。
//func (constructor *GeometricIndexConstructor) getMostUseBrick(spaceUsed [][]uint64, blockId int16) int16 {
//	ret := 0
//	for i := 1; i < len(spaceUsed); i++ {
//		if spaceUsed[i][blockId] > spaceUsed[ret][blockId] {
//			ret = i
//		}
//	}
//	return int16(ret)
//}
//func (constructor *GeometricIndexConstructor) getOptimalBrick(spaceUsed [][]uint64, blockId int16, isLargeObject bool) int16 {
//	// 对于大对象，优先使用空闲空间多的磁盘
//	// 对于小对象，优先使用已用空间少的磁盘
//	if isLargeObject {
//		return constructor.getMostFreeBrick(spaceUsed, blockId)
//	} else {
//		return constructor.getMostUseBrick(spaceUsed, blockId)
//	}
//}
// 使用轮询策略在磁盘之间分配数据块，并从当前磁盘开始查找具有最少已用空间的磁盘
//func (constructor *GeometricIndexConstructor) getNextMostFreeBrick(spaceUsed [][]uint64, blockId int16) int16 {
//	ret := constructor.currentDisk
//	for i := 1; i < constructor.K; i++ {
//		nextDisk := (constructor.currentDisk + int16(i)) % int16(constructor.K)
//		if spaceUsed[nextDisk][blockId] < spaceUsed[ret][blockId] {
//			ret = nextDisk
//		}
//	}
//	constructor.currentDisk = (constructor.currentDisk + 1) % int16(constructor.K)
//	return ret
//}
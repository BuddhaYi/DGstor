package index

import (
	"dgstor/common"
)

// GeoPartitionIndexConstructor implements Geometric Partitioning (Geo) from reference [13].
// All blocks of the same object are placed on the SAME brick (contiguous placement),
// unlike GeometricIndexConstructor which places blocks DISCRETELY across different bricks.
type GeoPartitionIndexConstructor struct {
	Partitioner GeometricPartitioner
	K           int
}

func (constructor *GeoPartitionIndexConstructor) Construct(args *PutIndexArgs, spaceUsed *interface{}) *ObjectIndex {
	sizeList := constructor.Partitioner.Split(args.Size)

	k := constructor.K
	if *spaceUsed == nil {
		*spaceUsed = make([][]uint64, k)
		spaces := (*spaceUsed).([][]uint64)
		for i := 0; i < k; i++ {
			spaces[i] = make([]uint64, constructor.Partitioner.SizeToBlockId(constructor.Partitioner.MaxBlock)+1)
		}
	}
	spaces := (*spaceUsed).([][]uint64)

	index := &GeometricIndex{}
	index.Blocks = make([]GeometricIndexBlock, 0)

	// Geo: choose ONE brick for the entire object, then place ALL blocks on it
	disk := constructor.getMostFreeBrick(spaces, constructor.Partitioner.SizeToBlockId(sizeList[len(sizeList)-1]))

	for i, size := range sizeList {
		block := &GeometricIndexBlock{}
		block.Size = size
		block.BlockID = constructor.Partitioner.SizeToBlockId(size)
		block.PaddSize = args.PaddSize
		block.IndexInPG = disk // all blocks go to the SAME brick
		block.OffsetInBucket = spaces[block.IndexInPG][block.BlockID]
		block.OffsetInObject = i
		index.Blocks = append(index.Blocks, *block)
		spaces[block.IndexInPG][block.BlockID] += block.Size

		if spaces[block.IndexInPG][block.BlockID]%common.StorageOffsetAlign != 0 {
			spaces[block.IndexInPG][block.BlockID] += common.StorageOffsetAlign - spaces[block.IndexInPG][block.BlockID]%common.StorageOffsetAlign
		}
	}

	return &ObjectIndex{Geometric: index}
}

func (constructor *GeoPartitionIndexConstructor) getMostFreeBrick(spaceUsed [][]uint64, blockId int16) int16 {
	ret := 0
	for i := 1; i < len(spaceUsed); i++ {
		if spaceUsed[i][blockId] < spaceUsed[ret][blockId] {
			ret = i
		}
	}
	return int16(ret)
}

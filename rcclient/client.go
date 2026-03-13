package rcclient

import (
	"github.com/sirupsen/logrus"
	"io"
	"dgstor/common"
	"dgstor/dir"
	"dgstor/ec"
	"dgstor/indexservice"
	"time"
)

var diff uint64 = 0

type Client struct {
	DirConn *common.Connection

	Pool      *dir.VolumeConnectionPool
	EC        ec.ECServiceInterface
	IobufPool *common.IOBufferPool

	ClientId   int
	NumClients int

	LocalHttpPort uint16
	timestat      common.TimeStats
}

func (client *Client) GetPGID(objectID uint64) int {

	var PGId int

	volume := client.Pool.GetVolume()
	//k := client.volume.Parameter.K
	nPG := volume.Parameter.NumberPG

	PGId = int(objectID % uint64(nPG))

	return PGId
}

func (client *Client) GetIndex(objectID uint64) (index.ObjectIndex, dir.PlacementGroup) {
	PGId := client.GetPGID(objectID)
	volume := client.Pool.GetVolume()

	pg := volume.PGs[PGId]
	var objectIndex index.ObjectIndex

	for indexId := 0; indexId < common.REPLICATION; indexId++ {
		indexBrick := pg.Index[indexId]

		conn := client.Pool.GetConnectionByBrick(indexBrick)
		var args index.GetIndexArgs
		args.ObjectId = objectID
		args.PGId = uint32(PGId)
		err := conn.Call("IndexService.GetIndex", &args, &objectIndex)
		if err != nil {
			logrus.Errorln("Failed to get index for", objectID, err)
		} else {
			return objectIndex, pg
		}
	}
	logrus.Fatal("Failed to get index")
	return objectIndex, pg
}

func (client *Client) PutIndex(objectID uint64, size uint64) (index.ObjectIndex, dir.PlacementGroup, error) {
	PGId := client.GetPGID(objectID)
	volume := client.Pool.GetVolume()

	pg := volume.PGs[PGId]
	var objectIndex index.ObjectIndex

	for indexId := 0; indexId < common.REPLICATION; indexId++ {
		indexBrick := pg.Index[indexId]
		//logrus.Infoln(client.volume)

		conn := client.Pool.GetConnectionByBrick(indexBrick)
		var args index.PutIndexArgs
		args.ObjectID = objectID
		args.Size = size
		args.PGId = uint32(PGId)
		//args.PaddSize = diff
		err := conn.Call("IndexService.PutIndex", &args, &objectIndex)
		if err != nil && err.Error() == index.ErrExist.Error() {
			logrus.Errorln(err)
			return objectIndex, pg, err
		} else if err != nil {
			logrus.Errorln("Failed to put index", err)
			return objectIndex, pg, err
		}
	}
	return objectIndex, pg, nil
}
func (client *Client) NewPutIndex(objectID uint64, size uint64, diff uint64) (index.ObjectIndex, dir.PlacementGroup, error) {
	PGId := client.GetPGID(objectID)
	volume := client.Pool.GetVolume()

	pg := volume.PGs[PGId]
	var objectIndex index.ObjectIndex

	for indexId := 0; indexId < common.REPLICATION; indexId++ {
		indexBrick := pg.Index[indexId]
		//logrus.Infoln(client.volume)

		conn := client.Pool.GetConnectionByBrick(indexBrick)
		var args index.PutIndexArgs
		args.ObjectID = objectID
		args.Size = size
		args.PGId = uint32(PGId)
		args.PaddSize = diff
		err := conn.Call("IndexService.PutIndex", &args, &objectIndex)
		if err != nil && err.Error() == index.ErrExist.Error() {
			logrus.Errorln(err)
			return objectIndex, pg, err
		} else if err != nil {
			logrus.Errorln("Failed to put index", err)
			return objectIndex, pg, err
		}
	}
	return objectIndex, pg, nil
}

// Put writes an object with even-multiple alignment per paper Algorithm 1.
// Objects >= MinBlockSize are padded to an even multiple of MinBlockSize
// to eliminate read amplification without hybrid coding.
func (client *Client) Put(objectId uint64, data common.IOBuffer) {
	size := uint64(len(data.Data))

	// Use MinBlockSize from volume config as threshold (paper: Threshold = s0)
	minBlock := client.Pool.GetVolume().Parameter.MinBlockSize
	if minBlock == 0 {
		minBlock = 1 << 20 // 1MB fallback
	}

	var diff uint64
	if size < minBlock {
		// Objects smaller than MinBlock: pad up to MinBlock
		// Split() outputs [MinBlock] for these, so buffer must match
		diff = minBlock - size
		padding := make([]byte, diff)
		data.Data = append(data.Data, padding...)
	} else {
		// Align to even multiple of minBlock (paper Algorithm 1, lines 3-8)
		n := size / minBlock
		if size%minBlock != 0 {
			n++
		}
		if n%2 != 0 {
			n++
		}
		alignedSize := n * minBlock
		diff = alignedSize - size
		if diff > 0 {
			padding := make([]byte, diff)
			data.Data = append(data.Data, padding...)
		}
	}

	paddAfterSize := len(data.Data)
	logrus.Infoln("PaddAftersize=", paddAfterSize)

	putIndex, PG, err := client.NewPutIndex(objectId, uint64(paddAfterSize), diff)
	if err == nil {
		client.EC.Put(objectId, data, PG, putIndex)
	} else {
		logrus.Errorln(err)
	}
}

func (client *Client) Get(w io.Writer, objectId uint64, size uint64, offset uint64) {
	var getTime time.Duration
	objectIndex, pg := client.GetIndex(objectId)

	if offset >= objectIndex.GetSize() {
		return
	}
	if offset+size >= objectIndex.GetSize() {
		size = objectIndex.GetSize() - offset
	}
	start := time.Now()
	client.EC.WriteResponse(RateWriter(w, common.MaxClientBandwidth), objectId, offset, size, pg, objectIndex, -1)
	getTime = time.Since(start)
	logrus.Println("get object", objectId, "time is ", getTime)
	client.timestat.AddGetTime(getTime)
	rep := client.timestat.AverageGet()
	logrus.Printf("Avg get time:%d ns\n", rep)
}

func (client *Client) GeneratePGParity(pgID int) {
	logrus.Println("Begin to generate parity of PG", pgID)
	volume := client.Pool.GetVolume()
	client.EC.GenerateParity(volume.PGs[pgID])
	logrus.Println("Finish generate parity for", pgID)
}

func (client *Client) DGet(w io.Writer, objectId uint64, size uint64, offset uint64) {
	var dgetTime time.Duration
	objectIndex, pg := client.GetIndex(objectId)
	broken := 0

	if offset >= objectIndex.GetSize() {
		return
	}
	if offset+size >= objectIndex.GetSize() {
		size = objectIndex.GetSize() - offset
	}

	if objectIndex.Contiguous != nil {
		broken = int(objectIndex.Contiguous.IndexInPG)
	} else if objectIndex.Geometric != nil {
		broken = int(objectIndex.Geometric.Blocks[0].IndexInPG)
	}
	//client.EC.WriteResponse(w, objectId, offset, size, pg, objectIndex, broken)

	logrus.Println("begin to dget object:", objectId)
	start := time.Now()
	client.EC.WriteResponse(RateWriter(w, common.MaxClientBandwidth), objectId, offset, size, pg, objectIndex, broken)
	dgetTime = time.Since(start)
	logrus.Println("dget object", objectId, "time is ", dgetTime)
	client.timestat.AddDgetTime(dgetTime)
	rep := client.timestat.AverageDget()
	logrus.Printf("Avg Dget time:%d ns\n", rep)
}

func MakeClient(directoryAddr string, volumeName string) *Client {
	dirConn := common.MakeConnection(directoryAddr)
	return MakeClientWithConn(dirConn, volumeName)
}

func MakeClientWithConn(dirConn *common.Connection, volumeName string) *Client {
	result := &Client{}

	result.DirConn = dirConn
	result.IobufPool = common.MakeIOBufferPool()

	result.Pool = dir.MakeVolumeConnectionPool(volumeName, dirConn)
	result.EC = ec.MakeECService(result.Pool)

	return result
}

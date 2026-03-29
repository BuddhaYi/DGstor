package rcclient

import (
	//"crypto/rand"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"dgstor/common"
	"dgstor/dir"
	"sync"
	"time"
)


func readHttp(url string)  {
	resp,err := http.Get(url)
	if err != nil {
		logrus.Errorln(err)
		return
	}
	defer resp.Body.Close()
	nullFile,_ := os.OpenFile("/dev/null",os.O_RDWR|os.O_CREATE,0644)
	defer nullFile.Close()
	io.Copy(nullFile,resp.Body)
}

func (client *Client) getTrace() *Trace{
	if client.Pool.GetVolume().Parameter.IsSSD {
		return GetTrace(0,common.SSDMaxSize,common.SSDNumObjects)
	} else {
		return GetTrace(common.SSDMaxSize,common.HDDMaxSize,common.HDDNumObjects)
	}
}

func (client *Client) TracePuts(args *interface{},reply *interface{}) error{

	logrus.Infoln("Begin to tracePuts, clientId:",client.ClientId)
	//buffer := make([]byte,math.MaxUint32)
	buffer := make([]byte,1458667491)//2750535104
	mrand.Read(buffer)
	logrus.Infoln("Data generated.")

	trace := client.getTrace()

	sizes := trace.SampleSizes()

	var wg sync.WaitGroup

	clients := 1
	ch := make(chan uint64)
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func() {
			for objectId := range ch {
				size := sizes[objectId]
				logrus.Println("Begin to put object", objectId, "size=", size)

				client.Put(objectId, common.IOBuffer{Data: buffer[0:size]})

				logrus.Println("Finish put object", objectId)
			}

			wg.Done()
		}()
	}

	for objectId := uint64(client.ClientId); objectId < trace.NumObjects; objectId += uint64(client.NumClients) {
		ch <- objectId
	}
	close(ch)
	wg.Wait()
	return nil
}


func (client *Client) GenerateParity(args* interface{},reply *interface{}) error{
	var wg sync.WaitGroup
	clients := 1
	mrand.Seed(time.Now().UnixNano())
	ch := make(chan int)
	wg.Add(clients)
	var latencys time.Duration
	for i := 0; i < clients; i++ {
		go func() {
			for pgID := range ch {

				logrus.Println("Begin to generate parity for", pgID)
				start := time.Now()
				client.GeneratePGParity(pgID)
				// 模拟延迟
				simulateDelay()

				latency := time.Since(start)
				logrus.Println("Finish generate parity for", pgID,"generate parity time is",latency)
				latencys += latency
			}
			wg.Done()
			logrus.Println("The total generate parity time is",latencys)
		}()
	}
	nPG := len(client.Pool.GetVolume().PGs)
	pgPerClient := (nPG + client.NumClients - 1) / client.NumClients

	for i := client.ClientId * pgPerClient; i < (client.ClientId +1) * pgPerClient && i < nPG; i++ {
		ch <- i
	}

	close(ch)
	wg.Wait()
	return nil
}

func (client *Client) ForegroundTraceGets(args *interface{}, reply *interface{}) error{

	trace := client.getTrace()
	ch := trace.SampleGet()


	for i := 0; i < common.ForegroundClients; i++ {

		go func(clientId int) {
			for objectId := range ch {
				url := fmt.Sprintf("http://localhost:%d/get/%d", client.LocalHttpPort, objectId)
				readHttp(url)
				logrus.Println("Finish get clientId:", clientId, "objectId:", objectId)
			}
		}(i)
	}
	return nil
}
func (client *Client) GetRecoveryTasks(args *dir.MoveInstruct,reply *[]dir.RecoveryTask) error {
	*reply = client.EC.GetRecoverTasks(*args)
	return nil
}

func (client *Client) Recovery(args *interface{},reply *interface{}) error {

	logrus.Infoln("Begin to recovery")

	start := time.Now()

	client.EC.Recovery()

	latency := time.Since(start)

	logrus.Infoln("Finish recovery,latency=",latency)

	return nil
}

func (client *Client) TraceDget(args *interface{}, reply *interface{}) error {

	trace := client.getTrace()

	sizes := trace.SampleSizes()
	DgetObjectId := [170]int{847, 613, 91, 1392, 619, 1481, 445, 274, 310, 137, 403, 382, 985, 31, 95, 287, 1044, 615, 837, 145, 1178, 1226, 305, 476, 513, 165, 261, 1009, 1603, 402, 1194, 106, 1471, 856, 1211, 1199, 1351, 444, 71, 245, 102, 848, 64, 5, 861, 317, 524, 1401, 155, 1198, 225, 1394, 275, 49, 1117, 12, 1411, 1248, 972, 951, 95, 1017, 407, 840, 1335, 1607, 243, 1368, 688, 247, 454, 1357, 384, 211, 243, 722, 134, 837, 1663, 1621, 1610, 953, 270, 628, 186, 1542, 384, 1571, 21, 934, 280, 899, 97, 1239, 89, 303, 897, 916, 244, 1145, 286, 1231, 453, 243, 328, 1021, 727, 1163, 1230, 937, 295, 837, 1264, 46, 12, 62, 869, 154, 824, 35, 0, 491, 974, 541, 1323, 473, 283, 1458, 1393, 419, 1148, 147, 164, 1540, 1368, 326, 900, 127, 1386, 130, 1668, 613, 1134, 804, 127, 137, 601, 578, 1176, 110, 4, 999, 106, 297, 25, 1039, 1455, 1565, 985, 22, 339, 188, 909, 1062, 1647, 182, 1395, 115, 614, 841}
	totalSize := uint64(0)
	totalLatency := 0.0
	validCount := 0
	logrus.Println("trace.NumObjects:", trace.NumObjects)
	for _, v := range DgetObjectId {
		if uint64(v) >= trace.NumObjects {
			continue
		}
		validCount++

		url := fmt.Sprintf("http://localhost:%d/dget/%d", client.LocalHttpPort, v)

		mrand.Seed(time.Now().UnixNano())
		simulateDelay()

		logrus.Println("Begin to read objectId:", v)
		start := time.Now()
		readHttp(url)
		latency := time.Since(start)

		logrus.Println("Finish read, latency:", latency)
		totalLatency += float64(latency.Milliseconds())
		totalSize += sizes[v]
	}
	logrus.Info("totalLatency=", totalLatency)

	logrus.Println("total latency:", totalLatency, "ms.", "Total sizes:", totalSize>>20, "MB.", "validCount:", validCount)
	if validCount > 0 {
		logrus.Println("Average latency:", totalLatency/float64(validCount), "ms.")
	}
	return nil
}

func (client *Client) TraceGet(args *interface{}, reply *interface{}) error {

	trace := client.getTrace()

	sample := trace.SampleGet()
	sizes := trace.SampleSizes()

	totalSize := uint64(0)
	totalLatency := 0.0
	logrus.Println("trace.NumObjects:", trace.NumObjects)
	//sampleNum := int(float64(trace.NumObjects) / 1000)
	sampleNum := int(float64(trace.NumObjects))
	for i:=0;i<sampleNum;i++{
		oid := <-sample

		url := fmt.Sprintf("http://localhost:%d/get/%d",client.LocalHttpPort, oid)

		// 为随机数生成器提供种子，这样每次程序运行时产生的随机数序列都不同
		mrand.Seed(time.Now().UnixNano())
		// 模拟延迟
		simulateDelay()

		logrus.Println("Begin to read objectId:", oid)
		start := time.Now()
		readHttp(url)
		latency := time.Since(start)

		logrus.Println("Finish TraceGet, latency:", latency)
		totalLatency += float64(latency.Milliseconds())
		totalSize += sizes[oid]
	}


	logrus.Println("TraceGet total latency:", totalLatency,"ms.", "Total sizes:",totalSize >> 20,"MB.")
	logrus.Println("TraceGet Average latency:",totalLatency/float64(sampleNum),"ms.")
	return nil
}
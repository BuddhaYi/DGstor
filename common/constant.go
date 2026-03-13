package common

import "time"

const LogDIR = "/usr/local/var/log"
const HTTPFilePath = "/usr/local/bin/rchttp"
const StorFilePath = "/usr/local/bin/rcstor"

const DefaultDirPort = 30100
const HeartBeatInterval = time.Second * 5
//TODO:控制trace对象的数量和大小范围
const SSDMaxSize uint64 = 2 << 20  //4 << 20
const HDDMaxSize uint64 = 512 * 1024 * 1024    // 4 << 30 = 4G要特别注意
const HDDNumObjects uint64 = 1
const SSDNumObjects uint64 = 1
const LargeObjectThreshold uint64 = 4194304
const DefaultHTTPPort = 30888
const ForegroundClients = 8

const StorageOffsetAlign = 512

const REPLICATION = 3
const DefaultMaxObjectId = 10000

const RecoveryConcurrentNum = 16
const MaxTCPPerConn = 16

//We have a default rate limiter for
//1Gbps = 1<<27
//2Gbps = 2 << 27
//4Gbps = 4 << 27
const MaxClientBandwidth uint64 =  (1<<27)
const DelayAccess int = 10000

const rpcReconnectTimeout = time.Second

const RegisterTimeout = time.Second * 20
const pingInterval = time.Second * 10
const pingTimeout = time.Second * 1000

//Timeout to send 256KB buffer
const IOTimeout = time.Millisecond * 500

const StorReadMinSize uint64 = 256 << 10
const StorWriteMinSize uint64 = 256 << 10

type Layout string

const (
	Contiguous Layout = "Contiguous"
	Geometric         = "Geometric"
	Stripe            = "Stripe"
	StripeMax         = "StripeMax"
	RS                = "RS"
	LRC               = "LRC"
	Hitchhiker        = "Hitchhiker"
)

type Status int

const (
	Started Status = iota
	Stopped
	Degraded
	Recovering
)

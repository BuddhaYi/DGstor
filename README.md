# DGStor

`DGStor` is a distributed object storage system based on erasure coding (regenerating codes). It supports multiple data layout strategies and encoding schemes, providing fault-tolerant storage with efficient data recovery across distributed clusters.

## Paper

> **Discrete Geometric Coded Data Layout for Large-scale Object Storage Systems**
>
> Yi Tian, Guangping Xu, Hongzhang Yang, Yue Ni, JiaXin Cao, Lei Yang
>
> *2023 IEEE Intl Conf on Parallel & Distributed Processing with Applications, Big Data & Cloud Computing, Sustainable Computing & Communications, Social Computing & Networking (ISPA/BDCloud/SocialCom/SustainCom)*
>
> DOI: [10.1109/ISPA-BDCloud-SocialCom-SustainCom59178.2023.00067](https://doi.org/10.1109/ISPA-BDCloud-SocialCom-SustainCom59178.2023.00067)

### Abstract

Regenerating codes are new network codes proposed to reduce the data required for fault repair, which can improve the recovery efficiency of faulty nodes in data storage systems. However, unlike Reed-Solomon code, which repairs at the granularity of bytes, regenerating codes require data stored in large chunks but leads to severe read amplification, which reads out excess data when degraded read objects and increases degraded read time. That reflects a mutual constraint between improving recovery efficiency and degraded read performance, as manifested in the amplification of data reads.

To solve this problem, we propose a new type of data layout -- discrete geometric, which splits the object into a series of geometric sequences of data blocks. They are placed discretely into corresponding containers on the disks at different nodes, with containers of the same size made into a strip for encoding. The discrete characteristic ensures lower repair costs for degraded reads. The geometric characteristic ensures the repair performance of regenerating codes by large blocks, and read amplification can be mitigated through small blocks. To reduce IOPS for discrete geometric, we propose Discrete Geometric-Locally Regenerating Codes (DG-LRCs), guaranteeing lower degraded read latency while improving recovery efficiency.

### Key Contributions

- **Discrete Geometric Layout**: Data blocks are placed discretely after an object's geometric partitioning in corresponding containers on disks at different nodes before encoding
- **DG-LRCs**: Discrete Geometric-Locally Regenerating Codes that significantly improve recovery efficiency by combining LRC codes with regenerating codes in a discrete geometric layout
- **DGStor System**: An object storage system implementing the discrete geometric layout for evaluation against existing layouts

### Key Results

- Recovery throughput of Discrete Geometry is **3.8x** higher than geometric partitioning
- Degraded read time of DG-LRCs compared to regenerating codes with geometric partitioning is **22.56% lower** at 2Gbps and **60.56% lower** at 4Gbps
- Recovery performance is **7.04x** better than RS code

## System Architecture

DGStor consists of four core components:

| Component | Binary | Description |
|-----------|--------|-------------|
| **Directory Service** | `rcdir` | Central metadata and coordination service, managing volumes, placement groups, and brick registration |
| **Storage Server** | `rcstor` | Data storage node, responsible for local brick data read/write and index management |
| **HTTP Server** | `rchttp` | HTTP interface for client data access (Put/Get/DGet) |
| **CLI Client** | `rccli` | Administrative command-line tool for volume and cluster management |

### Key Concepts

- **Volume**: A logical storage unit containing multiple placement groups, with specified encoding layout and parameters
- **Placement Group (PG)**: A group of bricks that stores encoded data blocks together
- **Brick**: A physical storage unit on a node, identified by UUID

## Supported Layouts and Encoding Schemes

| Layout | Encoder | Description |
|--------|---------|-------------|
| **Contiguous** | MSR | Sequential block layout, simple sequential access |
| **Geometric** | RS (small blocks) + MSR (large blocks) + Local RS/MSR | Variable-size blocks with hybrid global+local encoding, optimized for recovery bandwidth |
| **Stripe** | MSR | Fixed-size striped layout |
| **StripeMax** | MSR | Optimized striping variant |
| **RS** | Reed-Solomon | Classic erasure coding with fixed-size striping |
| **LRC** | Locally Repairable Codes | Two-tier recovery: local groups first, then global |
| **Hitchhiker** | Hitchhiker + RS | Geometric layout with Hitchhiker encoder for minimal repair bandwidth |

### Encoders

- **MSR (Minimum Storage Regenerating)**: Minimizes storage overhead with lower repair bandwidth, via C library binding (`libmsr`)
- **RS (Reed-Solomon)**: Classic erasure code with standard algorithm
- **LRC (Locally Repairable Codes)**: Two-tier repair with local groups and global parity
- **Hitchhiker**: Optimized for update efficiency, trades bandwidth for reduced update I/O

## Project Structure

```
DGStor/
├── rccli/              # CLI client (rccli binary)
├── rcdir/              # Directory service (rcdir binary)
├── rchttp/             # HTTP server (rchttp binary)
├── rcstor/             # Storage server (rcstor binary)
├── common/             # Shared constants, connection pool, I/O buffer, utilities
├── dir/                # Directory service core: volume, placement group, I/O dispatch, deployment
├── ec/                 # Erasure coding services (Contiguous, Geometric, Striped, RS, LRC, Hitchhiker)
├── encoder/            # Encoding implementations (MSR, RS, LRC, Hitchhiker) with C bindings
├── indexservice/       # Per-brick index and metadata tracking
├── rcclient/           # Client library: Put/Get/DGet, trace-based evaluation, rate limiter
├── rpc/                # Custom RPC server (Gob-based, 128 concurrent threads)
├── storageservice/     # Local disk I/O for brick data
├── tools/              # Utilities and test data generation
├── data/               # Example volume configuration JSON files
├── scripts/            # Deployment scripts, Dockerfile, docker-compose
├── Makefile            # Build targets
└── go.mod              # Go module (Go 1.14)
```

## Installation

### Prerequisites

- Go 1.14+
- Linux (CentOS/Ubuntu) or macOS
- SSH access between all cluster nodes
- GCC (for C library bindings)

### Install Go

```sh
wget https://dl.google.com/go/go1.14.10.linux-amd64.tar.gz
tar -xvf go1.14.10.linux-amd64.tar.gz -C /usr/local/
```

Set environment variables:

```sh
echo 'export GOROOT=/usr/local/go' >> ~/.profile
echo 'export GOPATH=$HOME/go' >> ~/.profile
echo 'export PATH=$GOPATH/bin:$GOROOT/bin:/usr/local/bin:$PATH' >> ~/.profile
source ~/.profile
ln -fs /usr/local/go/bin/go /usr/local/bin/go
```

If Go dependencies download timeout, set proxy:

```sh
export GOPROXY=https://goproxy.io,direct
```

### Build and Install DGStor

```sh
git clone https://github.com/DGStor/DGStor.git
cd DGStor
make install
```

This builds four binaries and installs them to `/usr/local/bin/`:
- `rcstor` - Storage server
- `rcdir` - Directory service
- `rchttp` - HTTP server
- `rccli` - CLI client

You only need to install DGStor on one server. All binaries will be distributed to other machines through SSH automatically.

### Build Targets

```sh
make bin      # Build all binaries to bin/ directory
make rcstor   # Build storage server only
make rcdir    # Build directory service only
make rccli    # Build CLI client only
make rchttp   # Build HTTP server only
make install  # Build and install all binaries to /usr/local/bin/
```

## Configure SSH

Start SSH services on every machine and copy keys between all servers:

```sh
apt update
apt install openssh-client openssh-server
/usr/sbin/sshd
ssh-keygen -b 4096 -N "" -f /root/.ssh/id_rsa
ssh-copy-id root@<ServerIP>
```

Repeat `ssh-copy-id` for all server pairs in the cluster.

## Quick Start

### 1. Start Directory Service

```sh
rcdir start
```

Directory service options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `30100` | Service port |
| `--configPath` | `/usr/local/var/rcdir/` | Config storage path |
| `--logPath` | `/usr/local/var/log/dir.log` | Log file path |

Other commands:

```sh
rcdir stop       # Stop directory service
rcdir restart    # Restart directory service
rcdir status     # Check service status
```

### 2. Configure Volume

Create a JSON file to define the volume. Key parameters:

| Parameter | Description |
|-----------|-------------|
| `VolumeName` | Unique name for the volume |
| `Servers` | List of servers, each with IP and disk paths |
| `nPG` | Number of placement groups |
| `Layout` | Data layout: `Contiguous`, `Geometric`, `Stripe`, `StripeMax`, `RS`, `LRC`, `Hitchhiker` |
| `K` | Number of data blocks |
| `Redundancy` | Number of global parity blocks |
| `LocalRedundancy` | Number of local groups (for Geometric/LRC) |
| `LocalEncoderType` | Local encoder: `MSR` (2 parity/group) or `RS` (1 parity/group) |
| `BlockSize` | Block size for Contiguous layout (bytes) |
| `MinBlockSize` / `MaxBlockSize` | Block size range for Geometric layout (bytes) |
| `GeometricBase` | Geometric progression base |
| `IsSSD` | `true` for SSD, `false` for HDD |
| `HTTPPort` | Exposed HTTP server port |

**Geometric layout example** (`data/geo1.json`):

```json
{
  "VolumeName": "Geo1",
  "Servers": [
    { "IP": "node1", "Dir": ["/disk/disk0", "/disk/disk1", "/disk/disk2", "/disk/disk3", "/disk/disk4", "/disk/disk5"] },
    { "IP": "node2", "Dir": ["/disk/disk0", "/disk/disk1", "/disk/disk2", "/disk/disk3", "/disk/disk4", "/disk/disk5"] },
    { "IP": "node3", "Dir": ["/disk/disk0", "/disk/disk1", "/disk/disk2", "/disk/disk3", "/disk/disk4", "/disk/disk5"] }
  ],
  "VolumeParameter": {
    "nPG": 200,
    "Layout": "Geometric",
    "K": 2,
    "Redundancy": 1,
    "MinBlockSize": 2097152,
    "MaxBlockSize": 268435456,
    "GeometricBase": 2,
    "LocalRedundancy": 2,
    "LocalEncoderType": "MSR",
    "IsSSD": true,
    "HTTPPort": 8080
  }
}
```

**Contiguous layout example** (`data/con1.json`):

```json
{
  "VolumeParameter": {
    "nPG": 20,
    "Layout": "Contiguous",
    "K": 2,
    "Redundancy": 1,
    "BlockSize": 1048576,
    "IsSSD": false,
    "HTTPPort": 8080
  }
}
```

More examples available in `data/`: `geo-128K.json`, `geo-256K.json`, `geo-4M.json`, `geo-16M.json`, `con-1M.json`.

### 3. Create and Start Volume

```sh
rccli create <config.json>    # Create volume from JSON config
rccli start <VolumeName>      # Start the volume
rccli list                    # List all volumes and their status
rccli list <VolumeName>       # Show details of a specific volume
```

### 4. Put/Get Objects

Objects can be put/get from any server via HTTP. Specify an unused object ID when putting.

**PUT** an object:

```sh
wget --post-file=<filename> http://<ServerIP>:<HTTPPort>/put/<objectID>
```

**GET** an object:

```sh
wget http://<ServerIP>:<HTTPPort>/get/<objectID>
```

GET supports optional query parameters: `?offset=<bytes>&size=<bytes>` for partial reads.

**DGET** (degraded get - read when a brick is unavailable):

```sh
wget http://<ServerIP>:<HTTPPort>/dget/<objectID>
```

## CLI Reference (rccli)

| Command | Usage | Description |
|---------|-------|-------------|
| `list [volumeName]` | `rccli list` | List all volumes or show details of a specific volume |
| `create <config.json>` | `rccli create geo1.json` | Create a volume from JSON config file |
| `start <volumeName>` | `rccli start Geo1` | Start a volume |
| `stop <volumeName>` | `rccli stop Geo1` | Stop a volume |
| `drop <volumeName>` | `rccli drop Geo1` | Drop a volume and delete its data |
| `genParity <volumeName>` | `rccli genParity Geo1` | Generate parity blocks for all placement groups |
| `tracePuts <volumeName>` | `rccli tracePuts Geo1` | Import random data sampled from trace |
| `traceGets <volumeName>` | `rccli traceGets Geo1` | Evaluate get operations using traces |
| `traceDgets <volumeName>` | `rccli traceDgets Geo1` | Evaluate degraded get operations using traces |
| `foregroundTraceGets <volumeName>` | `rccli foregroundTraceGets Geo1` | Run foreground get requests from trace |
| `recovery <volumeName>` | `rccli recovery Geo1` | Simulate brick failure and recover the first brick |

Global flag: `--dirAddr` (default: `localhost:30100`) - Directory service address.

## Evaluation Workflow

### 1. Import Trace Data

Ensure the volume is clean (no existing objects), then:

```sh
rccli tracePuts <VolumeName>
```

Check log at `/usr/local/var/log/` to confirm completion.

### 2. Generate Parity

```sh
rccli genParity <VolumeName>
```

Wait for completion in the log before proceeding.

### 3. Test Degraded Read

```sh
rccli traceDgets <VolumeName>
```

Results are written to the log.

### 4. Test Recovery

```sh
rccli recovery <VolumeName>
```

DGStor simulates a brick failure and begins recovery. Monitor progress in the log.

### 5. Test Degraded Read Under Foreground Load

```sh
rccli foregroundTraceGets <VolumeName>   # Start foreground requests
rccli traceDgets <VolumeName>            # Test degraded read concurrently
rccli stop <VolumeName>                  # Stop the volume when done
```

### 6. Cleanup

```sh
rccli drop <VolumeName>
```

## System Constants

| Constant | Value | Description |
|----------|-------|-------------|
| Default Dir Port | `30100` | Directory service default port |
| Default HTTP Port | `30888` | HTTP server default port |
| Heartbeat Interval | `5s` | Brick health check interval |
| Max Client Bandwidth | `1 Gbps` | Default client bandwidth limit |
| SSD Max Object Size | `4 MB` | Maximum single object size on SSD |
| HDD Max Object Size | `512 MB` | Maximum single object size on HDD |
| Index Replication | `3` | Index metadata replication factor |
| Recovery Concurrency | `16` | Concurrent recovery threads |
| RPC Concurrency | `128` | Concurrent RPC handling threads |
| Foreground Clients | `8` | Max foreground clients per machine |

## Performance Tuning

Enable huge pages to improve I/O performance:

```sh
echo 1000 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages
```

## Logs

All logs are stored under `/usr/local/var/log/`:
- `dir.log` - Directory service log
- `<VolumeName>/http.log` - HTTP server log per volume
- `<VolumeName>/<brick>.log` - Storage server log per brick

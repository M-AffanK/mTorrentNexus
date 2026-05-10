# mTorrentNexus
C++ backend for the distributed file sharing system. Contains two components: a **Coordinator Server** that manages metadata and orchestrates chunk transfers, and a **Storage Node** that stores chunk data on disk.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

## Coordinator Server

Tracks file metadata, plans chunk placement across nodes, and streams chunks to clients on download. Does not store file data itself. Metadata is persisted to disk and survives restarts.

```bash
./build/server/mtorrent_server <port> <replication_factor> <metadata_file>
```
- Arg 1: listen port (default `9090`)
- Arg 2: replication factor (default `2`)
- Arg 3: metadata file path (default `server_metadata.db`)

```bash
# Example
./build/server/mtorrent_server 9090 2 server_metadata.db
```

## Storage Node

Stores raw chunk files under a local directory. Registers with the coordinator on startup.

```bash
./build/storage_node/mtorrent_storage_node <node_id> <listen_port> <storage_dir> \
  <coordinator_host> <coordinator_port> <advertised_host> <capacity_bytes>
```
Arguments:
- `<node_id>`: unique node id (e.g. `node1`)
- `<listen_port>`: node TCP port
- `<storage_dir>`: local folder where chunk files are stored
- `<coordinator_host>` / `<coordinator_port>`: coordinator address
- `<advertised_host>`: host/IP sent to coordinator (must be reachable by coordinator)
- `<capacity_bytes>`: node capacity metadata

```bash
# Example – run three nodes
./build/storage_node/mtorrent_storage_node node1 10001 ./node1_data 127.0.0.1 9090 127.0.0.1 5000000000
./build/storage_node/mtorrent_storage_node node2 10002 ./node2_data 127.0.0.1 9090 127.0.0.1 5000000000
./build/storage_node/mtorrent_storage_node node3 10003 ./node3_data 127.0.0.1 9090 127.0.0.1 5000000000
```

## TCP Protocol

All commands are newline-delimited (`\n`). Binary payloads follow immediately after the command line.

### Client → Coordinator

**Upload**
1. `UPLOAD_INIT <filename> <filesize> <chunksize> <chunkcount>` → `OK <uploadId> <fileId>` + chunk plan + `END_PLAN`
2. `UPLOAD_CHUNK <uploadId> <index> <bytes>` + binary → `OK chunk_stored`
3. `UPLOAD_FINISH <uploadId>` → `OK <fileId> <shareUrl>`

**Download**
- `DOWNLOAD <fileId>` → `OK <fileName> <fileSize> <chunkSize> <chunkCount>` + chunks + `END_FILE`
- `DOWNLOAD_SHARE <token|url>` → same as above

**Browse**
- `LIST_FILES` → `OK <count>` + `FILE ...` lines + `END_LIST`
- `SEARCH_FILES <query>` → same format + `END_SEARCH`
- `GET_SHARE_LINK <fileId>` → `OK mtorrent://share/<token>`

### Coordinator → Storage Node

- `STORE_CHUNK <fileId> <index> <bytes>` + binary → `OK`
- `GET_CHUNK <fileId> <index>` → `CHUNK_DATA <bytes>` + binary

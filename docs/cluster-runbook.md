# Cluster Runbook

This is the first real cluster drill. Keep the goal narrow: prove the current coordinator and peer runtime survive real hosts, real disks, TLS, process restarts, and continuous scheduler passes.

## Target Shape

- 1 coordinator host.
- 5 to 8 peer hosts or VMs.
- Persistent peer storage on local disks.
- TLS enabled on every peer runtime.
- One coordinator SQLite state database.
- Scheduler running continuously.

## Build

Run this on every host after checkout:

```bash
npm ci
npm run build
```

## Coordinator Identity

On the coordinator host:

```bash
mkdir -p cluster
node dist/cli.js identity --id coordinator-1 --out cluster/coordinator.identity.json
```

Copy the printed `publicKeyBase64` value. Every peer must trust that coordinator public key.

## Peer TLS

Use a real certificate when possible. For a lab-only self-signed peer certificate:

```bash
mkdir -p /etc/kryden
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /etc/kryden/peer.key \
  -out /etc/kryden/peer.crt \
  -days 7 \
  -subj "/CN=$(hostname -f)" \
  -addext "subjectAltName=DNS:$(hostname -f),IP:<peer-ip>"
```

If using self-signed certs, the coordinator commands need `--tls-insecure` for this first drill only.

## Start Peers

On each peer host:

```bash
export KRYDEN_PEER_ID=peer-1
export KRYDEN_CAPACITY_BYTES=10737418240
export KRYDEN_PORT=9443
export KRYDEN_STORAGE_DIR=/var/lib/kryden/peer-1
export KRYDEN_TLS_CERT=/etc/kryden/peer.crt
export KRYDEN_TLS_KEY=/etc/kryden/peer.key
export KRYDEN_FAILURE_BUCKET=rack-a
export KRYDEN_TRUSTED_AUTHORITY_ID=coordinator-1
export KRYDEN_TRUSTED_AUTHORITY_PUBLIC_KEY_BASE64=<coordinator-public-key-base64>

./scripts/run-peer.sh
```

Confirm from the coordinator:

```bash
curl -k https://peer-1.example.net:9443/health
curl -k https://peer-1.example.net:9443/heartbeat
```

## Coordinator Config

Copy and edit the example:

```bash
cp examples/cluster.coordinator.example.json cluster/coordinator.json
```

Set `peerEndpoints` to the actual peer URLs. Set `tlsInsecure` to `true` only for self-signed lab certificates.

## Put A Test Object

On the coordinator:

```bash
node dist/cli.js cluster-put ./test-object.bin \
  --config cluster/coordinator.json \
  --out cluster/test-object.kryden.json
```

This stores encrypted shards on remote peers and tracks the manifest in the coordinator SQLite state.

## Run Scheduler

Start the continuous coordinator loop:

```bash
node dist/cli.js cluster-scheduler --config cluster/coordinator.json
```

For a one-shot validation pass:

```bash
node dist/cli.js cluster-scheduler --config cluster/coordinator.json --run-once
```

The scheduler prints one JSON line per run. Keep those logs.

## Inspect

Use the existing inspection commands against the coordinator state:

```bash
node dist/cli.js inspect run latest --state cluster/coordinator.sqlite
node dist/cli.js inspect degraded --state cluster/coordinator.sqlite
node dist/cli.js inspect object <content-id> --state cluster/coordinator.sqlite
node dist/cli.js inspect peer <peer-id> --state cluster/coordinator.sqlite
```

## Failure Drill

Run these one at a time, and inspect after each scheduler pass:

- Stop one peer process.
- Restart one peer process and confirm its shard store survives.
- Fill one peer disk close to its configured capacity.
- Add 500 ms latency between coordinator and one peer.
- Kill a peer during a repair write.
- Bring the peer back and confirm the next scheduler pass repairs or reports the remaining degradation.

Do not add new features during this drill. Capture the exact failure, command, logs, and inspection output.

## How to

### Set up

#### at startup
`redis-server <$conf-file> --loadmodule <path>/crdt.so CRDT.GID 2`

#### already started
`redis-cli module load <path>/crdt.so CRDT.GID 2`

### Use
`redis-cli set key val`
`redis-cli crdt.set key <gid> <timestamp> val`

`redis-cli get key`
`redis-cli crdt.get key`

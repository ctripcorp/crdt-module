## How to

### Set up

#### at startup
`redis-server <$conf-file> --loadmodule <path>/crdt.so`

#### already started
`redis-cli module load <path>/crdt.so`

### Use
`redis-cli set key val`

`redis-cli get key`
`redis-cli crdt.get key`

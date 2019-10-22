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


## Develop

### Mem Alloc/Free
- For temporary used mem/struct, using `Redis Module's` auto memory alloc.
- For permanent used mem/struct, pass a flag to tell function whether to copy or take directly
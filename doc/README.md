
## Valgrind Check

### Execution

Run on linux server:

`valgrind --track-origins=yes --suppressions=src/valgrind.sup --log-file=/home/xpipe/valgrind.txt --leak-check=full \
--show-leak-kinds=all src/redis-server slave.conf --loadmodule /home/xpipe/crdt.so`


Run on local:

- Jedis set -- set vals
- Jedis get -- check vals
- Jedis del -- cleanup for valgrind mem leak test

### Test Result

```
==3831== 1,728 bytes in 3 blocks are possibly lost in loss record 205 of 208
==3831==    at 0x4C2B975: calloc (vg_replace_malloc.c:711)
==3831==    by 0x4011F04: _dl_allocate_tls (in /usr/lib64/ld-2.17.so)
==3831==    by 0x53449C0: pthread_create@@GLIBC_2.2.5 (in /usr/lib64/libpthread-2.17.so)
==3831==    by 0x47A681: bioInit (bio.c:123)
==3831==    by 0x42F7ED: initServer (server.c:1973)
==3831==    by 0x4237A2: main (server.c:3855)


==3831== LEAK SUMMARY:
==3831==    definitely lost: 0 bytes in 0 blocks
==3831==    indirectly lost: 0 bytes in 0 blocks
==3831==      possibly lost: 1,728 bytes in 3 blocks
==3831==    still reachable: 40,883 bytes in 490 blocks
==3831==         suppressed: 0 bytes in 0 blocks
==3831==
==3831== For counts of detected and suppressed errors, rerun with: -v
==3831== ERROR SUMMARY: 1 errors from 1 contexts (suppressed: 0 from 0)
```
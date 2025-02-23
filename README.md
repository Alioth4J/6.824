# MIT 6.824, Spring 2020
## Lab1
current status:
```bash
$ sudo sh ./test-mr.sh
[sudo] password for alioth4j: 
*** Starting wc test.
read unix @->/var/tmp/824-mr-0: read: connection reset by peer
2025/02/19 13:59:35 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
2025/02/19 13:59:35 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
2025/02/19 13:59:35 cannot open 
--- wc test: PASS
*** Starting indexer test.
unexpected EOF
read unix @->/var/tmp/824-mr-0: read: connection reset by peer
2025/02/19 13:59:38 cannot open 
2025/02/19 13:59:38 cannot open 
--- indexer test: PASS
*** Starting map parallelism test.
unexpected EOF
unexpected EOF
2025/02/19 13:59:45 cannot open 
2025/02/19 13:59:45 cannot open 
--- map parallelism test: PASS
*** Starting reduce parallelism test.
2025/02/19 13:59:51 cannot open 
--- reduce parallelism test: PASS
2025/02/19 13:59:53 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
*** Starting crash test.
2025/02/19 14:02:53 dialing:dial unix /var/tmp/824-mr-0: connect: connection refused
2025/02/19 14:02:53 dialing:dial unix /var/tmp/824-mr-0: connect: connection refused
2025/02/19 14:02:53 dialing:dial unix /var/tmp/824-mr-0: connect: connection refused
sort: cannot read: 'mr-out*': No such file or directory
cmp: EOF on mr-crash-all which is empty
--- crash output is not the same as mr-correct-crash.txt
--- crash test: FAIL
*** FAILED SOME TESTS
```

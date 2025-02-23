# MIT 6.824, Spring 2020
## Lab1
Passed all tests.  
```bash
$ sudo sh test-mr.sh
*** Starting wc test.
read unix @->/var/tmp/824-mr-0: read: connection reset by peer
2025/02/23 22:42:29 dialing:dial-http unix /var/tmp/824-mr-0: unexpected EOF
2025/02/23 22:42:29 cannot open 
2025/02/23 22:42:29 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
--- wc test: PASS
*** Starting indexer test.
2025/02/23 22:42:32 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
2025/02/23 22:42:32 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
--- indexer test: PASS
*** Starting map parallelism test.
2025/02/23 22:42:39 dialing:dial-http unix /var/tmp/824-mr-0: read unix @->/var/tmp/824-mr-0: read: connection reset by peer
unexpected EOF
2025/02/23 22:42:39 cannot open 
--- map parallelism test: PASS
*** Starting reduce parallelism test.
2025/02/23 22:42:45 cannot open 
--- reduce parallelism test: PASS
2025/02/23 22:42:48 dialing:dial-http unix /var/tmp/824-mr-0: unexpected EOF
*** Starting crash test.
2025/02/23 22:43:13 cannot open 
2025/02/23 22:43:14 cannot open 
2025/02/23 22:43:14 cannot open 
2025/02/23 22:43:15 cannot open 
2025/02/23 22:43:15 cannot open 
2025/02/23 22:43:16 cannot open 
2025/02/23 22:43:16 cannot open 
2025/02/23 22:43:16 cannot open 
2025/02/23 22:43:17 cannot open 
2025/02/23 22:43:17 cannot open 
2025/02/23 22:43:17 cannot open 
2025/02/23 22:43:18 cannot open 
2025/02/23 22:43:18 cannot open 
2025/02/23 22:43:18 cannot open 
2025/02/23 22:43:19 cannot open 
2025/02/23 22:43:19 cannot open 
2025/02/23 22:43:19 cannot open 
2025/02/23 22:43:20 cannot open 
2025/02/23 22:43:20 cannot open 
2025/02/23 22:43:20 cannot open 
2025/02/23 22:43:21 cannot open 
2025/02/23 22:43:21 cannot open 
2025/02/23 22:43:21 cannot open 
2025/02/23 22:43:22 cannot open 
2025/02/23 22:43:22 cannot open 
2025/02/23 22:43:23 cannot open 
unexpected EOF
unexpected EOF
2025/02/23 22:43:25 dialing:dial-http unix /var/tmp/824-mr-0: unexpected EOF
2025/02/23 22:43:25 cannot open 
2025/02/23 22:43:25 cannot open 
--- crash test: PASS
*** PASSED ALL TESTS
```

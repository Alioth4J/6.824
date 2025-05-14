# MIT 6.824, Spring 2020
## Lab1 MapReduce
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
2025/02/23 22:42:39 $ go test -run 3A
Test: one client (3A) ...
  ... Passed --  15.4  5 12849 1393
Test: many clients (3A) ...
^Csignal: interrupt
FAIL	github.com/alioth4j/6.824/src/kvraft	56.069s
cannot open 
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

## Lab2 Raft
### 2A
Passed.  
```bash
$ go test -run 2A
Test (2A): initial election ...
  ... Passed --   3.0  3   30    7594    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3   80   14810    0
PASS
ok  	github.com/alioth4j/6.824/src/raft	7.493s
```
### 2B
Passed.  
```bash
$ go test -run 2B
Test (2B): basic agreement ...
  ... Passed --   0.6  3   16    4140    3
Test (2B): RPC byte count ...
  ... Passed --   1.4  3   48  113112   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   4.1  3   98   24735    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.4  5  208   42858    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.5  3   18    4933    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   4.1  3  152   35343    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  23.3  5 2590 2101590  104
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.0  3   60   16764   12
PASS
ok  	github.com/alioth4j/6.824/src/raft	39.506s
```
### 2C
Passed.  
```bash
$ go test -run 2C
Test (2C): basic persistence ...
  ... Passed --   3.8  3  136   34751    6
Test (2C): more persistence ...
  ... Passed --  15.1  5 1494  332482   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.7  3   48   12540    4
Test (2C): Figure 8 ...
  ... Passed --  24.0  5 1096  240238   38
Test (2C): unreliable agreement ...
  ... Passed --   1.5  5 1084  365479  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  32.8  5 13354 25032486   61
Test (2C): churn ...
  ... Passed --  16.5  5 16236 53451674 3736
Test (2C): unreliable churn ...
  ... Passed --  16.1  5 3464 2456591  476
PASS
ok  	github.com/alioth4j/6.824/src/raft	111.354s
```

## Lab3 KVRaft
### 3A
Partial passed.  
```bash
$ go test -run 3A
Test: one client (3A) ...
  ... Passed --  15.2  5  7531 1379
Test: many clients (3A) ...
^Csignal: interrupt
FAIL	github.com/alioth4j/6.824/src/kvraft	121.323s
```

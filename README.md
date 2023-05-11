# L1 Replay

A tool that replays requests to an L1 and outputs performance metrics.

## Installation

```sh
$ npm install
```

## Usage

```sh
$ node index.js replay -f sample-logs.ndjson
```

Use http1 (default):

```sh
$ node index.js replay --http=1
```

Use http2

```sh
$ node index.js replay --http=2
```

Use L1 ip address

```sh
$ node index.js replay --ip=1.2.3.4
```

Limit number of logs.

```sh
$ node index.js replay -n 100
```

Limit number of logs by duration. `-d 5` means "Replay 5 minutes of logs from the start of the log file".

```sh
$ node index.js replay -d 5
```

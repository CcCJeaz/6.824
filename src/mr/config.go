package mr

import "time"

// 中间文件命名 reduce号-taskId号
const mapTemFileformat = "mr-tem-%d-%d"

const reduceTemFileformat = "mr-tem-reduce-*"

// server-timeout
const serverTimeout = time.Second*5

// client-ping-frequency
const clienPingFrequency = time.Second

const resultFilenameFormat = "mr-out-%d"


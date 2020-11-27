go build -buildmode=plugin ../mrapps/wc.go
go build -race mrmaster.go
go build -race mrworker.go
go run -race mrmaster.go pg*.txt& 
#go run -race mrmaster.go test.txt&
go run mrworker.go wc.so&
go run mrworker.go wc.so&
go run mrworker.go wc.so&


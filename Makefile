.PHONY: proto
proto:
	protoc remote_execution.proto --go_out=plugins=grpc:.
	protoc google/api/*.proto --go_out=plugins=grpc:.
	protoc google/longrunning/*.proto --go_out=plugins=grpc:.
	protoc google/protobuf/any.proto --go_out=plugins=grpc:.
	protoc google/protobuf/duration.proto --go_out=plugins=grpc:.
	protoc google/protobuf/empty.proto --go_out=plugins=grpc:.
	protoc google/rpc/status.proto --go_out=plugins=grpc:.
	protoc google/rpc/code.proto --go_out=plugins=grpc:.
	protoc google/watcher/v1/watch.proto --go_out=plugins=grpc:.

out/remote-executor: server/main.go out
	go build -o out/remote-executor server/main.go

.PHONY: server
server: out/remote-executor
	./out/remote-executor --verbosity debug --bucket r2d4minikube

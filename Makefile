BINARY := bin/falcon
MODULE := github.com/FalconEngine/falcon

.PHONY: build test bench vet clean run demo cluster-demo chaos proto

build:
	go build -o $(BINARY) ./cmd/falcon

test:
	go test ./...

bench:
	go test -bench=. -benchmem ./...

vet:
	go vet ./...

clean:
	rm -rf bin data

run: build
	./$(BINARY) --config config.yaml

demo:
	bash scripts/demo.sh

cluster-demo:
	bash scripts/cluster-demo.sh

chaos:
	bash scripts/chaos-test.sh

# proto 代码生成（需安装 protoc 与 protoc-gen-go/protoc-gen-go-grpc）。
# 当前 Go 侧为手写等价实现（transport/server.go + JSON codec），
# 安装工具链后可执行本目标切换为生成链路。
proto:
	protoc --go_out=. --go-grpc_out=. \
	  --go_opt=module=github.com/FalconEngine/falcon \
	  --go-grpc_opt=module=github.com/FalconEngine/falcon \
	  transport/proto/transport.proto

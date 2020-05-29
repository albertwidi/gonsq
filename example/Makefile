GOPATH := $(shell go env GOPATH)

PROTO_FILE = ./message/id.proto

.PHONY: compile-proto
compile-proto:
	@echo ">>> generating grpc go file"
	@protoc -I/usr/local/include -I. \
		-I$(GOPATH)/src \
		--go_out=plugins=grpc:./ \
		$(PROTO_FILE)
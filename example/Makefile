GOPATH := $(shell go env GOPATH)

PROTO_FILE = ./message/id.proto

.PHONY: compile-proto
compile-proto:
	@echo ">>> generating grpc go file"
	@protoc -I/usr/local/include -I. \
		-I$(GOPATH)/src \
		--go_out=plugins=grpc:./ \
		$(PROTO_FILE)

.PHONY: compose-up
compose-up:
	@docker-compose build --no-cache
	@docker-compose up -d --no-build
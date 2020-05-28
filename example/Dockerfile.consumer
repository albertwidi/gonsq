FROM golang:1.14.2
WORKDIR /gonsq/example/consumer
COPY main.go .
RUN go mod init github.com/albertwidi/gonsq && \
    CGO_ENABLED=0 GOOS=linux go build -v -o consumer main.go
test:
	@go test -v --cover -count=1 -timeout 1m ./...

.PHONY: ci
ci:
	make test
	@cd example && docker-compose build
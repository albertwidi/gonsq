test:
	@go test -v --cover -count=1 -timeout 15m ./...

.PHONY: ci
ci:
	test
	@cd example && docker-compose build
	@cd example && docker build -f Dockerfile.pkggo .
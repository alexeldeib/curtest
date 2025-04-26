# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Test Commands
- Build: `go build ./...`
- Run: `go run main.go`
- Test all: `go test ./...`
- Test specific file: `go test -v ./registry_test.go`
- Test single function: `go test -v -run TestRegistryMirror/Test1_FallbackToUpstream`
- Skip long-running tests: `go test -short ./...`
- Format code: `go fmt ./...`
- Lint code: `go vet ./...`

## Code Style Guidelines
- Follow standard Go formatting (enforced by `go fmt`)
- Use meaningful error messages with `fmt.Errorf` and wrap errors: `fmt.Errorf("context: %w", err)`
- Use descriptive variable names with camelCase for private or PascalCase for exported
- Add comments for exported functions, types, and complex logic
- Organize imports: standard library first, then third-party
- Use mutex protection for concurrent operations
- Use appropriate context timeouts for network operations
- Follow standard Go error handling patterns with early returns
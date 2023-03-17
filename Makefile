build:
	go mod download
	go build -o godb-server ./cmd/

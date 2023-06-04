

test:
	go test . -v

coverage:
	go test . -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html
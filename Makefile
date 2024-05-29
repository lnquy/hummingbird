test:
	go test -v --cover ./...

bench:
	go test -v --run= --bench=. --cpu=1,2,4,8; \
	cd dashtable; \
	go test -v --run= --bench=. --cpu=1,2,4,8


build:
	rm -rf build
	gox -osarch="linux/amd64" -output="build/kadiradb"

docker: build
	docker build -t kadirahq/kadiradb ./

publish: docker
	docker push kadirahq/kadiradb:latest

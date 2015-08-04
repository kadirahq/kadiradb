docker:
	rm -rf build
	gox -osarch="linux/amd64" -output="build/kadiradb"
	docker build -t kadirahq/kadiradb ./
	rm -rf build

publish: docker
	docker push kadirahq/kadiradb:latest

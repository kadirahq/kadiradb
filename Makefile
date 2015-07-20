docker:
	rm -rf build
	gox -osarch="linux/amd64" -output="build/kadira-metrics"
	docker build -t kadirahq/kadiradb-metrics ./
	rm -rf build

publish: docker
	docker push kadirahq/kadiradb-metrics:latest

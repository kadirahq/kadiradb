
kadiradb: clean
	gox -osarch="linux/amd64" -output="build/kadiradb"
	docker build -t kadirahq/kadiradb ./

publish: kadiradb
	docker push kadirahq/kadiradb:latest

clean:
	rm -rf build

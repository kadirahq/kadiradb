
kadiradb: clean
	GOOS="linux" GOARCH="amd64" go build -o build/kadiradb -i -a .
	docker build -t kadirahq/kadiradb ./

publish: kadiradb
	docker push kadirahq/kadiradb:latest

clean:
	rm -rf build

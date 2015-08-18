FROM alpine:3.2

COPY build/kadiradb /bin/kadiradb
CMD ["sh", "-c", "/bin/kadiradb -data=/data"]

EXPOSE 6060
EXPOSE 19000

VOLUME ["/data"]

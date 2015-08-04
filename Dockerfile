FROM alpine:3.2

COPY build/kadiradb /bin/kadiradb
CMD ["sh", "-c", "/bin/kadiradb -addr=:19000 -path=/data -init=\"$KMDB_INIT\""]

EXPOSE 19000
VOLUME ["/data"]

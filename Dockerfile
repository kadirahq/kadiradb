FROM alpine:3.2

COPY build/kadira-metrics /bin/kadira-metrics
CMD ["sh", "-c", "/bin/kadira-metrics -addr=:19000 -path=/data -init=\"$KMDB_INIT\""]

EXPOSE 19000
VOLUME ["/data"]

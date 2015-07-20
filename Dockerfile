FROM alpine:3.2
COPY build/kadira-metrics /bin/kadira-metrics

ENTRYPOINT ["/bin/kadira-metrics"]
CMD ["-addr", ":19000", "-path", "/data"]

EXPOSE 19000
VOLUME ["/data"]

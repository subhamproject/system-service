FROM golang:1.19.4-buster


#copy binary
COPY bin/system-service system-service

EXPOSE 9093
# Run the binary program produced by `go install`
CMD ["./system-service"]



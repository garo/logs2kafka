FROM alpine

ADD logs2kafka /
CMD /logs2kafka logs2kafka

EXPOSE 8061

# BUILD: # docker run --rm -v $PWD:/usr/src/$(basename $PWD) -w /usr/src/$(basename $PWD) golang:latest /bin/bash -c "go get -v ./...; go build -a -ldflags '-s' -tags netgo -installsuffix netgo -v; ldd $(basename $PWD)"

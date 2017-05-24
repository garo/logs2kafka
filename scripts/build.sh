docker run --rm -v ${PWD}:/go/src/github.com/UnityTech/logs2kafka -w /go/src/github.com/UnityTech/logs2kafka golang /bin/bash -c "go get -v github.com/kardianos/govendor && govendor sync && CGO_ENABLED=0 go build -a --installsuffix cgo -ldflags='-s -w -X main.builddate=`date -u +\"%Y-%m-%dT%H:%M:%SZ\"`' -v && ! ldd logs2kafka"

docker build -t $image .

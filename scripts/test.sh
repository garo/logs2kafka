docker run --rm -v ${PWD}:/go/src/github.com/UnityTech/logs2kafka -w /go/src/github.com/UnityTech/logs2kafka golang /bin/bash -c "go get -v github.com/kardianos/govendor && govendor sync && go test -v "


FROM golang:onbuild
RUN go get github.com/tools/godep
RUN godep restore || True

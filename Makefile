GOPATH:=$(PWD):${GOPATH}
export GOPATH
flags=-ldflags="-s -w"

all: build

build:
	go clean; rm -rf pkg transfer2go*; go build ${flags}

build_all: build build_osx build_linux build_power8 build_arm64

build_osx:
	go clean; rm -rf pkg transfer2go_osx; GOOS=darwin go build ${flags}
	mv transfer2go transfer2go_osx

build_linux:
	go clean; rm -rf pkg transfer2go_linux; GOOS=linux go build ${flags}
	mv transfer2go transfer2go_linux

build_power8:
	go clean; rm -rf pkg transfer2go_power8; GOARCH=ppc64le GOOS=linux go build ${flags}
	mv transfer2go transfer2go_power8

build_arm64:
	go clean; rm -rf pkg transfer2go_arm64; GOARCH=arm64 GOOS=linux go build ${flags}
	mv transfer2go transfer2go_arm64

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	cd test; go test

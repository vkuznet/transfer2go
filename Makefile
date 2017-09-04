GOPATH:=$(PWD):${GOPATH}
export GOPATH
OS := $(shell uname)
ifeq ($(OS),Darwin)
flags=-ldflags="-s -w"
else
flags=-ldflags="-s -w -extldflags -static"
endif
TAG := $(shell git tag | sort -r | head -n 1)

all: build

build:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg transfer2go*; go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go

build_all: build build_osx build_linux build_power8 build_arm64

build_osx:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg transfer2go_osx; GOOS=darwin go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go
	mv transfer2go transfer2go_osx

build_linux:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg transfer2go_linux; GOOS=linux go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go
	mv transfer2go transfer2go_linux

build_power8:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg transfer2go_power8; GOARCH=ppc64le GOOS=linux go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go
	mv transfer2go transfer2go_power8

build_arm64:
	sed -i -e "s,{{VERSION}},$(TAG),g" main.go
	go clean; rm -rf pkg transfer2go_arm64; GOARCH=arm64 GOOS=linux go build ${flags}
	sed -i -e "s,$(TAG),{{VERSION}},g" main.go
	mv transfer2go transfer2go_arm64

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	cd test; go test

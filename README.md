# transfer2go

[![Build Status](https://travis-ci.org/vkuznet/transfer2go.svg?branch=master)](https://travis-ci.org/vkuznet/transfer2go)
[![Go Report Card](https://goreportcard.com/badge/github.com/vkuznet/transfer2go)](https://goreportcard.com/report/github.com/vkuznet/transfer2go)
[![GoDoc](https://godoc.org/github.com/vkuznet/transfer2go?status.svg)](https://godoc.org/github.com/vkuznet/transfer2go)

Go implementation of CMS
[PhEDEx](https://www.researchgate.net/publication/228732867_Data_transfer_infrastructure_for_CMS_data_taking)
distributed, loosly coupled agents for CMS transfering data.

### How it works
The PhEDEx agents are loosly coupled distributed agents which acts as
DNS servers. They can be deployed at any site which is capable to
serve data upon requests. The holds local Trivial File Catalog (TFC)
about files on a site and allows client to discover a specific file
and transfer it upon request to remote site (via agent request).
The file transfer should be fault-tolerant and handle various
failures during transfer as well as support various transfer
technologies. To name a few, we can transfer files via HTTP or
call SRM, etc.

The client may ask the following questions:
- find a file, the request can be send to any agent and it can look-up
if file is present in local TFC or send request to other agents and asks
who has this file. 

- transfer file, the agent who has a file initiate a transfer
request, i.e. fire up transfer goroutine which transfer
file via default and back-up protocol and yield back
request (via channel) if transfer is completed.
If request failed it can be scheduled again at a later time.

- agents should communicate with each other and exchange info
what they have, what is their load, etc. We need embeded support
for various metrics.

- shutdown the agent, to do that agent should look-up its pending
or request in transfer, finish them up and do clean-up.

So, we need:
- Transfer request object
  - file
  - source site
  - destination site
  - latency, i.e. when to start
- TFC which knows about local files
- registration of transfer tool/protocol, e.g.
I have running agent and later I installed on a system srm tool.
I want to register it in agent and make it available for usage.
Post to agent a JSON with description of transfer tool, e.g.
{"tool": "/usr/bin/srm", "priority":0}
location of the tool and its priority in hierarchy of tools,
0 means it will be used first.
- agent should have ability to register itself with other agent(s)
/register?uri=https://cern.ch/agent
it means that each agent has an internal list of other agents
- list file API which shows what agents has (dump its TFC)
- transfer API which initiates the transfer
- site list API, agent should find out how to map request
to an agent, e.g. transfer file A to site X. Find agent which can
talk to site X, resolve request
transfer LFN_a to siteX
- store API which knows how to store given file into its storage
- check md5 hash of file
- ask if file exists on siteX and return md5 hash of it
- once agent receives request to store file it should check if this file exists
  in its storage, it will allows to handle failures, e.g.
  if agent who initiate transfer got request to transfer of 10 files it may
  go down in a middle of the process. But it does not hold the notion
  of which transfer it had, instead its up to the client. Client can
  fire up any number of times the tranfer request. If the file exists
  on remote agent, it will get ok, otherwise it will initiate a transfer.
- transfer priority
- generate single executable which can be used as a server or as a client


### Examples
```
# start the server on port 8989 and use /tmp dir as catalog
transfer2go -uri :8989 -catalog /tmp

# place request from a client via HTTP
curl -v -H "Content-Type: application/json" -d '{"version":"123",
"data":[{"file":"fileA", "source":"source", "destination":"destination",
"latency":0}, {"file":"file2", "source":"source", "destination":"destination",
"latency":1}]}' http://localhost:8989

# start server mode and register agent with another one and use catalog
# from file.db (e.g. SQLite DB)
transfer2go -uri :8989 -register http://cern.ch/agent -catalog file.db

# will register with given agents
transfer2go -uri :8989 -register http://cern.ch/agent,http://fnal.gov,MyFriend

# request to transfer
transfer2go -agent http://cern.ch/agent -transfer fileA -to T1_CH_CERN
transfer2go -agent http://fnal.gov/agent -transfer fileA -to http://localhost
transfer2go -agent T1_CH_CERN -transfer fileA -to MyFriend

# return URIs of agents who has the file
transfer2go -agent MyFriend -find /fileA

# return status of transfers and pending requests
transfer2go -agent http://fnal.gov -status

# will shutdown a given agent
transfer2go -agent T1_CH_CERN -shutdown
```

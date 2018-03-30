package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	rpc "github.com/emmansun/go-jsonrpc"
	log "github.com/inconshreveable/log15"
	cli "gopkg.in/urfave/cli.v1"
)

type httpRpcServer struct {
	host         string
	port         int
	corsDomains  []string
	virtualHosts []string
	endpoint     string
	handler      *rpc.Server
	httpServer   *http.Server
}

var server *httpRpcServer

var (
	RPCListenAddrFlag = cli.StringFlag{
		Name:  "rpcaddr",
		Usage: "HTTP-RPC server listening interface",
		Value: "localhost",
	}
	RPCPortFlag = cli.IntFlag{
		Name:  "rpcport",
		Usage: "HTTP-RPC server listening port",
		Value: 8545,
	}
	RPCCORSDomainFlag = cli.StringFlag{
		Name:  "rpccorsdomain",
		Usage: "Comma separated list of domains from which to accept cross origin requests (browser enforced)",
		Value: "",
	}
	RPCVirtualHostsFlag = cli.StringFlag{
		Name:  "rpcvhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.",
		Value: strings.Join([]string{"localhost"}, ","),
	}
)

// splitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func splitAndTrim(input string) []string {
	result := strings.Split(input, ",")
	for i, r := range result {
		result[i] = strings.TrimSpace(r)
	}
	return result
}

// setHTTP creates the HTTP RPC listener interface string from the set
// command line flags, returning empty if the HTTP endpoint is disabled.
func setHTTP(ctx *cli.Context, rpcSrv *httpRpcServer) {
	if rpcSrv.host == "" {
		rpcSrv.host = "127.0.0.1"
		if ctx.GlobalIsSet(RPCListenAddrFlag.Name) {
			rpcSrv.host = ctx.GlobalString(RPCListenAddrFlag.Name)
		}
	}
	if rpcSrv.port == 0 {
		rpcSrv.port = 8454
		if ctx.GlobalIsSet(RPCPortFlag.Name) {
			rpcSrv.port = ctx.GlobalInt(RPCPortFlag.Name)
		}
	}
	rpcSrv.endpoint = fmt.Sprintf("%s:%d", rpcSrv.host, rpcSrv.port)
	if ctx.GlobalIsSet(RPCCORSDomainFlag.Name) {
		rpcSrv.corsDomains = splitAndTrim(ctx.GlobalString(RPCCORSDomainFlag.Name))
	}
	if ctx.GlobalIsSet(RPCVirtualHostsFlag.Name) {
		rpcSrv.virtualHosts = splitAndTrim(ctx.GlobalString(RPCVirtualHostsFlag.Name))
	}
}

func serveRpc(c *cli.Context) error {
	server = new(httpRpcServer)
	setHTTP(c, server)
	// Short circuit if the HTTP endpoint isn't being exposed
	if server.endpoint == "" {
		return nil
	}
	// Register all the APIs exposed by the services
	handler := rpc.NewServer()
	var (
		listener net.Listener
		err      error
	)
	if listener, err = net.Listen("tcp", server.endpoint); err != nil {
		return err
	}
	server.handler = handler
	server.httpServer = rpc.NewHTTPServer(server.corsDomains, server.virtualHosts, server.handler)
	go server.httpServer.Serve(listener)
	log.Info("HTTP endpoint opened", "url", fmt.Sprintf("http://%s", server.endpoint), "cors", strings.Join(server.corsDomains, ","), "vhosts", strings.Join(server.virtualHosts, ","))
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "http rpc server"
	app.Flags = []cli.Flag{RPCListenAddrFlag, RPCPortFlag, RPCCORSDomainFlag, RPCVirtualHostsFlag}
	app.Action = serveRpc
	err := app.Run(os.Args)
	if err != nil {
		log.Error("Error", err)
	}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	log.Info("Receive exit signal.", "singal", <-sigchan)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.httpServer.Shutdown(ctx)
	server.handler.Stop()
	log.Info("RPC Server gracefully stopped")
}

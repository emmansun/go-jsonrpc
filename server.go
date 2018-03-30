// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/inconshreveable/log15"
	set "gopkg.in/fatih/set.v0"
)

const MetadataApi = "rpc"

// CodecOption specifies which type of messages this codec supports
type CodecOption int

const (
	// OptionMethodInvocation is an indication that the codec supports RPC method calls
	OptionMethodInvocation CodecOption = 1 << iota

	// OptionSubscriptions is an indication that the codec suports RPC notifications
	OptionSubscriptions = 1 << iota // support pub sub
)

// NewServer will create a new server instance with no registered handlers.
func NewServer() *Server {
	server := &Server{
		services: make(serviceRegistry),
		codecs:   set.New(),
		run:      1,
	}

	// register a default service which will provide meta information about the RPC service such as the services and
	// methods it offers.
	rpcService := &RPCService{server}
	server.RegisterName(MetadataApi, rpcService)

	return server
}

// RPCService gives meta information about the server.
// e.g. gives information about the loaded modules.
type RPCService struct {
	server *Server
}

// Modules returns the list of RPC services with their version number
func (s *RPCService) Modules() map[string]string {
	modules := make(map[string]string)
	for name := range s.server.services {
		modules[name] = "1.0"
	}
	return modules
}

func newService(name string, rcvr interface{}) (*service, error) {
	svc := new(service)
	svc.name = name
	svc.typ = reflect.TypeOf(rcvr)
	if name == "" {
		return nil, fmt.Errorf("no service name for type %s", svc.typ.String())
	}
	return svc, nil
}

func (s *Server) initServicesIfNeed() {
	if s.services == nil {
		s.services = make(serviceRegistry)
	}
}

// RegisterName will create a service for the given rcvr type under the given name. When no methods on the given rcvr
// match the criteria to be either a RPC method or a subscription an error is returned. Otherwise a new service is
// created and added to the service collection this server instance serves.
func (s *Server) RegisterName(name string, rcvr interface{}) error {
	s.initServicesIfNeed()

	// new service
	svc, err := newService(name, rcvr)
	if err != nil {
		return err
	}

	rcvrVal := reflect.ValueOf(rcvr)

	if !isExported(reflect.Indirect(rcvrVal).Type().Name()) {
		return fmt.Errorf("%s is not exported", reflect.Indirect(rcvrVal).Type().Name())
	}

	methods, subscriptions := suitableCallbacks(rcvrVal, svc.typ)

	// already a previous service register under given sname, merge methods/subscriptions
	if regsvc, present := s.services[name]; present {
		if len(methods) == 0 && len(subscriptions) == 0 {
			return fmt.Errorf("Service %T doesn't have any suitable methods/subscriptions to expose", rcvr)
		}
		for _, m := range methods {
			regsvc.callbacks[formatName(m.method.Name)] = m
		}
		for _, s := range subscriptions {
			regsvc.subscriptions[formatName(s.method.Name)] = s
		}
		return nil
	}

	svc.callbacks, svc.subscriptions = methods, subscriptions

	if len(svc.callbacks) == 0 && len(svc.subscriptions) == 0 {
		return fmt.Errorf("Service %T doesn't have any suitable methods/subscriptions to expose", rcvr)
	}

	s.services[svc.name] = svc
	return nil
}

func (s *Server) isRunning() bool {
	return atomic.LoadInt32(&s.run) == 1
}

func (s *Server) isStopped() bool {
	return !s.isRunning()
}

func (s *Server) addCodec(codec ServerCodec) error {
	s.codecsMu.Lock()
	if s.isStopped() {
		s.codecsMu.Unlock()
		return &shutdownError{}
	}
	s.codecs.Add(codec)
	s.codecsMu.Unlock()
	return nil
}

func (s *Server) removeCodec(codec ServerCodec) {
	s.codecsMu.Lock()
	s.codecs.Remove(codec)
	s.codecsMu.Unlock()
}

func writeServerShutdownErr(codec ServerCodec, reqs []*serverRequest, batch bool) {
	err := &shutdownError{}
	if batch {
		resps := make([]interface{}, len(reqs))
		for i, r := range reqs {
			resps[i] = codec.CreateErrorResponse(&r.id, err)
		}
		codec.Write(resps)
	} else {
		codec.Write(codec.CreateErrorResponse(&reqs[0].id, err))
	}
}

// serveRequest will reads requests from the codec, calls the RPC callback and
// writes the response to the given codec.
//
// If singleShot is true it will process a single request, otherwise it will handle
// requests until the codec returns an error when reading a request (in most cases
// an EOF). It executes requests in parallel when singleShot is false.
func (s *Server) serveRequest(codec ServerCodec, singleShot bool, options CodecOption) error {
	var pend sync.WaitGroup

	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Error(string(buf))
		}
		s.removeCodec(codec)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// if the codec supports notification include a notifier that callbacks can use
	// to send notification to clients. It is thight to the codec/connection. If the
	// connection is closed the notifier will stop and cancels all active subscriptions.
	if options&OptionSubscriptions == OptionSubscriptions {
		ctx = context.WithValue(ctx, notifierKey{}, newNotifier(codec))
	}

	if err := s.addCodec(codec); err != nil {
		return err
	}

	// test if the server is ordered to stop
	for s.isRunning() {
		reqs, batch, err := s.readRequest(codec)
		log.Debug("Recieved new request", "reqs size", len(reqs))
		if err != nil {
			// If a parsing error occurred, send an error
			if err.Error() != "EOF" {
				codec.Write(codec.CreateErrorResponse(nil, err))
			}
			// Error or end of stream, wait for requests and tear down
			pend.Wait()
			return nil
		}

		// check if server is ordered to shutdown and return an error
		// telling the client that his request failed.
		if s.isStopped() {
			writeServerShutdownErr(codec, reqs, batch)
			return nil
		}

		// If a single shot request is executing, run and return immediately
		if singleShot {
			s.exec(ctx, codec, reqs, batch)
			return nil
		}

		// For multi-shot connections, start a goroutine to serve and loop back
		pend.Add(1)

		go func(reqs []*serverRequest, batch bool) {
			defer pend.Done()
			s.exec(ctx, codec, reqs, batch)
		}(reqs, batch)
	}
	return nil
}

// ServeCodec reads incoming requests from codec, calls the appropriate callback and writes the
// response back using the given codec. It will block until the codec is closed or the server is
// stopped. In either case the codec is closed.
func (s *Server) ServeCodec(codec ServerCodec, options CodecOption) {
	defer codec.Close()
	s.serveRequest(codec, false, options)
}

// ServeSingleRequest reads and processes a single RPC request from the given codec. It will not
// close the codec unless a non-recoverable error has occurred. Note, this method will return after
// a single request has been processed!
func (s *Server) ServeSingleRequest(codec ServerCodec, options CodecOption) {
	s.serveRequest(codec, true, options)
}

// Stop will stop reading new requests, wait for stopPendingRequestTimeout to allow pending requests to finish,
// close all codecs which will cancel pending requests/subscriptions.
func (s *Server) Stop() {
	if atomic.CompareAndSwapInt32(&s.run, 1, 0) {
		s.codecsMu.Lock()
		defer s.codecsMu.Unlock()
		s.codecs.Each(func(c interface{}) bool {
			c.(ServerCodec).Close()
			return true
		})
	}
}

func (s *Server) handleUnsubscribe(ctx context.Context, codec ServerCodec, req *serverRequest) interface{} {
	if len(req.args) >= 1 && req.args[0].Kind() == reflect.String {
		notifier, supported := NotifierFromContext(ctx)
		if !supported { // interface doesn't support subscriptions (e.g. http)
			return codec.CreateErrorResponse(&req.id, &callbackError{ErrNotificationsUnsupported.Error()})
		}

		subid := ID(req.args[0].String())
		if err := notifier.unsubscribe(subid); err != nil {
			return codec.CreateErrorResponse(&req.id, &callbackError{err.Error()})
		}

		return codec.CreateResponse(req.id, true)
	}
	return codec.CreateErrorResponse(&req.id, &invalidParamsError{"Expected subscription id as first argument"})
}

// createSubscription will call the subscription callback and returns the subscription id or error.
func (s *Server) createSubscription(ctx context.Context, c ServerCodec, req *serverRequest) (ID, error) {
	// subscription have as first argument the context following optional arguments
	args := []reflect.Value{req.callb.rcvr, reflect.ValueOf(ctx)}
	args = append(args, req.args...)
	reply := req.callb.method.Func.Call(args)

	if !reply[1].IsNil() { // subscription creation failed
		return "", reply[1].Interface().(error)
	}

	return reply[0].Interface().(*Subscription).ID, nil
}

func (s *Server) handleSubscribe(ctx context.Context, codec ServerCodec, req *serverRequest) (interface{}, func()) {
	subid, err := s.createSubscription(ctx, codec, req)
	if err != nil {
		return codec.CreateErrorResponse(&req.id, &callbackError{err.Error()}), nil
	}

	// active the subscription after the sub id was successfully sent to the client
	activateSub := func() {
		notifier, _ := NotifierFromContext(ctx)
		notifier.activate(subid, req.svcname)
	}

	return codec.CreateResponse(req.id, subid), activateSub
}

func (s *Server) handleRegularRequest(ctx context.Context, codec ServerCodec, req *serverRequest) interface{} {
	// regular RPC call, prepare arguments
	if len(req.args) != len(req.callb.argTypes) {
		rpcErr := &invalidParamsError{fmt.Sprintf("%s%s%s expects %d parameters, got %d",
			req.svcname, serviceMethodSeparator, req.callb.method.Name,
			len(req.callb.argTypes), len(req.args))}
		return codec.CreateErrorResponse(&req.id, rpcErr)
	}

	arguments := []reflect.Value{req.callb.rcvr}
	if req.callb.hasCtx {
		arguments = append(arguments, reflect.ValueOf(ctx))
	}
	if len(req.args) > 0 {
		arguments = append(arguments, req.args...)
	}

	// execute RPC method and return result
	reply := req.callb.method.Func.Call(arguments)
	if len(reply) == 0 {
		return codec.CreateResponse(req.id, nil)
	}

	if req.callb.errPos >= 0 { // test if method returned an error
		if !reply[req.callb.errPos].IsNil() {
			e := reply[req.callb.errPos].Interface().(error)
			res := codec.CreateErrorResponse(&req.id, &callbackError{e.Error()})
			return res
		}
	}
	return codec.CreateResponse(req.id, reply[0].Interface())
}

// handle executes a request and returns the response from the callback.
func (s *Server) handle(ctx context.Context, codec ServerCodec, req *serverRequest) (interface{}, func()) {
	if req.err != nil {
		return codec.CreateErrorResponse(&req.id, req.err), nil
	}

	if req.isUnsubscribe { // cancel subscription, first param must be the subscription id
		return s.handleUnsubscribe(ctx, codec, req), nil
	}

	if req.callb.isSubscribe {
		return s.handleSubscribe(ctx, codec, req)
	}

	return s.handleRegularRequest(ctx, codec, req), nil
}

// exec executes the given requests and writes the result back using the codec.
// It will only write the response back when the last request is processed.
func (s *Server) exec(ctx context.Context, codec ServerCodec, requests []*serverRequest, batch bool) {
	responses := make([]interface{}, len(requests))
	var callbacks []func()
	for i, req := range requests {
		if req.err != nil {
			responses[i] = codec.CreateErrorResponse(&req.id, req.err)
		} else {
			var callback func()
			if responses[i], callback = s.handle(ctx, codec, req); callback != nil {
				callbacks = append(callbacks, callback)
			}
		}
	}
	var err error
	if !batch {
		err = codec.Write(responses[0])
	} else {
		err = codec.Write(responses)
	}

	if err != nil {
		log.Error(fmt.Sprintf("%v\n", err))
		codec.Close()
	}

	// when request holds one of more subscribe requests this allows these subscriptions to be activated
	for _, c := range callbacks {
		c()
	}
}

// readRequest requests the next (batch) request from the codec. It will return the collection
// of requests, an indication if the request was a batch, the invalid request identifier and an
// error when the request could not be read/parsed.
func (s *Server) readRequest(codec ServerCodec) ([]*serverRequest, bool, Error) {
	reqs, batch, err := codec.ReadRequestHeaders()
	if err != nil {
		return nil, batch, err
	}

	requests := make([]*serverRequest, len(reqs))

	// verify requests
	for i, r := range reqs {
		var ok bool
		var svc *service

		if r.err != nil {
			requests[i] = &serverRequest{id: r.id, err: r.err}
			continue
		}

		if r.isPubSub && strings.HasSuffix(r.method, unsubscribeMethodSuffix) {
			requests[i] = s.parseUnsubscribeRequest(codec, &r)
			continue
		}

		if svc, ok = s.services[r.service]; !ok { // rpc method(service) isn't available
			requests[i] = &serverRequest{id: r.id, err: &methodNotFoundError{r.service, r.method}}
			continue
		}

		if r.isPubSub { // eth_subscribe, r.method contains the subscription method name
			requests[i] = s.parseSubscribeRequest(codec, &r, svc)
			continue
		}

		requests[i] = s.parseRegularRequest(codec, &r, svc)
	}

	return requests, batch, nil
}

func (s *Server) parseUnsubscribeRequest(codec ServerCodec, r *rpcRequest) *serverRequest {
	request := &serverRequest{id: r.id, isUnsubscribe: true}
	argTypes := []reflect.Type{reflect.TypeOf("")} // expect subscription id as first arg
	if args, err := codec.ParseRequestArguments(argTypes, r.params); err == nil {
		request.args = args
	} else {
		request.err = &invalidParamsError{err.Error()}
	}
	return request
}

func (s *Server) parseSubscribeRequest(codec ServerCodec, r *rpcRequest, svc *service) *serverRequest {
	var request *serverRequest
	if callb, ok := svc.subscriptions[r.method]; ok {
		request = &serverRequest{id: r.id, svcname: svc.name, callb: callb}
		if r.params != nil && len(callb.argTypes) > 0 {
			argTypes := []reflect.Type{reflect.TypeOf("")}
			argTypes = append(argTypes, callb.argTypes...)
			if args, err := codec.ParseRequestArguments(argTypes, r.params); err == nil {
				request.args = args[1:] // first one is service.method name which isn't an actual argument
			} else {
				request.err = &invalidParamsError{err.Error()}
			}
		}
	} else {
		request = &serverRequest{id: r.id, err: &methodNotFoundError{r.service, r.method}}
	}
	return request
}

func (s *Server) parseRegularRequest(codec ServerCodec, r *rpcRequest, svc *service) *serverRequest {
	var request *serverRequest
	if callb, ok := svc.callbacks[r.method]; ok { // lookup RPC method
		request = &serverRequest{id: r.id, svcname: svc.name, callb: callb}
		if r.params != nil && len(callb.argTypes) > 0 {
			if args, err := codec.ParseRequestArguments(callb.argTypes, r.params); err == nil {
				request.args = args
			} else {
				request.err = &invalidParamsError{err.Error()}
			}
		}
	} else {
		request = &serverRequest{id: r.id, err: &methodNotFoundError{r.service, r.method}}
	}
	return request
}

/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts ...OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...OptionDelete) error
	//TODO
	//TopicList(ctx context.Context, mq *primitive.MessageQueue) (*remote.RemotingCommand, error)
	GetBrokerClusterInfo(ctx context.Context, opts ...OptionInfo) (*internal.ClusterInfo, error)
	Close() error
}

// TODO: move outdated context to ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

type admin struct {
	cli     internal.RMQClient
	namesrv internal.Namesrvs

	opts *adminOptions

	closeOnce sync.Once
}

func (a *admin) GetBrokerClusterInfo(ctx context.Context, opts ...OptionInfo) (*internal.ClusterInfo, error) {
	cfg := defaultClusterInfo()
	for _, apply := range opts {
		apply(&cfg)
	}
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	res, err := a.cli.InvokeSync(ctx, cfg.NameSrvAddr[0], cmd, cfg.TimeoutMillis)

	if err != nil {
		rlog.Error("connect to namesrv failed.", map[string]interface{}{
			"namesrv": a,
			"config":  cfg,
		})
		return nil, primitive.NewRemotingErr(err.Error())
	}

	v := &internal.ClusterInfo{}

	switch res.Code {
	case internal.ResSuccess:
		if res.Body == nil {
			return nil, primitive.NewMQClientErr(res.Code, res.Remark)
		}
		/**
		todo 返回结果非json格式
		 */
		reg := regexp.MustCompile(`{(\d)`)
		bodyStr := reg.ReplaceAllString(string(res.Body), `{"${1}"`)
		json.Unmarshal([]byte(bodyStr), v)
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, primitive.NewMQClientErr(res.Code, res.Remark)
	}

	return v, err
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (Admin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver)
	if err != nil {
		return nil, err
	}
	defaultOpts.ClientOptions.Namesrv = namesrv

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)

	//log.Printf("Client: %#v", namesrv.srvs)
	return &admin{
		cli:     cli,
		namesrv: namesrv,
		opts:    defaultOpts,
	}, nil
}

// CreateTopic create topic.
// TODO: another implementation like sarama, without brokerAddr as input
func (a *admin) CreateTopic(ctx context.Context, opts ...OptionCreate) error {
	cfg := defaultTopicConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}

	request := &internal.CreateTopicRequestHeader{
		Topic:           cfg.Topic,
		DefaultTopic:    cfg.DefaultTopic,
		ReadQueueNums:   cfg.ReadQueueNums,
		WriteQueueNums:  cfg.WriteQueueNums,
		Perm:            cfg.Perm,
		TopicFilterType: cfg.TopicFilterType,
		TopicSysFlag:    cfg.TopicSysFlag,
		Order:           cfg.Order,
	}

	if cfg.BrokerAddr == "" {
		clusterInfo, _ := a.GetBrokerClusterInfo(ctx, WithNameSrvAddrClusterInfo(cfg.NameSrvAddr))
		fmt.Println(clusterInfo)
		var address string
		for _,v := range clusterInfo.BrokerAddrTable {
			data, _ := interface{} (v).(internal.BrokerData)
			for _,brokerAddr := range data.BrokerAddresses {
				address = brokerAddr
				break
			}
			fmt.Println("break inner")
			break
			fmt.Println("break outer")
		}
		cfg.BrokerAddr = address
	}
	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	_, err := a.cli.InvokeSync(ctx, cfg.BrokerAddr, cmd, 5*time.Second)
	if err != nil {
		rlog.Error("create topic error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
	} else {
		rlog.Info("create topic success", map[string]interface{}{
			rlog.LogKeyTopic:  cfg.Topic,
			rlog.LogKeyBroker: cfg.BrokerAddr,
		})
	}
	return err
}

// DeleteTopicInBroker delete topic in broker.
func (a *admin) deleteTopicInBroker(ctx context.Context, topic string, brokerAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInBroker, request, nil)
	return a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
}

// DeleteTopicInNameServer delete topic in nameserver.
func (a *admin) deleteTopicInNameServer(ctx context.Context, topic string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInNameSrv, request, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

// DeleteTopic delete topic in both broker and nameserver.
func (a *admin) DeleteTopic(ctx context.Context, opts ...OptionDelete) error {
	cfg := defaultTopicConfigDelete()
	for _, apply := range opts {
		apply(&cfg)
	}
	//delete topic in broker
	if cfg.BrokerAddr == "" {
		a.namesrv.UpdateTopicRouteInfo(cfg.Topic)
		cfg.BrokerAddr = a.namesrv.FindBrokerAddrByTopic(cfg.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, cfg.Topic, cfg.BrokerAddr); err != nil {
		if err != nil {
			rlog.Error("delete topic in broker error", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyBroker:        cfg.BrokerAddr,
				rlog.LogKeyUnderlayError: err,
			})
		}
		return err
	}

	//delete topic in nameserver
	if len(cfg.NameSrvAddr) == 0 {
		a.namesrv.UpdateTopicRouteInfo(cfg.Topic)
		cfg.NameSrvAddr = a.namesrv.AddrList()
	}

	for _, nameSrvAddr := range cfg.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, cfg.Topic, nameSrvAddr); err != nil {
			if err != nil {
				rlog.Error("delete topic in name server error", map[string]interface{}{
					rlog.LogKeyTopic:         cfg.Topic,
					"nameServer":             nameSrvAddr,
					rlog.LogKeyUnderlayError: err,
				})
			}
			return err
		}
	}
	rlog.Info("delete topic success", map[string]interface{}{
		rlog.LogKeyTopic:  cfg.Topic,
		rlog.LogKeyBroker: cfg.BrokerAddr,
		"nameServer":      cfg.NameSrvAddr,
	})
	return nil
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}

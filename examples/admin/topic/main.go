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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"regexp"
)

func main() {
	topic := "newOne"
	//clusterName := "DefaultCluster"
	nameSrvAddr := []string{"127.0.0.1:9876"}
	brokerAddr := "127.0.0.1:10911"
	fmt.Println(brokerAddr)


	testAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	if err != nil {
		fmt.Println(err.Error())
	}

	//var json  = jsoniter.ConfigCompatibleWithStandardLibrary

	res, err := testAdmin.GetBrokerClusterInfo(context.Background(), admin.WithNameSrvAddrClusterInfo(nameSrvAddr))
	fmt.Println(res)
	var v internal.ClusterInfo
	err = json.Unmarshal([]byte(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{"0":"192.168.137.232:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"},"broker-b":{"brokerAddrs":{"1":"192.168.137.232:10912"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`), &v)
	//err = json.Unmarshal(res.Body, &v)
	//fmt.Println(res.Code,string(res.Body))
	reg := regexp.MustCompile(`((\d{1,3}.){3}\d{1,3}:\d+)`)
	//submatchRes := reg.FindString(string(res.Body))

	submatch := reg.FindStringSubmatch(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{
0:"192.168.137.232:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"},"broker-b":{"brokerAddrs":{1:"192.168.137.232:10912"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`)
	reg1:= regexp.MustCompile(`{(\d)`)
	allString := reg1.ReplaceAllString(`{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"192.168.137.232:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"},"broker-b":{"brokerAddrs":{1:"192.168.137.232:10912"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}`, `{"${1}"`)
	fmt.Println(allString)
	//fmt.Println(submatchRes)
	fmt.Println(submatch)
	//create topic
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topic),
		admin.WithBrokerAddrCreate(brokerAddr),
		admin.WithNameSrvAddrCreate(nameSrvAddr),
		admin.WithClusterNameCreate("DefaultCluster"),
	)
	if err != nil {
		fmt.Println("Create topic error:", err.Error())
	}

	//deletetopic
	err = testAdmin.DeleteTopic(
		context.Background(),
		admin.WithTopicDelete(topic),
		//admin.WithBrokerAddrDelete(brokerAddr),
		//admin.WithNameSrvAddr(nameSrvAddr),
	)
	if err != nil {
		fmt.Println("Delete topic error:", err.Error())
	}

	err = testAdmin.Close()
	if err != nil {
		fmt.Printf("Shutdown admin error: %s", err.Error())
	}
}

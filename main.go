package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

func main() {
	// 配置RocketMQ NameServer地址
	nameServers := []string{"127.0.0.1:9876"}
	rlog.SetOutputPath(
		"./logs/consumer.log",
	)

	// 创建推送消费者
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(nameServers),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("test_consumer_group"),
	)
	defer c.Shutdown()
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}

	// 订阅主题和标签
	err = c.Subscribe("test_topic", consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: "*", // 订阅所有标签
	}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		// 处理消息
		for _, msg := range msgs {
			fmt.Printf("接收到消息 - ID: %s, Topic: %s, Tag: %s, Key: %s, Body: %s, 队列: %d, 偏移量: %d, 重试次数: %d\n",
				msg.MsgId,
				msg.Topic,
				msg.GetTags(),
				msg.GetKeys(),
				string(msg.Body),
				msg.Queue.QueueId,
				msg.QueueOffset,
				msg.ReconsumeTimes,
			)

			// 模拟处理时间
			time.Sleep(100 * time.Millisecond)
		}

		// 返回消费成功
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		log.Fatalf("订阅主题失败: %v", err)
	}

	// 启动消费者
	err = c.Start()
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}

	fmt.Println("消费者启动成功，开始消费消息...")
	fmt.Println("按 Ctrl+C 退出程序")

	// 创建信号通道来监听中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号
	<-sigChan

	fmt.Println("接收到退出信号...")

	// 不优雅关闭 - 直接退出程序，不调用 c.Shutdown()
	fmt.Println("程序即将退出，不进行优雅关闭...")
	os.Exit(0)
}


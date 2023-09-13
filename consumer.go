package rocketmq

import (
	"context"
	"dario.cat/mergo"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/go-tron/base-error"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"reflect"
	"strings"
)

type DelayLevel int

const (
	DelayLevel1s DelayLevel = iota + 1
	DelayLevel5s
	DelayLevel10s
	DelayLevel30s
	DelayLevel1m
	DelayLevel2m
	DelayLevel3m
	DelayLevel4m
	DelayLevel5m
	DelayLevel6m
	DelayLevel7m
	DelayLevel8m
	DelayLevel9m
	DelayLevel10m
	DelayLevel20m
	DelayLevel30m
	DelayLevel1h
	DelayLevel2h
)

// The DelayLevel specify the waiting time that before next reconsume, [1s, 5s, 10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h]
type Retry struct {
	MaxReconsumeTimes  int32
	RetryDelayLevel    DelayLevel
	MaxRetryDelayLevel DelayLevel
}

func NewRetry(retryDelayLevel DelayLevel, maxRetryDelayLevel DelayLevel, maxReconsumeTimes int32) *Retry {
	if retryDelayLevel > maxRetryDelayLevel {
		panic("retryDelayLevel must less than maxRetryDelayLevel")
	}
	return &Retry{
		MaxReconsumeTimes:  maxReconsumeTimes,
		RetryDelayLevel:    retryDelayLevel,
		MaxRetryDelayLevel: maxRetryDelayLevel,
	}
}
func defaultRetry() *Retry {
	return NewRetry(DelayLevel5s, DelayLevel2h, 30)
}

type RetryStrategy interface {
	Calculate(times int32) int
}

type defaultRetryStrategy struct {
	*ConsumerConfig
}

func (s *defaultRetryStrategy) Calculate(times int32) int {
	level := int(times) + int(s.Retry.RetryDelayLevel)
	if level > int(s.Retry.MaxRetryDelayLevel) {
		level = int(s.Retry.MaxRetryDelayLevel)
	}
	return level
}

type ConsumerConfig struct {
	NameServer    string
	ConsumerGroup string
	Topic         string
	Tag           string
	Key           string
	Model         string
	Orderly       bool
	Retry         *Retry
	RetryStrategy
	RLogger   logger.Logger
	MsgLogger logger.Logger
}

func NewConsumerWithConfig(c *config.Config, consumer Consumer) {
	NewConsumer(&ConsumerConfig{
		NameServer:    c.GetString("rocketmq.nameServer"),
		ConsumerGroup: c.GetString("application.name"),
		RLogger:       logger.NewZapWithConfig(c, "rocketmq-consumer", "error"),
		MsgLogger:     logger.NewZapWithConfig(c, "mq-consumer", "info"),
	}, consumer)
}

type Consumer interface {
	Config() *ConsumerConfig
	Handler(msg string) error
}

func NewConsumer(config *ConsumerConfig, c Consumer) {
	if err := mergo.Merge(config, c.Config()); err != nil {
		panic(err)
	}
	if config == nil {
		panic("config 必须设置")
	}
	if config.NameServer == "" {
		panic("NameServer 必须设置")
	}
	if config.ConsumerGroup == "" {
		panic("ConsumerGroup 必须设置")
	}
	if config.RLogger == nil {
		panic("RLogger 必须设置")
	}
	if config.MsgLogger == nil {
		panic("MsgLogger 必须设置")
	}

	rlog.SetLogger(&Logger{config.RLogger})

	if config.Model == "" {
		config.Model = "clustering"
	}
	var model consumer.MessageModel
	switch strings.ToLower(config.Model) {
	case "broadcasting":
		model = consumer.BroadCasting
	case "clustering":
		model = consumer.Clustering
	default:
		panic("unknown MessageModel")
	}

	if config.Retry == nil {
		config.Retry = defaultRetry()
	}

	if config.RetryStrategy == nil {
		config.RetryStrategy = &defaultRetryStrategy{
			config,
		}
	}

	var opts = []consumer.Option{
		consumer.WithNameServer([]string{config.NameServer}),
		consumer.WithGroupName(config.ConsumerGroup),
		consumer.WithConsumerModel(model),
		consumer.WithMaxReconsumeTimes(config.Retry.MaxReconsumeTimes),
	}

	if config.Orderly {
		opts = append(opts,
			consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
			consumer.WithConsumerOrder(true))
	}

	pc, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		panic(err)
	}

	selector := consumer.MessageSelector{}
	if config.Tag != "" {
		selector.Type = consumer.TAG
		selector.Expression = config.Tag
	}
	if err := pc.Subscribe(config.Topic, selector, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {

		var (
			errs              []error
			minReconsumeTimes = msgs[0].ReconsumeTimes
		)
		for _, msg := range msgs {
			if msg.ReconsumeTimes < minReconsumeTimes {
				minReconsumeTimes = msg.ReconsumeTimes
			}

			body := string(msg.Body)
			err := c.Handler(body)

			if err != nil && !(reflect.TypeOf(err).String() == "*baseError.Error" && !err.(*baseError.Error).System) {
				errs = append(errs, err)
			}

			config.MsgLogger.Info(body,
				config.MsgLogger.Field("topic", msg.Topic),
				config.MsgLogger.Field("tag", msg.GetTags()),
				config.MsgLogger.Field("keys", msg.GetKeys()),
				config.MsgLogger.Field("attempts", msg.ReconsumeTimes),
				config.MsgLogger.Field("error", err),
			)
		}

		concurrentCtx, _ := primitive.GetConcurrentlyCtx(ctx)
		concurrentCtx.DelayLevelWhenNextConsume = config.RetryStrategy.Calculate(minReconsumeTimes)

		if len(errs) == 0 {
			return consumer.ConsumeSuccess, nil
		} else {
			return consumer.ConsumeRetryLater, nil
		}
	}); err != nil {
		panic(err)
	}

	if err := pc.Start(); err != nil {
		panic(err)
	}
}

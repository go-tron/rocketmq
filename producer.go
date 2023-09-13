package rocketmq

import (
	"context"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"reflect"
)

type ProducerConfig struct {
	NameServer    string `json:"nameServer"`
	ProducerGroup string `json:"producerGroup"`
	Retry         int
	RLogger       logger.Logger
	MsgLogger     logger.Logger
}

func NewProducerWithConfig(c *config.Config) *Producer {
	return NewProducer(&ProducerConfig{
		NameServer:    c.GetString("rocketmq.nameServer"),
		ProducerGroup: c.GetString("application.name"),
		RLogger:       logger.NewZapWithConfig(c, "rocketmq-producer", "error"),
		MsgLogger:     logger.NewZapWithConfig(c, "mq-producer", "info"),
	})
}

func NewProducer(config *ProducerConfig) *Producer {
	if config == nil {
		panic("config 必须设置")
	}
	if config.NameServer == "" {
		panic("NameServer 必须设置")
	}
	if config.ProducerGroup == "" {
		panic("ProducerGroup 必须设置")
	}
	if config.RLogger == nil {
		panic("RLogger 必须设置")
	}
	if config.MsgLogger == nil {
		panic("MsgLogger 必须设置")
	}

	rlog.SetLogger(&Logger{config.RLogger})

	var opts = []producer.Option{
		producer.WithNameServer([]string{config.NameServer}),
		producer.WithGroupName(config.ProducerGroup),
	}

	if config.Retry != 0 {
		producer.WithRetry(config.Retry)
	}

	p, _ := rocketmq.NewProducer(opts...)
	err := p.Start()
	if err != nil {
		panic(err)
	}

	return &Producer{
		p, config,
	}
}

type Producer struct {
	rocketmq.Producer
	*ProducerConfig
}

func (p *Producer) Logger() logger.Logger {
	return p.ProducerConfig.MsgLogger
}
func (p *Producer) MessageFormat(data interface{}) ([]byte, error) {
	var message []byte
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type() == reflect.TypeOf(message) {
		message = data.([]byte)
	} else {
		if reflect.TypeOf(data).Kind() == reflect.String {
			message = []byte(data.(string))
		} else {
			var err error
			message, err = json.Marshal(data)
			if err != nil {
				return nil, err
			}
		}
	}
	return message, nil
}

type SendOption func(*MessageOption)

type MessageOption struct {
	Tag            string
	Keys           []string
	DelayTimeLevel DelayLevel
}

func WithTag(tag string) SendOption {
	return func(msg *MessageOption) {
		msg.Tag = tag
	}
}
func WithKeys(keys ...string) SendOption {
	return func(msg *MessageOption) {
		msg.Keys = append(msg.Keys, keys...)
	}
}
func WithDelayTimeLevel(delayTimeLevel DelayLevel) SendOption {
	return func(msg *MessageOption) {
		msg.DelayTimeLevel = delayTimeLevel
	}
}

func (p *Producer) SendSync(topic string, data interface{}, opts ...SendOption) error {

	messageOption := &MessageOption{}
	if len(opts) > 0 {
		for _, apply := range opts {
			if apply != nil {
				apply(messageOption)
			}
		}
	}

	message, err := p.MessageFormat(data)
	defer func() {
		p.Logger().Info(string(message),
			p.Logger().Field("topic", topic),
			p.Logger().Field("tag", messageOption.Tag),
			p.Logger().Field("keys", messageOption.Keys),
			p.Logger().Field("error", err),
		)
	}()

	if err != nil {
		return err
	}

	msg := primitive.NewMessage(topic, message)

	if messageOption.DelayTimeLevel != 0 {
		msg.WithDelayTimeLevel(int(messageOption.DelayTimeLevel))
	}

	if messageOption.Tag != "" {
		msg.WithTag(messageOption.Tag)
	}

	if messageOption.Keys != nil {
		msg.WithKeys(messageOption.Keys)
	}
	_, err = p.Producer.SendSync(context.Background(), msg)
	return err
}

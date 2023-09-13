package rocketmq

import (
	"github.com/go-tron/logger"
)

type Logger struct {
	logger logger.Logger
}

func (l *Logger) Level(level string) {
}

func (l *Logger) Debug(msg string, fields map[string]interface{}) {
	l.logger.Debug(msg, logger.MapToFields(fields)...)
}

func (l *Logger) Info(msg string, fields map[string]interface{}) {
	l.logger.Info(msg, logger.MapToFields(fields)...)
}

func (l *Logger) Warning(msg string, fields map[string]interface{}) {
	l.logger.Warn(msg, logger.MapToFields(fields)...)
}

func (l *Logger) Error(msg string, fields map[string]interface{}) {
	l.logger.Error(msg, logger.MapToFields(fields)...)
}
func (l *Logger) Fatal(msg string, fields map[string]interface{}) {
	l.logger.Fatal(msg, logger.MapToFields(fields)...)
}

func (l *Logger) OutputPath(path string) (err error) {
	return nil
}

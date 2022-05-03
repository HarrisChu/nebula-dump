package common

import (
	"bytes"
	"io"

	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger

func SetUpLogs(out io.Writer, verbose bool) {
	Logger = logrus.New()
	Logger.SetOutput(out)
	Logger.SetFormatter(&CustomFormatter{})
	if verbose {
		Logger.SetLevel(logrus.DebugLevel)
	} else {
		Logger.SetLevel(logrus.InfoLevel)
	}
}

type CustomFormatter struct {
}

func (f *CustomFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	b.Write([]byte(entry.Message))
	b.WriteByte('\n')
	return b.Bytes(), nil
}

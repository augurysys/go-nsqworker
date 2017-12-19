package nsqworker

import (
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

type logWrapper struct {
	log *logrus.Logger
	*logrus.Entry
}

func newLogWrapper(topic, channel string) *logWrapper {
	log := logrus.New()
	log.Out = ioutil.Discard

	formatter := new(logrus.TextFormatter)
	formatter.DisableColors = true

	log.Formatter = formatter


	entry := log.WithFields(logrus.Fields{
		"topic":	topic,
		"channel":	channel,
	})

	return &logWrapper{log, entry}
}

// Capture nsq.Consumer logging messages
func (lw *logWrapper) Output(calldepth int, s string) error {
	lw.Debug(s)
	return nil
}

func (nw *NsqWorker) ToggleLogging(enable bool) {
	if enable {
		nw.internalLogger.log.Out = os.Stderr
	} else {
		nw.internalLogger.log.Out = ioutil.Discard
	}
}

func (nw *NsqWorker) ToggleDebug(enable bool) {
	if enable {
		nw.internalLogger.log.Level = logrus.DebugLevel
	} else {
		nw.internalLogger.log.Level = logrus.InfoLevel
	}
}

func (nw *NsqWorker) UseExternalLogger(extLogger logrus.FieldLogger) {
	nw.log = extLogger
}

func (nw *NsqWorker) UseInternalLogger() {
	nw.log = nw.internalLogger
}

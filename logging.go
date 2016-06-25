package nsqworker

import (
	"github.com/Sirupsen/logrus"
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
		nw.log.log.Out = os.Stderr
	} else {
		nw.log.log.Out = ioutil.Discard
	}
}

func (nw *NsqWorker) ToggleDebug(enable bool) {
	if enable {
		nw.log.log.Level = logrus.DebugLevel
	} else {
		nw.log.log.Level = logrus.InfoLevel
	}
}
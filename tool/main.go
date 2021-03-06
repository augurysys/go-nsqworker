package main

import (
	"github.com/augurysys/go-nsqworker"
	"fmt"
	"github.com/vrecan/death"
	"syscall"
	"github.com/augurysys/go-nsqworker/routers/json"
)


func grrr(m *json.Message) error {
	ar := make([]string,0)
	ar[1] = "dfsd"
	return nil

}

func main() {
	nsqw, _ := nsqworker.New("testi", "testa", []string{"http://172.17.0.1:4161"})
	nsqw.ToggleLogging(true)
	nsqw.ToggleDebug(true)

	route := json.Route{}

	route.M = []json.Matcher{
		json.FieldMatch{"name","azk"},
		json.FieldMatch{"name","michael"},
	}


	route.H = grrr


	jsnr := json.NewRouter([]json.Route{route})
	nsqw.RegisterRouter(jsnr)

	d := death.NewDeath(syscall.SIGINT, syscall.SIGTERM)

	if err := nsqw.Start(); err != nil {
		fmt.Println(err)
	}

	d.WaitForDeath(nsqw)
}

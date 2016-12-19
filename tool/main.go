package main

import (
	"github.com/augurysys/go-nsqworker"
	"fmt"
	"github.com/vrecan/death"
	"syscall"
	"github.com/augurysys/go-nsqworker/routers/json"
	"errors"
)


func grrr(m *json.Message) error {
	m.Log.Info(string(m.Body))

	m.Log.Info(m.JsonBody.GetBool("bool:bool1"))
	m.Log.Info(m.JsonBody.GetString("string:string1"))
	m.Log.Info(m.JsonBody.GetFloat("float"))
	m.Log.Info(m.JsonBody.GetArray("array"))
	m.Log.Info(m.JsonBody.GetObject("object"))
	m.Log.Info("fdsfdsfsfdsffsffd")
	return errors.New("shitty")
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


	jsnr := json.NewRouter([]json.Route{route}, json.NewRedisPersistor())
	nsqw.RegisterRouter(jsnr)

	d := death.NewDeath(syscall.SIGINT, syscall.SIGTERM)

	if err := nsqw.Start(); err != nil {
		fmt.Println(err)
	}

	d.WaitForDeath(nsqw)
}

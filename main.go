package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"

	"time"

	"github.com/crgimenes/go-osc"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

func main() {
	config := readConfig("data/config.yaml")

	localUdpAddr, _ := net.ResolveUDPAddr("udp", "0.0.0.0:" + strconv.Itoa(config.OscListenPort))
	deskUdpAddr, _ := net.ResolveUDPAddr("udp", config.Console.Host + ":" + strconv.Itoa(config.Console.OscRecivePort))

	d1 := osc.NewStandardDispatcher()
	app1 := osc.NewServerAndClient(d1)
	app1.NewConn(localUdpAddr, deskUdpAddr)

	// var wsConns = make(map[string]chan string)
	wsConns := make(map[string]chan EncodedWsMessage)
	wsConnsWrite := make(chan struct{uuid string; ch chan EncodedWsMessage}, 10)
	go func () {
		for {
			write := <- wsConnsWrite
			wsConns[write.uuid] = write.ch
		}
	}()

	oscConnected := false
	d1.AddMsgHandler("*", func(msg *osc.Message) {
		storeDataLock.Lock()
		storeData[msg.Address] = msg.Arguments
		storeDataLock.Unlock()
		oscConnected = true
		
		b, err := json.Marshal(WsMessage{msg.Address, msg.Arguments})
		if err != nil {
			println("error encoding to json: " + err.Error())
			return
		}
		println("from osc: " + string(b))

		for _, conn := range wsConns {
			conn <- b
		}
	})

	go app1.ListenAndServe()

	// app1.SendMsg("/console/ping")

	go func ()  {
		for !oscConnected {
			if config.Console.Series == "sd" {
				for ch := 1; ch <= config.MaxChannel; ch++ {
					for aux := 1; aux <= config.MaxAux; aux++ {
						app1.SendMsg("/sd/Input_Channels/" + strconv.Itoa(ch) + "/Aux_Send/" + strconv.Itoa(aux) + "/send_level/?")
						app1.SendMsg("/sd/Input_Channels/" + strconv.Itoa(ch) + "/Aux_Send/" + strconv.Itoa(aux) + "/send_pan/?")
					}
				}
			} else if config.Console.Series == "s" {
				app1.SendMsg("/console/resend")
			}
			time.Sleep(10 * time.Second)
		}
	}()

	readChannelsData()
	httpChannelsDataRoutes()
	httpServeConfig(config)

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	handleConnections := func (w http.ResponseWriter, r *http.Request)  {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		var uuid string
		for {
			uuid = genUuid()
			if wsConns[uuid] == nil {
				break
			}
		}

		ch := make(chan EncodedWsMessage)
		wsConnsWrite <- struct{uuid string; ch chan EncodedWsMessage}{uuid, ch}
		defer func(){ wsConns[uuid] = nil }()

		go func() {
			for {
				_, message, err := ws.ReadMessage()
				if err != nil {
					println(err.Error())
					break
				}
				fmt.Printf("Received: %s\n", message)

				var wsMessage WsMessage
				err = json.Unmarshal(message, &wsMessage)
				if err != nil {
					println(err.Error())
					continue
				}

				if (wsMessage.Address == "init") {
					for addr, args := range storeData {
						sendWsMessage := WsMessage{addr, args}
						ch <- encodeWs(sendWsMessage)
					}
				} else {
					storeDataLock.Lock()
					storeData[wsMessage.Address] = wsMessage.Args
					storeDataLock.Unlock()
					app1.SendMsg(wsMessage.Address, float32(wsMessage.Args[0].(float64)))
					go func() {
						for uuid2, ws2 := range wsConns {
							if uuid2 != uuid {
								ws2 <- message
							}
						}
					}()
				}
			}
		}()

		go func() {
			for {
				encodedWsMessage := <- ch
				if (encodedWsMessage != nil) {
					ws.WriteMessage(1, encodedWsMessage)
				}
			}
		}()

		for {
			time.Sleep(time.Minute)
		}
	}

	http.HandleFunc("/", handleConnections)
	httpHost := "0.0.0.0:" + strconv.Itoa(config.HttpServerPort)
    log.Println("http server started on " + httpHost)
    err := http.ListenAndServe(httpHost, nil)
    if err != nil {
        log.Fatal("ListenAndServe: ", err)
    }

	for {
		time.Sleep(time.Second)
	}
}

func genUuid() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

type WsMessage struct {
	Address string `json:"address"`
	Args    []any  `json:"args"`
}

type EncodedWsMessage []byte

func encodeWs(msg WsMessage) EncodedWsMessage {
	b, err := json.Marshal(WsMessage{msg.Address, msg.Args})
	if err != nil {
		println("error encoding to json: " + err.Error())
		return nil
	}
	return b
}

var storeData = make(map[string][]any)
var storeDataLock = sync.Mutex{}

type Config struct {
    Console        ConsoleConfig `json:"console"`
    HttpServerPort int           `json:"httpServerPort"`
    OscListenPort  int           `json:"oscListenPort"`
	MaxChannel     int           `json:"maxChannel"`
	MaxAux         int           `json:"maxAux"`
	BackendHost    string        `json:"backendHost"`
}

type ConsoleConfig struct {
	Series        string `json:"series"`
	Host          string `json:"host"`
	OscRecivePort int    `json:"oscReceivePort"`
}

func readConfig(filepath string) Config {
    yamlFile, err := os.ReadFile(filepath)
    if err != nil {
        println("Error reading config: " + err.Error())
		os.Exit(1)
    }
    var data map[string]any
    err = yaml.Unmarshal(yamlFile, &data)
    if err != nil {
        println("Error parsing config: " + err.Error())
		os.Exit(1)
    }

	console, ok := data["console"]
    if !ok {
		println("no \"console\" key in config")
		os.Exit(1)
	}
	consoleMap := console.(map[string]any)
	series := "s"
	if s, ok := consoleMap["series"]; ok {
		series = s.(string)
	}
	consoleHost, ok := consoleMap["host"]
	if !ok {
		println("no \"host\" key in config in console/")
		os.Exit(1)
	}
	consoleOscReceivePort := 8001
    if p, ok := consoleMap["oscReceivePort"]; ok {
		consoleOscReceivePort = p.(int)
	}

    httpServerPort := 8002
	if p, ok := data["httpServerPort"]; ok {
		httpServerPort = p.(int)
	}
	oscListenPort := 9100
	if p, ok := data["oscListenPort"]; ok {
		oscListenPort = p.(int)
	}
	maxChannel := 64
	if p, ok := data["maxChannel"]; ok {
		maxChannel = p.(int)
	}
	maxAux := 32
	if p, ok := data["maxAux"]; ok {
		maxAux = p.(int)
	}
	backendHost := "127.0.0.1"
	if p, ok := data["backendHost"]; ok {
		backendHost = p.(string)
	}

    return Config{ConsoleConfig{series, consoleHost.(string), consoleOscReceivePort}, 
		httpServerPort, oscListenPort, maxChannel, maxAux, backendHost}
}

func httpServeConfig(config Config) {
	http.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		// fmt.Printf("%v\n", *r)
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
		
		b, err := json.Marshal(config)
		if err != nil {
			println(err.Error())
			w.WriteHeader(500)
			return
		}
		w.Write(b)
	})
}


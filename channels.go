package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
)

type ChannelGroup struct {
	Name     string    `json:"name"`
	Order    int       `json:"order"`
	Hidden   bool      `json:"hidden"`
	Channels []Channel `json:"channels"`
}

type Channel struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
	Order  int    `json:"order"`
	Hidden bool   `json:"hidden"`
	Color  string `json:"color"`
}

type Aux struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
	Order  int    `json:"order"`
	Hidden bool   `json:"hidden"`
	Stereo bool   `json:"stereo"`
	Color  string `json:"color"`
}

type ChannelsData struct {
	Channels []ChannelGroup `json:"channels"`
	Auxes    []Aux          `json:"auxes"`
}

var channelsDataFilePath = "data/channels.json"
var channelsData = ChannelsData{[]ChannelGroup{}, []Aux{}}

var writeChannelsFile = make(chan bool)

func readChannelsData() {
	b, err := os.ReadFile(channelsDataFilePath)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b, &channelsData)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			<-writeChannelsFile
			b, err := json.Marshal(channelsData)
			if err != nil {
				println("Error encoding to json: " + err.Error())
				continue
			}
			fo, err := os.Create(channelsDataFilePath)
			if err != nil {
				println("Error opening file: " + err.Error())
				continue
			}

			_, err = fo.Write(b)
			if err != nil {
				println("Error writing to file: " + err.Error())
			}
		}
	}()
}

func httpChannelsDataRoutes() {
	http.HandleFunc("/channels", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "GET" {
			channels := struct {
				Channels []ChannelGroup `json:"channels"`
			}{channelsData.Channels}
			b, err := json.Marshal(channels)
			if err != nil {
				println("Error encoding to json: " + err.Error())
				http.Error(w, "Error encoding to json", 500)
				return
			}

			w.Header().Add("Content-Type", "application/json")
			w.Write(b)
			return
		}
		if r.Method == "POST" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Error reading body", 400)
				println(err.Error())
				return
			}
			var channels struct {
				Channels []ChannelGroup `json:"channels"`
			}
			err = json.Unmarshal(body, &channels)
			if err != nil {
				http.Error(w, "Error parsing body", 400)
				println(err.Error())
				return
			}
			channelsData.Channels = channels.Channels
			writeChannelsFile <- true
			wsMessage := encodeWs(WsMessage{"channels", []any{channels}})
			go func() {
				for _, ws := range wsConns {
					ws <- wsMessage
				}
			}()
			w.WriteHeader(200)
			return
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	http.HandleFunc("/auxes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "GET" {
			auxes := struct {
				Auxes []Aux `json:"auxes"`
			}{channelsData.Auxes}
			b, err := json.Marshal(auxes)
			if err != nil {
				println("Error encoding to json: " + err.Error())
				http.Error(w, "Error encoding to json", 500)
				return
			}

			w.Header().Add("Content-Type", "application/json")
			w.Write(b)
			return
		}
		if r.Method == "POST" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Error reading body", 400)
				println(err.Error())
				return
			}
			var auxes struct {
				Auxes []Aux `json:"auxes"`
			}
			err = json.Unmarshal(body, &auxes)
			if err != nil {
				http.Error(w, "Error parsing body", 400)
				println(err.Error())
				return
			}
			channelsData.Auxes = auxes.Auxes
			writeChannelsFile <- true
			w.WriteHeader(200)
			return
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

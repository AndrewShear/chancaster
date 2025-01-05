package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/AndrewShear/chancaster"
)

type hloc struct {
	open     float64
	high     float64
	low      float64
	close    float64
	volume   int
	datetime time.Time
}

func main() {
	ctx := context.Background()
	caster := chancaster.New[string, hloc](ctx)
	defer caster.Wait()

	mux := http.NewServeMux()
	mux.HandleFunc("/hlocs/start/{symbol}", startHLOC(caster))
	mux.HandleFunc("/hlocs/end/{symbol}", endHLOC(caster))
	mux.HandleFunc("/hlocs/publish/{symbol}", publishHLOC(caster))
	mux.HandleFunc("/hlocs/all", allHLOCs(caster))

	if err := http.ListenAndServe(":8000", mux); err != nil {
		log.Fatal(err, "listen and serve")
	}
}

func startHLOC(caster *chancaster.ChanCaster[string, hloc]) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.PathValue("symbol")
		if symbol == "" {
			http.Error(w, "symbol required", http.StatusBadRequest)
			return
		}

		startChan(symbol, caster)

		fmt.Fprintf(w, "Started HLOC for symbol %s\n", symbol)
	}
}

func endHLOC(caster *chancaster.ChanCaster[string, hloc]) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.PathValue("symbol")
		if symbol == "" {
			http.Error(w, "symbol required", http.StatusBadRequest)
			return
		}

		err := caster.Close(symbol)
		if err != nil {
			http.Error(w, "error closing channel: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "Ended HLOC for symbol %s\n", symbol)
	}
}

func publishHLOC(caster *chancaster.ChanCaster[string, hloc]) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := r.PathValue("symbol")
		if symbol == "" {
			http.Error(w, "symbol required", http.StatusBadRequest)
			return
		}

		msg := hloc{
			open:     rand.Float64(),
			high:     rand.Float64(),
			low:      rand.Float64(),
			close:    rand.Float64(),
			volume:   rand.Int() % 10000,
			datetime: time.Now(),
		}

		err := caster.Publish(symbol, msg)
		if err != nil {
			http.Error(w, "error publishing message: "+err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "Published HLOC for symbol: %s\n data: %+v", symbol, msg)
	}
}

func allHLOCs(caster *chancaster.ChanCaster[string, hloc]) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		hlocs := caster.All()

		if len(hlocs) == 0 {
			fmt.Fprintf(w, "No HLOCs found")
			return
		}

		fmt.Fprintf(w, "HLOCs found %v:\n", hlocs)
	}
}

func startChan[K comparable, T any](key K, caster *chancaster.ChanCaster[K, T]) {
	caster.Add(key, func() error {
		ch, err := caster.Get(key)
		if err != nil {
			return err
		}

		for val := range ch {
			// Your code here to process the received data
			fmt.Printf("Received %T for key %v: %v\n", val, key, val)
		}
		return nil
	})
}

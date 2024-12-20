package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Temperature struct {
	TimeStamp int64 `json:"timestamp"`
	Degree    int   `json:"degree"`
}

func main() {
	http.HandleFunc("/temperature", temperaturePOST)
	log.Println("listening on 3030")
	log.Fatal(http.ListenAndServe(":3030", nil))
}

// url: /temperature
func temperaturePOST(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	temperature := new(Temperature)

	if err := json.NewDecoder(r.Body).Decode(temperature); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	temeratureInBytes, err := json.Marshal(temperature)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	err = SendToKafka("temperature", temeratureInBytes)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"success": true,
		"msg":     fmt.Sprintf("Temperature for timecode %d is added successfully! ", temperature.TimeStamp),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, "Error posting temperature", http.StatusInternalServerError)
	}
}

func CreateKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func SendToKafka(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}

	producer, err := CreateKafkaProducer(brokers)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return nil
	}

	log.Printf("temperature is stored in topc(%s)/partition/(%d)/offset(%d)\n",
		topic, partition, offset)

	return nil
}

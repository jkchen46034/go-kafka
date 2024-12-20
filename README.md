# Go-kafka

## Producer
```
~/dev/go-kafka/server$ go run main.go
2024/12/20 09:52:26 listening on 3030
2024/12/20 09:55:28 temperature is stored in topc(temperature)/partition/(0)/offset(10)
2024/12/20 09:57:31 temperature is stored in topc(temperature)/partition/(0)/offset(11)
```

## Consumer
```~/dev/go-kafka/client$ go run main.go
Worker started
Receiver temperature, count 1: | topic (temperature), message ({"timestamp":100,"degree":-12}) 
Receiver temperature, count 2: | topic (temperature), message ({"timestamp":101,"degree":-13}) 
```

## Console
```
/dev/kafka/kafka_2.13-3.9.0$ bin/kafka-console-consumer.sh --topic temperature --from-beginning --bootstrap-server localhost:9092
{"timestamp":100,"degree":-12}
{"timestamp":101,"degree":-13}

```

## HTTP POST
```
curl -X POST http://localhost:3030/temperature \
  -H "Content-Type: application/json" \
  -d '{"timestamp": 101, "degree": -13}'
```

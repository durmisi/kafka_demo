# kafka_demo
Exploring Real-Time Data Processing: Kafka and Golang Experimentation

install Docker Desktop

navigate to: cd src\env\kafka-stack
execute: docker compose -f full-stack.yml up in Command prompt or Powershell

Open localhost:8080 in browser and make sure your kafka cluster is ready.

Producer: 
Open new  Command prompt or Powershell session
navigate to: cd src\poc\producer
execute: go run main.go


Consumer: 
Open new  Command prompt or Powershell session
navigate to: cd src\poc\consumer
execute: go run main.go



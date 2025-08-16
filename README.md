# RedePanda

A minimal terminal-based chat built with **.NET 9** and **Redpanda** (Kafka-compatible).  
Each client can act as a **producer** (send messages) or **consumer** (receive messages).  
Messages are serialized as JSON and streamed through a Kafka topic.

![RedePandaLogo](RedePanda.png)

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [.NET 9 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/9.0)
- Git clone of this repository

---

## Project Structure

```
RedePanda/
├── RedePanda-chat-client/      # C# .NET console app (producer/consumer)
└── RedePanda-kafka-docker/     # Docker Compose for Redpanda broker
```

---

## 1. Start Redpanda

From inside `RedePanda-kafka-docker`:

```bash
docker compose --env-file env.local up -d
docker ps
```

You should see `redpanda-0` running.

---

## 2. Create a Chat Topic (if not already there)

```bash
docker exec -it redpanda-0 rpk topic create chat.room1 -p 1 --brokers redpanda-0:9092
docker exec -it redpanda-0 rpk topic list --brokers redpanda-0:9092
```

---

## 3. Build the Client

From inside `RedePanda-chat-client`:

```bash
dotnet restore
dotnet build
```

---

## 4. Run the Consumer

Open one terminal:

```bash
cd RedePanda-chat-client
dotnet run -- consume 127.0.0.1:19092
```

Output:

```
Consumer ← 127.0.0.1:19092 topic=chat.room1. Ctrl+C exits.
```

---

## 5. Run the Producer

Open another terminal:

```bash
cd RedePanda-chat-client
dotnet run -- produce 127.0.0.1:19092 --nick alice
```
Output:

```
Producer → 127.0.0.1:19092 topic=chat.room1. Type and Enter. Ctrl+C exits.
```

---
Type a message:

```
Hello world
```

The consumer terminal will show:

```
[12:34:56] alice: Hello world
```

---

## Multiple Participants

Start as many producers as you like, each with a different nickname:

```bash
dotnet run -- produce 127.0.0.1:19092 --nick bob
```
All messages will appear in the consumer terminal.

---

<!-- ## LAN Setup (Optional)

To chat across multiple machines in the same network:

1. Edit `env.lan` inside `RedePanda-kafka-docker`:
   ```
   ADVERTISED_HOST=192.168.x.x
   ```
   Replace with your host machine’s LAN IP.
2. Restart broker:
   ```bash
   docker compose down
   docker compose --env-file env.lan up -d
   ```
3. Other machines can now run:
   ```bash
   dotnet run -- consume 192.168.x.x:19092
   dotnet run -- produce 192.168.x.x:19092 --nick bob
   ```

--- -->

## Inspect Consumer Groups (Debugging)

```bash
docker exec -it redpanda-0 rpk group list --brokers redpanda-0:9092
docker exec -it redpanda-0 rpk group describe <GROUPNAME> --brokers redpanda-0:9092
```

---

## Stop Everything

```bash
cd RedePanda-kafka-docker
docker compose down
```

---

## Notes

- Producers/consumers are **not persistent**: once you close the terminal, the client is gone.
- Messages remain in the Kafka topic until log retention deletes them.
- To simulate “who is online”, you can extend the client to send join/leave messages.

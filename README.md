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

## 2. Build the Client

From inside `RedePanda-chat-client`:

```bash
dotnet restore
dotnet build
```

---

## 3. Create a Chat Topic (if not already there)

If you are starting the chat for the first time or if you want to start a new chat, you need to add the following command that includes the newTopic tag.
This creates a new chat and opens the chat instantly.

```bash
dotnet run -- 127.0.0.1:19092 --nick alice --newTopic newChat
```

You can choose any name you want instead of "newChat" to reference your chat. Or create multiple topics for chatting with different people.
To check if the topic was created successfully you can optionally execute the following command in the docker folder.

```bash
docker exec -it redpanda-0 rpk topic list --brokers redpanda-0:9092
```

---

## 4. Run an existing Chat

Open one terminal and enter the following command:

```bash
cd RedePanda-chat-client
dotnet run -- 127.0.0.1:19092 --nick alice --topic newChat
```

---
Type a message:

```
Hello world
```

The terminal will show:

```
[12:34:56] alice: Hello world
```

---

## Multiple Participants

Enter the chat with as many participants as you like, each with a different nickname:

```bash
dotnet run -- 127.0.0.1:19092 --nick bob --topic newChat
```
All messages will appear in the terminal of all participants of the chat.

---

## Show chat history

If you want to show the history of the chat when starting the chat and not only the current live chat you can execute the following command instead of starting the normal chat.

```bash
dotnet run -- 127.0.0.1:19092 --nick alice --topic newChat --hist true
```

This is only recommended if the history does not contain too many messages, because that would make the depiction messy.

---

## Deleting the history

If you want to delete old messages you have to execute the following command in the docker folder. Remember to use the name of your own topic.

```bash
docker exec -it redpanda-0 rpk topic delete newChat --brokers redpanda-0:9092
```

Note that this will the delete the whole topic which means that you will have to once again execute the command which creates a topic.

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

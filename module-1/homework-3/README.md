## Commands to test

```text
curl -X 'POST' 'http://0.0.0.0:8000/api/messages' -H 'accept: application/json' -H 'Content-Type: application/json' -d '{"message": "Message #1", "write_concern": 1}'
curl -X 'POST' 'http://0.0.0.0:8000/api/messages' -H 'accept: application/json' -H 'Content-Type: application/json' -d '{"message": "Message #2", "write_concern": 2}'
curl -X 'POST' 'http://0.0.0.0:8000/api/messages' -H 'accept: application/json' -H 'Content-Type: application/json' -d '{"message": "Message #2", "write_concern": 3}'

curl -X 'GET' 'http://0.0.0.0:8002/api/messages' -H 'accept: application/json'
```
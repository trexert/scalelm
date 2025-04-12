docker build . -f containers/http_frontend/Dockerfile -t http_frontend:latest
docker build . -f containers/llm_handler/Dockerfile -t llm_handler:latest
docker build . -f containers/ollama-gemma3-1b -t ollama-gemma3-1b:latest
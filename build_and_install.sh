#!/bin/bash
set -x
docker build . -f containers/http_frontend/Dockerfile -t http_frontend:latest
docker build . -f containers/llm_handler/Dockerfile -t llm_handler:latest
docker build . -f containers/ollama-qwen25-05b/Dockerfile -t ollama-qwen25-05b:latest
# docker build . -f containers/ollama-gemma3-1b/Dockerfile -t ollama-gemma3-1b:latest
minikube image load http_frontend:latest --daemon
minikube image load llm_handler:latest --daemon
minikube image load ollama-qwen25-05b:latest --daemon
# minikube image load ollama-gemma3-1b:latest --daemon

helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda -n scalelm --create-namespace
helm install scalelm helm -n scalelm

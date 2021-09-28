APP_NAME=chinmay-hb-app
PORT=8888

.PHONY: help

help: ## Get help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

build: ## Run unit tests and build the image
	docker build -t $(APP_NAME) .

build-no-cache: ## Run unit tests and build the image without using the cache
	docker build --no-cache -t $(APP_NAME) .

run: ## Run container with ports exposed for Jupyter Notebook
	docker run -i -t --rm -p=$(PORT):$(PORT) --name="$(APP_NAME)" $(APP_NAME)

up: build run ## Build the image and run container

stop: ## Stop and remove the running container
	docker stop $(APP_NAME) || true && docker rm $(APP_NAME) || true

destroy: ## Forcefully remove the image
	docker rmi -f $(APP_NAME)

clean: stop destroy ## Stop and remove a running container and also remove the image

docker_image := iomete/iomete-mongodb-sync
docker_tag := 1.0.0

export SPARK_CONF_DIR=./spark_conf
export APPLICATION_CONFIG_PATH=application.conf
export MONGO_PASSWORD=rootpassword

install-dev-requirements:
	pip install -r infra/requirements-dev.txt

run:
	python job.py

tests:
	pytest --capture=no --log-cli-level=INFO
	
mongodb-start:
	docker-compose -f infra/docker-compose-mongo.yml up -d

mongodb-stop:
	docker-compose -f infra/docker-compose-mongo.yml down

docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f infra/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f infra/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
# IOMETE MongoDB Sync Spark Job

This is a ready-to-use spark job to sync MongoDB collections to the Iomete Lakehouse as Iceberg tables.

## Project Structure

The project is composed of the following folders/files:
- `infra/`: contains requirements and Dockerfile files  
  - `requirements-dev.txt`: contains the list of python packages to install for development
  - `requirements.txt`: contains the list of python packages to install for production. This requirements file is used to build the Docker image
  - `Dockerfile`: contains the Dockerfile to build the spark job image
- `spark-conf/`: contains the spark configuration files for development environment
  - `spark-defaults.conf`: contains the spark configuration
  - `log4j.properties`: contains the log4j configuration for the PySpark job. This file is used to configure the logging level of the job
- `test_data/`: contains the test data for the job unit/integration tests
- `job.py`: contains the spark job code. Template comes with a sample code that reads a csv file from S3 and writes the data to a table in the Lakehouse. Feel free to modify the code to fit your needs.
- `test_job.py`: contains the spark job tests. Template comes with a sample test that reads the test data from `test_data/` and asserts the output of the job. Feel free to modify the tests to fit your needs.
- `Makefile`: contains the commands to run the job and tests


## Local Development

### Setup Virtual Environment

```bash
virtualenv .env
source .env/bin/activate

# make sure you have python version 3.7.0 or higher
make install-dev-requirements
```

### Run job locally

> Note: make sure the local mongodb is up and running. To start the mongodb, run the following command: `make mongodb-start`

```bash
make run
```

### Run tests

> Note: make sure the local mongodb is up and running. To start the mongodb, run the following command: `make mongodb-start`

```bash
make tests
```

## Deployment

### Build Docker Image

In the Makefile, modify `docker_image` and `docker_tag` variables to match your Docker image name and tag. 
For example, if you push your image to AWS ECR, your docker image name will be something like `123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image`.

Then, run the following command to build the Docker image:

```bash
make docker-push
```


### Creating a Spark Job

**Using the IOMETE Control Plane UI**

1. Go to `Spark Jobs` page
2. Click on `Create New` button
3. Provide the following information:
   - `Name`: `mongo-sync`
   - `Image`: `iomete/iomete-mongodb-sync:1.0.0`
   - `Main Application File`: `local:///app/job.py`
   - `Config file`: See application.conf as an example

### Run Spark Job

Once the Spark Job is created, you can run it using the Iomete Control Plane UI or API. To run it from the UI, go to the `Spark Jobs` page and click on the `Run` button.





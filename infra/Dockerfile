# Choose the IOMETE managed Spark image as the base image
FROM iomete/spark-py:3.2.1.1

# Set the working directory to /app
WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add additional jars to the spark classpath
COPY ./jars /opt/spark/jars

# add requirements.txt first to leverage Docker cache
COPY infra/requirements.txt ./
RUN pip install -r requirements.txt

# Copy the application files
COPY spark_conf/log4j.properties /opt/spark/iomete/log4j.properties
COPY job.py ./

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
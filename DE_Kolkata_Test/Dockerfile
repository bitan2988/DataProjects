# Use the official Python 3.11 image as the base image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy the entire directory "my_project" from your host machine to the container's working directory "/app"
COPY data /app/data
COPY requirements /app/requirements
COPY src /app/src
COPY config.json /app/config.json
COPY main.py /app/main.py
COPY readme.txt /app/readme.txt

# Install any dependencies if required (e.g., using pip)
RUN pip3 install -r requirements/requirements.txt

# Define the command to run your Python script or application
CMD ["python3", "/app/main.py"]


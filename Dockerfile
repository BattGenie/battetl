########################################
### This is the Dockerfile for BattETL
########################################

# Use the official image as a parent image.
FROM python:3.10.0-slim-buster

# Set the working directory.
WORKDIR /app

# Copy the file from your host to your current location.
COPY requirements.txt requirements.txt

# Run the command inside your image filesystem.
RUN pip3 install -r requirements.txt

# Copy the rest of your app's source code from your host to your image filesystem.
COPY . .    

# Run the specified command within the container.
RUN pip install -e .
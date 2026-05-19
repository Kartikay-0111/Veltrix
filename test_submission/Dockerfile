# Use a lightweight GCC image
FROM gcc:latest

WORKDIR /app

# Copy the C++ file into the container
COPY main.cpp .

# Compile the code
RUN g++ -O3 -o engine main.cpp

# Expose the port the app runs on
EXPOSE 8080

# Run the engine
CMD ["./engine"]
FROM gcc:13

WORKDIR /app
COPY main.cpp /app/main.cpp
RUN g++ -std=c++20 -O2 -pthread /app/main.cpp -o /app/server
EXPOSE 8080
CMD ["/app/server"]
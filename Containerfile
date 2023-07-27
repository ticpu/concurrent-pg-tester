FROM docker.io/library/golang:1.18-stretch
WORKDIR /app
COPY . .
RUN go build -o concurrent-pg-tester
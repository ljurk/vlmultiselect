FROM golang:alpine as build
WORKDIR /
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o app .

FROM alpine:latest
WORKDIR /
COPY --from=build /app /app
ENTRYPOINT ["sh", "-c", "./app"]

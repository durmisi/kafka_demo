# Stage 1: Build the Go binary
FROM golang:latest AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source code from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app without debug information
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o main .

# Stage 2: Create a lightweight image to run the Go binary
FROM scratch

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the built Go binary from the builder stage
COPY --from=builder /app/main .

# Command to run the executable
CMD ["./main"]

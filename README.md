# Redis Clone in Rust

This project is a Redis clone implemented in Rust, developed as part of the CodeCrafters "Build Your Own Redis" challenge. It aims to replicate the behavior of a Redis server, handling various commands and managing data in memory.

## Project Structure

*   **`src/main.rs`**: The main entry point of the application. It sets up the TCP listener, accepts incoming client connections, and spawns asynchronous tasks to handle each connection.
*   **`src/commands.rs`**: Defines the `Command` enum, which represents all supported Redis commands. It includes the `execute` method for each command, containing the core logic for processing requests and interacting with the database. It also handles parsing raw RESP data into `Command` structs.
*   **`src/db.rs`**: Manages the in-memory data store. It defines `Db`, `DbValue` (for different data types like strings, lists, and streams), `StreamList`, and `StreamItem`. It provides methods for database operations such as `SET`, `GET`, `RPUSH`, `LPUSH`, `LPOP`, `LLEN`, `LRANGE`, `XADD`, and `XRANGE`, including expiration handling for keys.
*   **`src/resp.rs`**: Implements the Redis Serialization Protocol (RESP) for communication. It contains the `RespValue` enum to represent various RESP data types and methods for serializing these values into bytes to be sent over the network, as well as parsing incoming bytes from the client into `RespValue`s.

## How to Run

To run this Redis server:

1.  **Ensure Rust is installed**: Make sure you have the Rust toolchain (including Cargo) installed on your system. If not, you can install it via `rustup`.
2.  **Build the project**: Navigate to the root directory of the project in your terminal and run:
    ```bash
    cargo build
    ```
3.  **Start the server**: Execute the provided script to run your server:
    ```bash
    ./your_program.sh
    ```
    The server will start listening on `127.0.0.1:6379`.

## Testing with `redis-cli`

You can interact with your running Redis clone using the official `redis-cli` tool:

1.  **Connect to the server**:
    ```bash
    redis-cli -p 6379
    ```
2.  **Try some commands**:
    ```
    127.0.0.1:6379> PING
    PONG
    127.0.0.1:6379> SET mykey myvalue PX 10000
    OK
    127.0.0.1:6379> GET mykey
    "myvalue"
    127.0.0.1:6379> RPUSH mylist item1 item2 item3
    (integer) 3
    127.0.0.1:6379> LRANGE mylist 0 -1
    1) "item1"
    2) "item2"
    3) "item3"
    127.0.0.1:6379> XADD mystream * temperature 25 humidity 80
    "1701000000000-0"
    127.0.0.1:6379> XRANGE mystream - +
    1) 1) "1701000000000-0"
       2) 1) "temperature"
          2) "25"
          3) "humidity"
          4) "80"
    ```

package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

type Type byte

const (
	SimpleString Type = '+'
	BulkString   Type = '$'
	Array        Type = '*'
)

type Value struct {
	typ   Type
	bytes []byte
}

func (v Value) String() string {
	if v.typ == BulkString || v.typ == SimpleString {
		return string(v.bytes)
	}

	return ""
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	//Initialize the data store
	storage := NewStorage()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, storage)
	}
}

func handleConnection(conn net.Conn, storage *Storage) {
	defer conn.Close()
	for {
		value, err := DecodeRESP(bufio.NewReader(conn))
		if err != nil {
			fmt.Println("Error decoding RESP: ", err.Error())
			return // Ignore clients that we fail to read from
		}

		command := strings.Fields(value.String())[0]
		command = strings.ToLower(command)
		args := strings.Fields(value.String())[1:]

		switch command {
		case "ping":
			conn.Write([]byte("+PONG\r\n"))
		case "echo":
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(string(args[0])), string(args[0]))))
		case "set":
			storage.Set(args[0], args[1])
			conn.Write([]byte("+OK\r\n"))
		case "get":
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", storage.Get(args[0]))))
		default:
			conn.Write([]byte("-ERR unknown command '" + command + "'\r\n"))
		}
	}

}

func DecodeRESP(byteStream *bufio.Reader) (Value, error) {
	dataTypeByte, err := byteStream.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch string(dataTypeByte) {
	case "+":
		return decodeSimpleString(byteStream)
	case "$":
		return decodeBulkString(byteStream)
	}

	return Value{}, fmt.Errorf("invalid RESP data type byte: %s", string(dataTypeByte))
}

func decodeSimpleString(byteStream *bufio.Reader) (Value, error) {
	readBytes, err := readUntilCRLF(byteStream)
	if err != nil {
		return Value{}, err
	}

	return Value{
		typ:   SimpleString,
		bytes: readBytes,
	}, nil
}

func decodeBulkString(byteStream *bufio.Reader) (Value, error) {
	readBytesForCount, err := readUntilCRLF(byteStream)
	if err != nil {
		return Value{}, fmt.Errorf("failed to read bulk string length: %s", err)
	}

	count, err := strconv.Atoi(string(readBytesForCount))
	if err != nil {
		return Value{}, fmt.Errorf("failed to parse bulk string length: %s", err)
	}

	readBytes := make([]byte, count+2)

	if _, err := io.ReadFull(byteStream, readBytes); err != nil {
		return Value{}, fmt.Errorf("failed to read bulk string contents: %s", err)
	}

	return Value{
		typ:   BulkString,
		bytes: readBytes[:count],
	}, nil
}

func readUntilCRLF(byteStream *bufio.Reader) ([]byte, error) {
	readBytes := []byte{}

	for {
		b, err := byteStream.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		readBytes = append(readBytes, b...)
		if len(readBytes) >= 2 && readBytes[len(readBytes)-2] == '\r' {
			break
		}
	}

	return readBytes[:len(readBytes)-2], nil
}

package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os/exec"

	"github.com/kr/pty"
	"golang.org/x/crypto/ssh"
)

var (
	serverPort = flag.Uint("port", uint(22), "the port to listen on")
)

func main() {
	flag.Parse()

	config, err := setupAuthentication()
	if err != nil {
		log.Fatalf("setup authentication: %v", err)
	}

	serverAddress := fmt.Sprintf(":%d", *serverPort)
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("listen for connection: %v", err)
	}
	log.Printf("Listening on %s...", serverAddress)

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Fatalf("accept incoming connection: %v", err)
		}

		go handleConnection(connection, config)
	}
}

func setupAuthentication() (*ssh.ServerConfig, error) {
	config := &ssh.ServerConfig{
		NoClientAuth: true,
	}

	privateBytes, err := ioutil.ReadFile("id_rsa")
	if err != nil {
		return nil, fmt.Errorf("read private server key: %v", err)
	}
	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		return nil, fmt.Errorf("parse private server key: %v", err)
	}
	config.AddHostKey(private)
	return config, nil
}

func handleConnection(nConn net.Conn, serverConfig *ssh.ServerConfig) error {
	// Before use, a handshake must be performed on the incoming
	// net.Conn.
	_, chans, reqs, err := ssh.NewServerConn(nConn, serverConfig)
	if err != nil {
		return fmt.Errorf("handshake: %v", err)
	}

	// Service the incoming request channel so connection doesn't hang
	go ssh.DiscardRequests(reqs)

	// Service the incoming channels
	for newChannel := range chans {
		if err = serviceChannel(newChannel); err != nil {
			return err
		}
	}
	return nil
}

func serviceChannel(newChannel ssh.NewChannel) error {
	if newChannel.ChannelType() != "session" {
		newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
		return nil
	}
	channel, requests, err := newChannel.Accept()
	if err != nil {
		return fmt.Errorf("accept channel: %v", err)
	}
	defer channel.Close()

	// Start the command with a pseudo-terminal.
	bashPTY, err := pty.Start(exec.Command("bash"))
	if err != nil {
		return fmt.Errorf("start pty: %v", err)
	}
	defer bashPTY.Close()

	go func(in <-chan *ssh.Request) {
		for req := range in {
			// Tell the client we accept pty and shell commands
			req.Reply(req.Type == "pty-req" || req.Type == "shell", nil)

			var winSize struct {
				Width  uint32
				Height uint32
			}

			if req.Type == "pty-req" {
				skip := binary.BigEndian.Uint32(req.Payload)
				buf := bytes.NewReader(req.Payload[4+skip:])
				err = binary.Read(buf, binary.BigEndian, &winSize)
				pty.Setsize(bashPTY, &pty.Winsize{Cols: uint16(winSize.Width), Rows: uint16(winSize.Height)})
			}

			if req.Type == "window-change" {
				buf := bytes.NewReader(req.Payload)
				err = binary.Read(buf, binary.BigEndian, &winSize)
				pty.Setsize(bashPTY, &pty.Winsize{Cols: uint16(winSize.Width), Rows: uint16(winSize.Height)})
			}
		}
	}(requests)

	// Redirect pseudo-terminal output to client channel
	go io.Copy(bashPTY, channel)
	// Redirect client channel input to pseudo-terminal
	io.Copy(channel, bashPTY)
	return nil
}

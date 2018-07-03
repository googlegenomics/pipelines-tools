package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"

	"github.com/googlegenomics/pipelines-tools/gce"
	"github.com/kr/pty"
	"golang.org/x/crypto/ssh"
)

var (
	port = flag.Uint("port", 22, "the port to listen on")
)

func main() {
	flag.Parse()

	config, listener, err := startServer(*port)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	log.Printf("Listening on %s...", listener.Addr())

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Fatalf("Failed to accept incoming connection: %v", err)
		}

		go func() {
			handleConnection(connection, config)
		}()
	}
}

func startServer(port uint) (*ssh.ServerConfig, net.Listener, error) {
	config, err := getConfiguration()
	if err != nil {
		return nil, nil, fmt.Errorf("getting configuration: %v", err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("listen: %v", err)
	}
	return config, listener, nil
}

func getConfiguration() (*ssh.ServerConfig, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generating server key pair: %v", err)
	}

	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return nil, fmt.Errorf("creating signer: %v", err)
	}

	config := &ssh.ServerConfig{
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			authorizedKeys, err := gce.GetAuthorizedKeys()
			if err != nil {
				return nil, fmt.Errorf("getting the authorized keys: %v", err)
			}

			if !authorizedKeys[string(key.Marshal())] {
				return nil, errors.New("unauthorized")
			}
			return nil, nil
		},
	}
	config.AddHostKey(signer)
	return config, nil
}

func handleConnection(conn net.Conn, serverConfig *ssh.ServerConfig) error {
	_, chans, reqs, err := ssh.NewServerConn(conn, serverConfig)
	if err != nil {
		return fmt.Errorf("handshake: %v", err)
	}

	// Discard out-of-band SSH requests (not supported by this server).
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if err := serviceChannel(newChannel); err != nil {
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
		return fmt.Errorf("accepting channel: %v", err)
	}
	defer channel.Close()

	// Start the command with a pseudo-terminal.
	shell, err := pty.Start(exec.Command("bash"))
	if err != nil {
		return fmt.Errorf("starting pty: %v", err)
	}
	defer shell.Close()

	go func(in <-chan *ssh.Request) {
		for req := range in {
			// Tell the client we accept pty and shell commands
			req.Reply(req.Type == "pty-req" || req.Type == "shell", nil)

			if req.Type == "pty-req" {
				skip := binary.BigEndian.Uint32(req.Payload)
				resize(shell, req.Payload[4+skip:])
			}
			if req.Type == "window-change" {
				resize(shell, req.Payload)
			}
		}
	}(requests)

	go io.Copy(shell, channel)
	io.Copy(channel, shell)
	return nil
}

func resize(f *os.File, payload []byte) {
	var size struct {
		Width, Height uint32
	}
	if err := binary.Read(bytes.NewReader(payload), binary.BigEndian, &size); err == nil {
		pty.Setsize(f, &pty.Winsize{Cols: uint16(size.Width), Rows: uint16(size.Height)})
	}
}

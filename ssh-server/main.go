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
	defer listener.Close()

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Fatalf("Failed to accept incoming connection: %v", err)
		}

		go func() {
			if err := handleConnection(connection, config); err != nil {
				log.Printf("Handling connection: %v", err)
			}
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
	sConn, chans, reqs, err := ssh.NewServerConn(conn, serverConfig)
	if err != nil {
		return fmt.Errorf("handshake: %v", err)
	}
	defer sConn.Close()

	// Discard out-of-band SSH requests (not supported by this server).
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		go func() {
			if err := serviceChannel(newChannel); err != nil {
				log.Printf("Failed to service channel: %v", err)
			}
		}()
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

	allow := map[string]bool{"shell": true, "exec": true, "pty-req": true, "window-change": true}
	resize := make(chan *pty.Winsize, 1)
	done := make(chan struct{})
	for {
		select {
		case <-done:
			return nil
		case req, ok := <-requests:
			if !ok {
				return nil
			}
			if req.WantReply {
				req.Reply(allow[req.Type], nil)
			}
			switch req.Type {
			case "pty-req":
				go func() {
					newPTY(channel, resize)
					close(done)
				}()

				skip := binary.BigEndian.Uint32(req.Payload)
				size, err := windowSize(req.Payload[4+skip:])
				if err != nil {
					log.Printf("Failed to get window size: %v", err)
					continue
				}
				resize <- size
			case "window-change":
				size, err := windowSize(req.Payload)
				if err != nil {
					log.Printf("Failed to get window size: %v", err)
					continue
				}
				resize <- size
			}
		}
	}
}

func newPTY(channel ssh.Channel, resize chan *pty.Winsize) {
	//Start the command with a pseudo-terminal.
	shell, err := pty.Start(exec.Command("bash"))
	if err != nil {
		log.Printf("starting pty: %v", err)
	}
	defer shell.Close()

	go func() {
		for size := range resize {
			pty.Setsize(shell, size)
		}
	}()

	go io.Copy(shell, channel)
	io.Copy(channel, shell)

	status := struct{ Status uint32 }{uint32(0)}
	if _, err := channel.SendRequest("exit-status", false, ssh.Marshal(&status)); err != nil {
		log.Printf("Failed to send exit request: %v", err)
	}
	channel.Close()
}

func windowSize(payload []byte) (*pty.Winsize, error) {
	var size struct {
		Width, Height uint32
	}
	if err := binary.Read(bytes.NewReader(payload), binary.BigEndian, &size); err == nil {
		return &pty.Winsize{Cols: uint16(size.Width), Rows: uint16(size.Height)}, nil
	} else {
		return nil, err
	}
}

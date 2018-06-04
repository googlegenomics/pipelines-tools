package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"

	"github.com/kr/pty"
	"golang.org/x/crypto/ssh"
)

var (
	serverPort = flag.Uint("port", uint(22), "the port to listen on")
	password   = flag.String("password", "", "the client authentication password")
)

func main() {
	flag.Parse()

	if envpwd := os.Getenv("password"); *password == "" && envpwd != "" {
		*password = envpwd
	}

	config, err := setupAuthentication()
	if err != nil {
		log.Fatalf("failed to complete authentication setup: %v", err)
	}

	serverAddress := fmt.Sprintf("0.0.0.0:%d", *serverPort)
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("failed to listen for connection: %v", err)
	}
	log.Printf("Listening on %s...", serverAddress)

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Fatalf("failed to accept incoming connection: ", err)
		}

		go handleConnection(connection, config)
	}
}

func setupAuthentication() (*ssh.ServerConfig, error) {
	config := &ssh.ServerConfig{
		PasswordCallback: func(conn ssh.ConnMetadata, providedPwd []byte) (*ssh.Permissions, error) {
			if string(providedPwd) == *password {
				return nil, nil
			}
			return nil, fmt.Errorf("authentication failed for user %s", conn.User())
		},
	}

	privateBytes, err := ioutil.ReadFile("id_rsa")
	if err != nil {
		log.Fatal("Failed to read private server key (id_rsa)")
	}
	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		log.Fatal("Failed to parse private server key")
	}
	config.AddHostKey(private)
	return config, nil
}

func handleConnection(nConn net.Conn, serverConfig *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(nConn, serverConfig)
	if err != nil {
		fmt.Errorf("failed to handshake, %v", err)
	}

	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		// only accept ServerShell protocol
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			fmt.Errorf("Could not accept channel: %v", err)
		}
		defer channel.Close()

		// Start the command with a pseudo-terminal.
		bashPTY, err := pty.Start(exec.Command("bash"))
		if err != nil {
			fmt.Errorf("Could not start pty: %v", err)
		}

		// Copy stdin to the pty and the pty to stdout.
		go func() {
			_, _ = io.Copy(bashPTY, channel)
			channel.Close()
		}()
		go func() {
			_, _ = io.Copy(channel, bashPTY)
			channel.Close()
		}()

		go func(in <-chan *ssh.Request) {
			for req := range in {

				req.Reply(req.Type == "pty-req" || req.Type == "shell", nil)
			}
		}(requests)
	}
}

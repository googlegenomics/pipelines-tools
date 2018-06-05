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

	"os/signal"
	"syscall"

	"github.com/kr/pty"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	serverPort = flag.Uint("port", uint(22), "the port to listen on")
)

func main() {
	flag.Parse()

	config, err := setupAuthentication()
	if err != nil {
		log.Fatalf("failed to complete authentication setup: %v", err)
	}

	serverAddress := fmt.Sprintf(":%d", *serverPort)
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("failed to listen for connection: %v", err)
	}
	log.Printf("Listening on %s...", serverAddress)

	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Fatalf("failed to accept incoming connection: %v", err)
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
		log.Fatal("Failed to read private server key (id_rsa)")
		return nil, err
	}
	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		log.Fatal("Failed to parse private server key")
		return nil, err
	}
	config.AddHostKey(private)
	return config, nil
}

func handleConnection(nConn net.Conn, serverConfig *ssh.ServerConfig) error {
	// Before use, a handshake must be performed on the incoming
	// net.Conn.
	_, chans, reqs, err := ssh.NewServerConn(nConn, serverConfig)
	if err != nil {
		return fmt.Errorf("failed to handshake, %v", err)
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
		return fmt.Errorf("Could not accept channel: %v", err)
	}
	defer channel.Close()

	// Tell the client we accept pty and shell commands
	go func(in <-chan *ssh.Request) {
		for req := range in {
			req.Reply(req.Type == "pty-req" || req.Type == "shell", nil)
		}
	}(requests)

	// Start the command with a pseudo-terminal.
	bashPTY, err := pty.Start(exec.Command("bash"))
	if err != nil {
		return fmt.Errorf("Could not start pty: %v", err)
	}
	defer bashPTY.Close()

	// Resize pseudo terminal so the terminal pointer will be at the correct position
	resizePTY(bashPTY)

	// Redirect pseudo-terminal output to client channel
	go func() {
		io.Copy(bashPTY, channel)
	}()
	// Redirect client channel input to pseudo-terminal
	io.Copy(channel, bashPTY)
	return nil
}

func resizePTY(bashPTY *os.File) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	go func() {
		for range ch {
			if err := pty.InheritSize(os.Stdin, bashPTY); err != nil {
				log.Printf("error resizing pty: %s", err)
			}
		}
	}()
	ch <- syscall.SIGWINCH
	// Initial resize.
	// Set stdin in raw mode.
	oldState, err := terminal.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = terminal.Restore(int(os.Stdin.Fd()), oldState) }()
}

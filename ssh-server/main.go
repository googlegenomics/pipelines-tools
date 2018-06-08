package main

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	genomics "google.golang.org/api/genomics/v1"
)

var (
	serverPort = flag.Uint("port", uint(22), "the port to listen on")
	project    = flag.String("project", defaultProject(), "the cloud project name")
)

func main() {
	flag.Parse()

	config, listener, err := startServer()
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

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

func defaultProject() string {
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func startServer() (*ssh.ServerConfig, net.Listener, error) {
	config, err := getConfiguration()
	if err != nil {
		return nil, nil, fmt.Errorf("config server: %v", err)
	}

	serverAddress := fmt.Sprintf(":%d", *serverPort)
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("listening for connection: %v", err)
	}
	log.Printf("Listening on %s...", serverAddress)
	return config, listener, nil
}

func getConfiguration() (*ssh.ServerConfig, error) {

	config := &ssh.ServerConfig{
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			authorizedKeys, err := getAuthorizedKeys()
			if err != nil {
				return nil, fmt.Errorf("getting the authorized keys: %v", err)
			}

			if !authorizedKeys[string(key.Marshal())] {
				return nil, fmt.Errorf("authorizing key: %v", err)
			}
			return nil, nil
		},
	}
	privateBytes, err := ioutil.ReadFile("id_rsa")
	if err != nil {
		return nil, fmt.Errorf("reading private server key: %v", err)
	}
	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing private server key: %v", err)
	}
	config.AddHostKey(private)
	return config, nil
}

func getAuthorizedKeys() (map[string]bool, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, genomics.GenomicsScope)
	if err != nil {
		return nil, fmt.Errorf("creating authenticated client: %v", err)
	}

	gce, err := compute.New(client)
	if err != nil {
		return nil, fmt.Errorf("getting GCE service instance: %v", err)
	}

	op, err := gce.Projects.Get(*project).Do()
	if err != nil {
		return nil, fmt.Errorf("getting projects metadata: %v", err)
	}

	authorizedKeys := map[string]bool{}
	for _, item := range op.CommonInstanceMetadata.Items {
		if item.Key == "ssh-keys" {
			keysBytes := []byte(*item.Value)
			for len(keysBytes) > 0 {
				pbkey, _, _, rest, err := ssh.ParseAuthorizedKey(keysBytes)
				if err != nil {
					return nil, fmt.Errorf("parsing authorized key: %v", err)
				}
				authorizedKeys[string(pbkey.Marshal())] = true
				keysBytes = rest
			}
			break
		}
	}
	return authorizedKeys, nil
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
	bashPTY, err := pty.Start(exec.Command("bash"))
	if err != nil {
		return fmt.Errorf("starting pty: %v", err)
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

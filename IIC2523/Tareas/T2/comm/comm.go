package comm

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// IDCounter counts number of ids in the execution
var IDCounter int = 0

// Client ...
type Client struct {
	ID           int
	Host         string
	Port         int
	ListenSocket net.Listener
}

func (c Client) String() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Host ...
type Host struct {
	ID           int
	Host         string
	Port         int
	IsActive     bool
	ClientSocket net.Conn
	ListenSocket net.Conn
}

func (h Host) String() string {
	return fmt.Sprintf("%s:%d", h.Host, h.Port)
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// HostToClient transforms a Host object into a Client
func HostToClient(h *Host) *Client {
	var client Client
	client.ID = h.ID
	client.Host = h.Host
	client.Port = h.Port
	return &client
}

// ParseHost processes a string into a Host object
func ParseHost(s string) *Host {
	var host Host
	slice := strings.Split(s, " ")
	host.Host = slice[0]
	port, err := strconv.Atoi(slice[1])
	check(err)
	host.Port = port
	host.ID = IDCounter
	IDCounter++
	return &host
}

// GetHosts retrieves hosts from file
func GetHosts(path string) []*Host {
	f, err := os.Open(path)
	check(err)
	defer f.Close()

	var result []*Host
	r := bufio.NewReader(f)

	err = nil
	var buffer string
	for err != io.EOF {
		check(err)
		buffer, err = r.ReadString('\n')
		result = append(result, ParseHost(strings.Trim(buffer, "\r\n")))
	}

	return result
}

// FindHostByAddress finds a host from the list by their address
func FindHostByAddress(address string, hosts []*Host) *Host {
	var host *Host = nil
	for i := range hosts {
		if fmt.Sprint(hosts[i]) == address {
			host = hosts[i]
			break
		}
	}
	return host
}

// FindHostByID finds a host from the list by their id
func FindHostByID(id int, hosts []*Host) *Host {
	var host *Host = nil
	for i := range hosts {
		if hosts[i].ID == id {
			host = hosts[i]
			break
		}
	}
	return host
}

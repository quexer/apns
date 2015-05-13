package apns

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

var (
	MAX_SEND_Q = 10000
	TIME_OUT   = time.Minute // dial & write timeout, avoid infinite block
	ErrChannel = make(chan *SendErr)
)

type SendErr struct {
	Pn  *PushNotification
	Res *errResponse
}

type opSend struct {
	Pn    *PushNotification
	ChErr chan error
}

const (
	signal_STOP = iota
)

// Client contains the fields necessary to communicate
// with Apple, such as the gateway to use and your
// certificate contents.
//
// You'll need to provide your own CertificateFile
// and KeyFile to send notifications. Ideally, you'll
// just set the CertificateFile and KeyFile fields to
// a location on drive where the certs can be loaded,
// but if you prefer you can use the CertificateBase64
// and KeyBase64 fields to store the actual contents.
type Client struct {
	Gateway           string
	CertificateFile   string
	CertificateBase64 string
	KeyFile           string
	KeyBase64         string
	certificate       tls.Certificate
	apnsConnection    *tls.Conn
	chErrResponse     chan *errResponse
	chSend            chan *opSend
	chSignal          chan int
	chConnect         chan chan error
	chConnectionErr   chan *tls.Conn
	sentQ             *pnQueue
	counter           int32
}

// BareClient can be used to set the contents of your
// certificate and key blocks manually.
func BareClient(gateway, certificateBase64, keyBase64 string) (c *Client) {
	c = create(gateway)
	c.CertificateBase64 = certificateBase64
	c.KeyBase64 = keyBase64
	return
}

// NewClient assumes you'll be passing in paths that
// point to your certificate and key.
func NewClient(gateway, certificateFile, keyFile string) (c *Client) {
	c = create(gateway)
	c.CertificateFile = certificateFile
	c.KeyFile = keyFile
	return
}

func create(gateway string) (c *Client) {
	c = new(Client)
	c.Gateway = gateway
	c.chErrResponse = make(chan *errResponse, 10)
	c.chSend = make(chan *opSend)
	c.chConnect = make(chan chan error)
	c.chConnectionErr = make(chan *tls.Conn)
	c.chSignal = make(chan int)
	c.sentQ = newPnQueue(MAX_SEND_Q)

	return c
}

func (p *Client) run() {
	defer log.Printf("client %p stop running \f", p)
	for {
		select {
		case res := <-p.chErrResponse:
			p.handleErrResponse(res)
		case op := <-p.chSend:
			op.ChErr <- p.innerSend(op.Pn)
		case ch := <-p.chConnect:
			if p.apnsConnection == nil {
				ch <- p.openConnection()
			} else {
				ch <- nil
			}
		case conn := <-p.chConnectionErr:
			if p.apnsConnection == conn {
				p.innerClose()
			} else {
				go conn.Close()
			}
		case <-p.chSignal:
			p.innerClose()
			//final stop
			return
		}
	}
}

func (client *Client) handleErrResponse(res *errResponse) {
	if res.Command == 0 {
		//no error
		return
	}

	errPn, reSend := client.sentQ.Tail(res.Identifier)
	log.Printf("handle err response %d, %##v\n", res.Identifier, errPn)

	if errPn == nil {
		log.Println("[warn] maybe MAX_SEND_Q is too short:", MAX_SEND_Q)
		return
	}

	go func() {
		ErrChannel <- &SendErr{Pn: errPn, Res: res}
	}()

	client.sentQ.Clear()

	if len(reSend) == 0 {
		return
	}

	go func(l []*PushNotification) {
		for _, pn := range l {
			if err := client.Send(pn); err != nil {
				log.Println("re-send err", err, pn.Identifier)
			}
		}
	}(reSend)
}

func (client *Client) Send(pn *PushNotification) error {
	op := &opSend{Pn: pn, ChErr: make(chan error)}
	client.chSend <- op
	return <-op.ChErr
}

// Send connects to the APN service and sends your push notification.
// Remember that if the submission is successful, Apple won't reply.
func (client *Client) innerSend(pn *PushNotification) error {

	pn.Identifier = client.counter
	client.counter = (client.counter + 1) % IdentifierUbound

	payload, err := pn.ToBytes()
	if err != nil {
		return err
	}

	err = client.connectAndWrite(payload)
	if err == nil {
		client.sentQ.Append(pn)
	} else {
		client.apnsConnection = nil
		go func() {
			ErrChannel <- &SendErr{Pn: pn, Res: nil}
		}()
	}

	return err
}

func (client *Client) Connect() error {
	go client.run()

	op := make(chan error)
	client.chConnect <- op
	return <-op
}

// ConnectAndWrite establishes the connection to Apple and handles the
// transmission of your push notification, as well as waiting for a reply.
//
// In lieu of a timeout (which would be available in Go 1.1)
// we use a timeout channel pattern instead. We start two goroutines,
// one of which just sleeps for TimeoutSeconds seconds, while the other
// waits for a response from the Apple servers.
//
// Whichever channel puts data on first is the "winner". As such, it's
// possible to get a false positive if Apple takes a long time to respond.
// It's probably not a deal-breaker, but something to be aware of.
func (client *Client) connectAndWrite(payload []byte) error {
	if client.apnsConnection == nil {
		if err := client.openConnection(); err != nil {
			return err
		}
	}

	if err := client.apnsConnection.SetWriteDeadline(time.Now().Add(TIME_OUT)); err != nil {
		return err
	}
	_, err := client.apnsConnection.Write(payload)
	if err != nil {
		log.Println("write error ", err, "try again")
		//		if err != io.EOF && err.Error() != "use of closed network connection" && err != syscall.EPIPE {
		//			return err
		//		}
		//		log.Println("try again")

		// If the connection is closed, reconnect
		if err := client.openConnection(); err != nil {
			return err
		}

		if err := client.apnsConnection.SetWriteDeadline(time.Now().Add(TIME_OUT)); err != nil {
			return err
		}
		if _, err := client.apnsConnection.Write(payload); err != nil {
			return err
		}
	}
	return err
}

// Opens a connection to the Apple APNS server
// The connection is created and persisted to the client's apnsConnection property
//	to save on the overhead of the crypto libraries.
func (client *Client) openConnection() error {
	log.Printf("open connection %p\n", client)
	err := client.getCertificate()
	if err != nil {
		log.Println("cert err", err)
		return err
	}

	conf := &tls.Config{
		Certificates: []tls.Certificate{client.certificate},
		ServerName:   strings.Split(client.Gateway, ":")[0],
		MinVersion:   tls.VersionTLS10,
	}

	conn, err := net.DialTimeout("tcp", client.Gateway, TIME_OUT)
	if err != nil {
		log.Println("open connection err", err)
		return err
	}

	tlsConn := tls.Client(conn, conf)
	//add handshake timeout
	if err := tlsConn.SetDeadline(time.Now().Add(TIME_OUT)); err != nil {
		return err
	}
	err = tlsConn.Handshake()
	if err != nil {
		log.Println("tls handshake err", err)
		return err
	}

	//clear read timeout
	if err := tlsConn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	client.apnsConnection = tlsConn
	go read(client, tlsConn)
	return nil
}

func (p *Client) tryReset(conn *tls.Conn) {
	if p.apnsConnection == conn {
		p.apnsConnection = nil
	}
}

func read(client *Client, conn *tls.Conn) {
	buffer := make([]byte, ERR_RESPONSE_LEN)

	if _, err := conn.Read(buffer); err != nil {
		log.Printf("read err %v, %v, %p\n", err, err == io.EOF, client)
		client.chConnectionErr <- conn
		return
	}

	errRsp := &errResponse{
		Command: uint8(buffer[0]),
		Status:  uint8(buffer[1]),
	}

	if err := binary.Read(bytes.NewBuffer(buffer[2:]), binary.BigEndian, &errRsp.Identifier); err != nil {
		log.Println("read identifier err", err)
		return
	}

	if errRsp.Command != ERR_RESPONSE_CMD {
		log.Println("unknown err response", buffer)
		return
	}

	errMsg, ok := ApplePushResponses[errRsp.Status]
	if !ok {
		log.Println("unknown err status", buffer)
		return
	}

	log.Printf("get err response : %##v, %s\n", errRsp, errMsg)

	client.chErrResponse <- errRsp
}

// Returns a certificate to use to send the notification.
// The certificate is only created once to save on
// the overhead of the crypto libraries.
func (client *Client) getCertificate() error {
	var err error

	if client.certificate.PrivateKey == nil {
		if len(client.CertificateBase64) == 0 && len(client.KeyBase64) == 0 {
			// The user did not specify raw block contents, so check the filesystem.
			client.certificate, err = tls.LoadX509KeyPair(client.CertificateFile, client.KeyFile)
		} else {
			// The user provided the raw block contents, so use that.
			client.certificate, err = tls.X509KeyPair([]byte(client.CertificateBase64), []byte(client.KeyBase64))
		}
	}

	return err
}

func (p *Client) Stop() {
	p.chSignal <- signal_STOP
}

func (client *Client) innerClose() {
	if client.apnsConnection != nil {
		go client.apnsConnection.Close()
		client.apnsConnection = nil
	}
}

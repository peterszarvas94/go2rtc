// Suggested pkg/xiaomi/miss/cs2/conn_test.go; no physical camera/audio required.
package cs2

import (
	"bytes"
	"net"
	"testing"
)

type captureConn struct {
	net.Conn
	packet []byte
}

func (c *captureConn) Write(p []byte) (int, error) {
	c.packet = append([]byte(nil), p...)
	return len(p), nil
}
func TestWritePacketPreservesPayload(t *testing.T) {
	wire := &captureConn{}
	conn := &Conn{Conn: wire}
	hdr := bytes.Repeat([]byte{0xa5}, int(hdrSize))
	payload := []byte{1, 2, 3, 4, 5, 6, 7}
	if err := conn.WritePacket(hdr, payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire.packet[12:12+int(hdrSize)], hdr) {
		t.Fatal("header changed")
	}
	if !bytes.Equal(wire.packet[12+int(hdrSize):], payload) {
		t.Fatal("payload corrupted")
	}
}

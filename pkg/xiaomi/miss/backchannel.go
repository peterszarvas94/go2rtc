package miss

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/opus"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
)

func (p *Producer) AddTrack(media *core.Media, _ *core.Codec, track *core.Receiver) error {
	if err := p.client.StartSpeaker(); err != nil {
		return err
	}
	// TODO: check this!!!
	time.Sleep(time.Second)

	sender := core.NewSender(media, track.Codec)

	switch track.Codec.Name {
	case core.CodecPCMA:
		var buf []byte

		if p.client.SpeakerCodec() == codecPCM {
			dst := &core.Codec{Name: core.CodecPCML, ClockRate: 8000}
			transcode := pcm.Transcode(dst, track.Codec)

			sender.Handler = func(pkt *rtp.Packet) {
				buf = append(buf, transcode(pkt.Payload)...)
				const size = 2 * 8000 * 0.040 // 16bit 40ms
				for len(buf) >= size {
					p.Send += size
					_ = p.client.WriteAudio(codecPCM, buf[:size])
					buf = buf[size:]
				}
			}
		} else {
			sender.Handler = func(pkt *rtp.Packet) {
				buf = append(buf, pkt.Payload...)
				const size = 8000 * 0.040 // 8bit 40 ms
				for len(buf) >= size {
					p.Send += size
					_ = p.client.WriteAudio(codecPCMA, buf[:size])
					buf = buf[size:]
				}
			}
		}
	case core.CodecOpus:
		if p.client.SpeakerCodec() == codecOPUS {
			var buf []byte
			sender.Handler = func(pkt *rtp.Packet) {
				if buf == nil {
					buf = pkt.Payload
				} else {
					// convert two 20ms to one 40ms
					buf = opus.JoinFrames(buf, pkt.Payload)
					p.Send += len(buf)
					_ = p.client.WriteAudio(codecOPUS, buf)
					buf = nil
				}
			}
		} else {
			sender.Handler = func(pkt *rtp.Packet) {
				p.Send += len(pkt.Payload)
				_ = p.client.WriteAudio(codecOPUS, pkt.Payload)
			}
		}
	}

	sender.HandleRTP(track)
	p.Senders = append(p.Senders, sender)
	return nil
}

// The cameras expect 40 ms audio packets
const speakerPacket = 40 * time.Millisecond

// Speak plays raw audio read from r on the camera speaker, until r ends. Unlike AddTrack, the audio does not need to
// come from a go2rtc stream: e.g. the body of an HTTP request.
// format: "pcma" (A-law, 8 bits) or "pcm" (16-bit little endian); rate: 8000 or 16000. Without a format: A-law
// 8 kHz, converted to PCM for the models that need it (like AddTrack).
func (p *Producer) Speak(r io.Reader, format string, rate uint32) error {
	model := p.client.SpeakerCodec()
	if model == codecOPUS {
		return errors.New("miss: this camera speaker needs Opus")
	}
	if rate == 0 {
		rate = 8000
	}

	codec := uint32(codecPCMA)
	bytesPerSample := 1
	var transcode func([]byte) []byte

	switch format {
	case "":
		if model == codecPCM {
			codec = codecPCM
			transcode = pcm.Transcode(&core.Codec{Name: core.CodecPCML, ClockRate: rate}, &core.Codec{Name: core.CodecPCMA, ClockRate: rate})
		}
	case "pcma":
	case "pcm":
		codec = codecPCM
		bytesPerSample = 2
	default:
		return fmt.Errorf("miss: unknown speaker format %q", format)
	}

	if err := p.client.StartSpeaker(); err != nil {
		return err
	}
	defer p.client.StopSpeaker()
	// Same delay as AddTrack: the speaker is not ready right after the command
	time.Sleep(time.Second)

	buf := make([]byte, int(rate)*bytesPerSample*int(speakerPacket/time.Millisecond)/1000)
	var start time.Time

	for n := 0; ; n++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}

		// The speaker plays packets as they come: audio received in bursts is paced at 40 ms per packet. After a
		// pause (microphone off), the pace starts again from now instead of sending the backlog at once.
		due := start.Add(time.Duration(n) * speakerPacket)
		if wait := time.Until(due); n == 0 || wait < -200*time.Millisecond {
			start = time.Now().Add(-time.Duration(n) * speakerPacket)
		} else if wait > 0 {
			time.Sleep(wait)
		}

		data := buf
		if transcode != nil {
			data = transcode(buf)
		}
		p.Send += len(data)
		if err := p.client.WriteAudio(codec, data); err != nil {
			return err
		}
	}
}

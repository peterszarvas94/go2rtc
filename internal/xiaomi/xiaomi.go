package xiaomi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/crypto"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/miss"
	"github.com/AlexxIT/go2rtc/pkg/xiaomi/miss/cs2"
)

func Init() {
	var v struct {
		Cfg map[string]string `yaml:"xiaomi"`
	}
	app.LoadConfig(&v)

	tokens = v.Cfg

	log := app.GetLogger("xiaomi")

	streams.HandleFunc("xiaomi", func(rawURL string) (core.Producer, error) {
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, err
		}

		if u.User != nil {
			rawURL, err = getCameraURL(u)
			if err != nil {
				return nil, err
			}
		}

		log.Debug().Msgf("xiaomi: dial %s", rawURL)

		return xiaomi.Dial(rawURL)
	})

	// Every message the camera sends on the command channel: replies to our commands, and possibly
	// unsolicited events like motion alarms. Only in debug mode.
	miss.Log = func(cmd uint32, data []byte) {
		log.Debug().Msgf("xiaomi: camera message cmd=%#x len=%d text=%q hex=%s", cmd, len(data), printableText(data), shortHex(data))
	}

	// Data on the channels go2rtc does not use, and unknown message types
	cs2.Log = func(format string, args ...any) {
		log.Warn().Msgf("xiaomi: "+format, args...)
	}

	api.HandleFunc("api/xiaomi", apiXiaomi)
	api.HandleFunc("api/xiaomi/command", apiCommand)
	api.HandleFunc("api/xiaomi/miot", apiMiot)
	api.HandleFunc("api/xiaomi/watch", apiWatch)
	api.HandleFunc("api/xiaomi/motion", apiMotion)
	api.HandleFunc("api/xiaomi/events", apiEvents)
	api.HandleFunc("api/xiaomi/image", apiEventImage)
	api.HandleFunc("api/xiaomi/speaker", apiSpeaker)
}

type miotParam struct {
	Did   string          `json:"did"`
	Siid  int             `json:"siid"`
	Piid  int             `json:"piid"`
	Value json.RawMessage `json:"value,omitempty"`
}

// apiMiot reads or writes MIoT properties of the camera behind a stream, through the Xiaomi cloud.
// Only the device of the stream can be reached:
// GET  /api/xiaomi/miot?src=camera1&props=2.1,3.1
// POST /api/xiaomi/miot?src=camera1&siid=2&piid=1&value=false
func apiMiot(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	stream := streams.Get(query.Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	var u *url.URL
	for _, source := range stream.Sources() {
		if strings.HasPrefix(source, "xiaomi://") {
			u, _ = url.Parse(source)
			break
		}
	}
	if u == nil || u.User == nil {
		http.Error(w, "not a Xiaomi cloud stream", http.StatusBadRequest)
		return
	}
	did := u.Query().Get("did")

	var apiURL string
	var params []miotParam

	switch r.Method {
	case "GET":
		apiURL = "/miotspec/prop/get"
		for _, prop := range strings.Split(query.Get("props"), ",") {
			var siid, piid int
			if _, err := fmt.Sscanf(prop, "%d.%d", &siid, &piid); err != nil {
				http.Error(w, "wrong props", http.StatusBadRequest)
				return
			}
			params = append(params, miotParam{Did: did, Siid: siid, Piid: piid})
		}
	case "POST":
		apiURL = "/miotspec/prop/set"
		siid, err1 := strconv.Atoi(query.Get("siid"))
		piid, err2 := strconv.Atoi(query.Get("piid"))
		value := json.RawMessage(query.Get("value"))
		if err1 != nil || err2 != nil || !json.Valid(value) {
			http.Error(w, "wrong siid, piid or value", http.StatusBadRequest)
			return
		}
		params = []miotParam{{Did: did, Siid: siid, Piid: piid, Value: value}}
	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	body, err := json.Marshal(map[string]any{"params": params})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := cloudUserRequest(u.User, apiURL, string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", api.MimeJSON)
	_, _ = w.Write(res)
}

// printableText keeps the readable part of a message (most are JSON), shortHex shows the raw start of it.
func printableText(data []byte) string {
	text := make([]rune, 0, len(data))
	for _, b := range data {
		if b >= 0x20 && b < 0x7f {
			text = append(text, rune(b))
		}
	}
	return string(text)
}

func shortHex(data []byte) string {
	if len(data) > 48 {
		return hex.EncodeToString(data[:48]) + "..."
	}
	return hex.EncodeToString(data)
}

// apiWatch opens a control-only connection to the camera (no video) and reports what it sends on its own:
// GET /api/xiaomi/watch?src=camera1&seconds=120
func apiWatch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	stream := streams.Get(query.Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	var u *url.URL
	for _, source := range stream.Sources() {
		if strings.HasPrefix(source, "xiaomi://") {
			u, _ = url.Parse(source)
			break
		}
	}
	if u == nil || u.User == nil {
		http.Error(w, "not a Xiaomi cloud stream", http.StatusBadRequest)
		return
	}

	seconds, _ := strconv.Atoi(query.Get("seconds"))
	if seconds <= 0 || seconds > 600 {
		seconds = 120
	}

	rawURL, err := getCameraURL(u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	client, err := miss.NewClient(rawURL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	type message struct {
		Time string `json:"time"`
		Text string `json:"text"`
		Hex  string `json:"hex"`
	}
	messages := make([]message, 0, 16)
	deadline := time.After(time.Duration(seconds) * time.Second)

	for {
		select {
		case data := <-client.Replies():
			messages = append(messages, message{
				Time: time.Now().Format("15:04:05"),
				Text: printableText(data),
				Hex:  shortHex(data),
			})
		case <-deadline:
			api.ResponseJSON(w, map[string]any{"seconds": seconds, "count": len(messages), "messages": messages})
			return
		case <-r.Context().Done():
			return
		}
	}
}

// cameraBaseURL is the Xiaomi host that keeps the camera events and clips.
func cameraBaseURL(region string) string {
	host := "business.smartcamera.api.io.mi.com"
	if region != "" && region != "cn" {
		host = region + "." + host
	}
	return "https://" + host
}

// apiEvents lists the events the camera uploaded to the Xiaomi cloud (read only), the same list the Mi Home app
// shows: GET /api/xiaomi/events?src=camera1&minutes=60&limit=10
func apiEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	stream := streams.Get(query.Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	u := xiaomiSource(stream)
	if u == nil || u.User == nil {
		http.Error(w, "not a Xiaomi cloud stream", http.StatusBadRequest)
		return
	}

	minutes, _ := strconv.Atoi(query.Get("minutes"))
	if minutes <= 0 {
		minutes = 60
	}
	limit, _ := strconv.Atoi(query.Get("limit"))
	if limit <= 0 {
		limit = 10
	}

	cloud, err := getCloud(u.User.Username())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	region, _ := u.User.Password()
	source := u.Query()
	end := time.Now().UnixMilli()
	begin := end - int64(minutes)*60_000

	params := fmt.Sprintf(
		`{"did":"%s","model":"%s","doorBell":false,"eventType":"Default","needMerge":true,"sortType":"DESC","region":"%s","language":"en","beginTime":%d,"endTime":%d,"limit":%d}`,
		source.Get("did"), source.Get("model"), strings.ToUpper(region), begin, end, limit,
	)

	res, err := cloud.RequestGet(cameraBaseURL(region), "/common/app/get/eventlist", params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", api.MimeJSON)
	_, _ = w.Write(res)
}

// processorBaseURL is the Xiaomi host that serves the event thumbnails.
func processorBaseURL(region string) string {
	host := "processor.smartcamera.api.io.mi.com"
	if region != "" && region != "cn" {
		host = region + "." + host
	}
	return "https://" + host
}

// apiEventImage returns, as a JPEG, the thumbnail the camera took with an event of the event list (its fileId and
// imgStoreId): GET /api/xiaomi/image?src=camera1&fileId=123&stoId=CAMERA_IMG_...
func apiEventImage(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	stream := streams.Get(query.Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	u := xiaomiSource(stream)
	if u == nil || u.User == nil {
		http.Error(w, "not a Xiaomi cloud stream", http.StatusBadRequest)
		return
	}

	fileID, storeID := query.Get("fileId"), query.Get("stoId")
	if fileID == "" || storeID == "" {
		http.Error(w, "fileId and stoId are required", http.StatusBadRequest)
		return
	}

	cloud, err := getCloud(u.User.Username())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	region, _ := u.User.Password()
	image, err := cloud.RequestEventImage(processorBaseURL(region), u.Query().Get("did"), fileID, storeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "image/jpeg")
	_, _ = w.Write(image)
}

type subscriber interface {
	Subscribe() (<-chan []byte, func())
}

// xiaomiSource returns the xiaomi:// source of a stream.
func xiaomiSource(stream *streams.Stream) *url.URL {
	for _, source := range stream.Sources() {
		if strings.HasPrefix(source, "xiaomi://") {
			u, _ := url.Parse(source)
			return u
		}
	}
	return nil
}

// apiMotion streams what the camera says, as Server-Sent Events. A camera with motion tracking enabled sends
// a motor message on its own when it turns to follow a movement:
// GET /api/xiaomi/motion?src=camera1
func apiMotion(w http.ResponseWriter, r *http.Request) {
	stream := streams.Get(r.URL.Query().Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	// Reuse the connection of a playing stream, so the camera does not get a second session
	var shared subscriber
	for _, conn := range stream.Conns() {
		if sub, ok := conn.(subscriber); ok {
			shared = sub
			break
		}
	}

	var messages <-chan []byte
	var unsubscribe func()

	if shared != nil {
		messages, unsubscribe = shared.Subscribe()
	} else {
		u := xiaomiSource(stream)
		if u == nil || u.User == nil {
			http.Error(w, "not a Xiaomi cloud stream", http.StatusBadRequest)
			return
		}
		rawURL, err := getCameraURL(u)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		client, err := miss.NewClient(rawURL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer client.Close()
		messages, unsubscribe = client.Subscribe()
	}
	defer unsubscribe()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = fmt.Fprintf(w, "event: ready\ndata: {\"shared\":%t}\n\n", shared != nil)
	flusher.Flush()

	// A shared connection dies with its stream, and a kept alive one needs regular pings
	check := time.NewTicker(15 * time.Second)
	defer check.Stop()

	for {
		select {
		case data := <-messages:
			event, _ := json.Marshal(map[string]string{"text": printableText(data), "hex": shortHex(data)})
			_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", event)
			flusher.Flush()

		case <-check.C:
			if shared != nil && !stillConnected(stream, shared) {
				return // the stream stopped: the client reconnects and gets its own connection
			}
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()

		case <-r.Context().Done():
			return
		}
	}
}

func stillConnected(stream *streams.Stream, shared subscriber) bool {
	for _, conn := range stream.Conns() {
		if sub, ok := conn.(subscriber); ok && sub == shared {
			return true
		}
	}
	return false
}

type commandSender interface {
	SendCommand(cmd uint32, payload []byte) ([]byte, error)
}

// apiCommand sends a raw MISS command to a playing Xiaomi stream, e.g. PTZ:
// POST /api/xiaomi/command?src=camera1&cmd=0x112&data={"operation":1}
func apiCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	stream := streams.Get(query.Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	cmd, err := strconv.ParseUint(query.Get("cmd"), 0, 32)
	if err != nil {
		http.Error(w, "wrong cmd", http.StatusBadRequest)
		return
	}

	for _, conn := range stream.Conns() {
		sender, ok := conn.(commandSender)
		if !ok {
			continue
		}

		reply, err := sender.SendCommand(uint32(cmd), []byte(query.Get("data")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		api.ResponseJSON(w, map[string]string{
			"reply": string(reply),
			"hex":   hex.EncodeToString(reply),
		})
		return
	}

	http.Error(w, "no Xiaomi camera is playing on this stream", http.StatusConflict)
}

type speaker interface {
	Speak(r io.Reader, format string, rate uint32) error
}

// apiSpeaker plays the request body on the speaker of the Xiaomi camera of a playing stream (two-way audio): raw
// mono audio, sent as it is produced. format=pcma (A-law, default) or pcm (16-bit little endian), rate=8000 (default)
// or 16000: POST /api/xiaomi/speaker?src=camera1&format=pcma&rate=8000
func apiSpeaker(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	stream := streams.Get(query.Get("src"))
	if stream == nil {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	var rate uint32
	switch query.Get("rate") {
	case "", "8000":
		rate = 8000
	case "16000":
		rate = 16000
	default:
		http.Error(w, "rate must be 8000 or 16000", http.StatusBadRequest)
		return
	}

	for _, conn := range stream.Conns() {
		if s, ok := conn.(speaker); ok {
			if err := s.Speak(r.Body, query.Get("format"), rate); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
	}

	http.Error(w, "no Xiaomi camera is playing on this stream", http.StatusConflict)
}

var tokens map[string]string
var clouds map[string]*xiaomi.Cloud
var cloudsMu sync.Mutex

func getCloud(userID string) (*xiaomi.Cloud, error) {
	cloudsMu.Lock()
	defer cloudsMu.Unlock()

	if cloud := clouds[userID]; cloud != nil {
		return cloud, nil
	}

	cloud := xiaomi.NewCloud(AppXiaomiHome)
	if err := cloud.LoginWithToken(userID, tokens[userID]); err != nil {
		return nil, err
	}
	if clouds == nil {
		clouds = map[string]*xiaomi.Cloud{userID: cloud}
	} else {
		clouds[userID] = cloud
	}
	return cloud, nil
}

func cloudRequest(userID, region, apiURL, params string) ([]byte, error) {
	cloud, err := getCloud(userID)
	if err != nil {
		return nil, err
	}
	return cloud.Request(GetBaseURL(region), apiURL, params, nil)
}

func cloudUserRequest(user *url.Userinfo, apiURL, params string) ([]byte, error) {
	userID := user.Username()
	region, _ := user.Password()
	return cloudRequest(userID, region, apiURL, params)
}

func getCameraURL(url *url.URL) (string, error) {
	model := url.Query().Get("model")

	// It is not known which models need to be awakened.
	// Probably all the doorbells and all the battery cameras.
	if strings.Contains(model, ".cateye.") {
		_ = wakeUpCamera(url)
	}

	// The getMissURL request has a fallback to getP2PURL.
	// But for known models we can save one request to the cloud.
	if xiaomi.IsLegacy(model) {
		return getLegacyURL(url)
	}
	return getMissURL(url)
}

func getLegacyURL(url *url.URL) (string, error) {
	query := url.Query()

	clientPublic, clientPrivate, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}

	params := fmt.Sprintf(`{"did":"%s","toSignAppData":"%x"}`, query.Get("did"), clientPublic)

	userID := url.User.Username()
	region, _ := url.User.Password()
	res, err := cloudRequest(userID, region, "/device/devicepass", params)
	if err != nil {
		return "", err
	}

	var v struct {
		UID       string `json:"p2p_id"`
		Password  string `json:"password"`
		PublicKey string `json:"p2p_dev_public_key"`
		Sign      string `json:"signForAppData"`
	}
	if err = json.Unmarshal(res, &v); err != nil {
		return "", err
	}

	query.Set("uid", v.UID)

	if v.Sign != "" {
		query.Set("client_public", hex.EncodeToString(clientPublic))
		query.Set("client_private", hex.EncodeToString(clientPrivate))
		query.Set("device_public", v.PublicKey)
		query.Set("sign", v.Sign)
	} else {
		query.Set("password", v.Password)
	}

	url.RawQuery = query.Encode()
	return url.String(), nil
}

func getMissURL(url *url.URL) (string, error) {
	clientPublic, clientPrivate, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}

	query := url.Query()
	params := fmt.Sprintf(
		`{"app_pubkey":"%x","did":"%s","support_vendors":"TUTK_CS2_MTP"}`,
		clientPublic, query.Get("did"),
	)

	res, err := cloudUserRequest(url.User, "/v2/device/miss_get_vendor", params)
	if err != nil {
		if strings.Contains(err.Error(), "no available vendor support") {
			return getLegacyURL(url)
		}
		return "", err
	}

	var v struct {
		Vendor struct {
			ID     byte `json:"vendor"`
			Params struct {
				UID string `json:"p2p_id"`
			} `json:"vendor_params"`
		} `json:"vendor"`
		PublicKey string `json:"public_key"`
		Sign      string `json:"sign"`
	}
	if err = json.Unmarshal(res, &v); err != nil {
		return "", err
	}

	query.Set("client_public", hex.EncodeToString(clientPublic))
	query.Set("client_private", hex.EncodeToString(clientPrivate))
	query.Set("device_public", v.PublicKey)
	query.Set("sign", v.Sign)
	query.Set("vendor", getVendorName(v.Vendor.ID))

	if v.Vendor.ID == 1 {
		query.Set("uid", v.Vendor.Params.UID)
	}

	url.RawQuery = query.Encode()
	return url.String(), nil
}

func getVendorName(i byte) string {
	switch i {
	case 1:
		return "tutk"
	case 3:
		return "agora"
	case 4:
		return "cs2"
	case 6:
		return "mtp"
	}
	return fmt.Sprintf("%d", i)
}

func wakeUpCamera(url *url.URL) error {
	const params = `{"id":1,"method":"wakeup","params":{"video":"1"}}`
	did := url.Query().Get("did")
	_, err := cloudUserRequest(url.User, "/home/rpc/"+did, params)
	return err
}

func apiXiaomi(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		apiDeviceList(w, r)
	case "POST":
		apiAuth(w, r)
	}
}

func apiDeviceList(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	user := query.Get("id")
	if user == "" {
		cloudsMu.Lock()
		users := make([]string, 0, len(tokens))
		for s := range tokens {
			users = append(users, s)
		}
		cloudsMu.Unlock()

		api.ResponseJSON(w, users)
		return
	}

	err := func() error {
		region := query.Get("region")
		res, err := cloudRequest(user, region, "/v2/home/device_list_page", "{}")
		if err != nil {
			return err
		}
		var v struct {
			List []*Device `json:"list"`
		}

		if err = json.Unmarshal(res, &v); err != nil {
			return err
		}

		var items []*api.Source

		for _, device := range v.List {
			if !strings.Contains(device.Model, ".camera.") && !strings.Contains(device.Model, ".cateye.") {
				continue
			}
			items = append(items, &api.Source{
				Name: device.Name,
				Info: fmt.Sprintf("ip: %s, mac: %s", device.IP, device.MAC),
				URL:  fmt.Sprintf("xiaomi://%s:%s@%s?did=%s&model=%s", user, region, device.IP, device.Did, device.Model),
			})
		}

		api.ResponseSources(w, items)
		return nil
	}()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type Device struct {
	Did   string `json:"did"`
	Name  string `json:"name"`
	Model string `json:"model"`
	MAC   string `json:"mac"`
	IP    string `json:"localip"`
}

var auth *xiaomi.Cloud

func apiAuth(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	username := r.Form.Get("username")
	password := r.Form.Get("password")
	captcha := r.Form.Get("captcha")
	verify := r.Form.Get("verify")

	var err error

	switch {
	case username != "" || password != "":
		auth = xiaomi.NewCloud(AppXiaomiHome)
		err = auth.Login(username, password)
	case captcha != "":
		err = auth.LoginWithCaptcha(captcha)
	case verify != "":
		err = auth.LoginWithVerify(verify)
	default:
		http.Error(w, "wrong request", http.StatusBadRequest)
		return
	}

	if err == nil {
		userID, token := auth.UserToken()
		auth = nil

		cloudsMu.Lock()
		if tokens == nil {
			tokens = map[string]string{userID: token}
		} else {
			tokens[userID] = token
		}
		cloudsMu.Unlock()

		err = app.PatchConfig([]string{"xiaomi", userID}, token)
	}

	if err != nil {
		var login *xiaomi.LoginError
		if errors.As(err, &login) {
			w.Header().Set("Content-Type", api.MimeJSON)
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(err)
			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

const AppXiaomiHome = "xiaomiio"

func GetBaseURL(region string) string {
	switch region {
	case "de", "i2", "ru", "sg", "us":
		return "https://" + region + ".api.io.mi.com/app"
	}
	return "https://api.io.mi.com/app"
}

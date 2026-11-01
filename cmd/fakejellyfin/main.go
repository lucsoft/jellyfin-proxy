// fakejellyfin plays a Jellyfin server for local tests without a real client:
// one MKV file, PlaybackInfo and range downloads, throttled per
// connection like the real server (~18 Mbit/s per connection there).
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	file := flag.String("file", "", "MKV file")
	listen := flag.String("listen", "127.0.0.1:8096", "listen address")
	mbit := flag.Float64("mbit", 18, "Mbit/s per connection (0 = unthrottled)")
	flag.Parse()
	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	st, _ := f.Stat()

	http.HandleFunc("POST /Items/{item}/PlaybackInfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"MediaSources":[{"Id":"demo","Name":"Demo movie","Container":"mkv","Size":%d,
			"SupportsDirectPlay":true,"DefaultAudioStreamIndex":1,"MediaStreams":[
			{"Index":0,"Type":"Video","Codec":"h264"},
			{"Index":1,"Type":"Audio","Codec":"aac","DisplayTitle":"AAC Stereo","Channels":2}]}]}`, st.Size())
	})
	http.HandleFunc("GET /Videos/{item}/stream", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "film.mkv", time.Time{}, throttled{io.NewSectionReader(f, 0, st.Size()), *mbit})
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	log.Printf("fakejellyfin %s on %s, %.0f Mbit/s per connection", *file, *listen, *mbit)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

// throttled slows a connection's reads down to mbit Mbit/s.
type throttled struct {
	*io.SectionReader
	mbit float64
}

func (t throttled) Read(p []byte) (int, error) {
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	n, err := t.SectionReader.Read(p)
	if t.mbit > 0 {
		time.Sleep(time.Duration(float64(n) * 8 / (t.mbit * 1e6) * float64(time.Second)))
	}
	return n, err
}

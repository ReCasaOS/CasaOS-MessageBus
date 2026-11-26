package route

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocHandler(t *testing.T) {
	h := docHandler("/doc/x", "/doc/x/openapi.yaml", "openapi: 3.0.0\n")
	for _, p := range []string{"/doc/x", "/doc/x/", "/doc/x/openapi.yaml"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusOK || rec.Body.String() != "openapi: 3.0.0\n" {
			t.Fatalf("%s: %d %q", p, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("Content-Type") != "application/yaml" {
			t.Fatalf("%s: headers %v", p, rec.Header())
		}
	}
	for _, p := range []string{"/doc/x/other", "/doc/x/index.html", "/doc/xy"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: %d", p, rec.Code)
		}
	}
}

// No file under api/ (embedded by main.go) may load remote code. (Plain
// cdn.jsdelivr.net/gh icon URLs in spec examples are data, not loaded.)
func TestEmbeddedAPIFilesLoadNothingRemote(t *testing.T) {
	bad := []string{`<script src="http`, `<link`, "cdn.jsdelivr.net/npm", "unpkg.com"}
	err := filepath.WalkDir("../api", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if strings.HasSuffix(p, ".html") {
			t.Errorf("%s: HTML page must not exist", p)
		}
		for _, s := range bad {
			if strings.Contains(string(b), s) {
				t.Errorf("%s contains %q", p, s)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

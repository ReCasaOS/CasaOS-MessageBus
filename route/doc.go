package route

import "net/http"

// docHandler serves the API description: the YAML at base, base+"/" and
// yamlPath. Anything else is a 404.
func docHandler(base, yamlPath, docYAML string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p := r.URL.Path; p != base && p != base+"/" && p != yamlPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/yaml")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if _, err := w.Write([]byte(docYAML)); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
}

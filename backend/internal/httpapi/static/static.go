package static

import (
	"fmt"
	"html"
	"net/http"
	"os"
	urlpath "path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	indexCacheMu   sync.RWMutex
	cachedIndexKey string
	cachedIndexMod time.Time
	cachedIndexVal []byte
	hashedAssetRe  = regexp.MustCompile(`-[A-Za-z0-9_-]{8,}\.[a-z0-9]+$`)
)

var panelStaticPathPrefixes = []string{
	"assets/",
	"favicons/",
	"lotties/",
	"splash_screens/",
}

var panelStaticPathFiles = map[string]struct{}{
	"site.webmanifest": {},
}

func ServeAppConfigJS(w http.ResponseWriter, basePath string) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = fmt.Fprintf(
		w,
		"window.__EXODUS_RUNTIME__={basePath:%q};\n",
		basePath,
	)
}

func getOrRenderPanelIndex(indexPath string, basePathWithSlash, basePath string) ([]byte, error) {
	info, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}

	cacheKey := indexPath + "|" + basePathWithSlash
	indexCacheMu.RLock()
	if cachedIndexKey == cacheKey && cachedIndexMod.Equal(info.ModTime()) && len(cachedIndexVal) > 0 {
		val := cachedIndexVal
		indexCacheMu.RUnlock()
		return val, nil
	}
	indexCacheMu.RUnlock()

	indexCacheMu.Lock()
	defer indexCacheMu.Unlock()
	if cachedIndexKey == cacheKey && cachedIndexMod.Equal(info.ModTime()) && len(cachedIndexVal) > 0 {
		return cachedIndexVal, nil
	}

	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}

	rendered := RenderPanelIndex(string(indexBytes), basePathWithSlash, basePath)
	renderedBytes := []byte(rendered)

	cachedIndexKey = cacheKey
	cachedIndexMod = info.ModTime()
	cachedIndexVal = renderedBytes

	return renderedBytes, nil
}

func ServePanelIndex(w http.ResponseWriter, indexPath string, basePathWithSlash, basePath string) {
	pageBytes, err := getOrRenderPanelIndex(indexPath, basePathWithSlash, basePath)
	if err != nil {
		http.Error(w, "panel index not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(pageBytes)
}

func RenderPanelIndex(page string, basePathWithSlash, basePath string) string {
	injected := fmt.Sprintf(
		"<base href=\"%s\" />\n<script>window.__EXODUS_RUNTIME__={basePath:%q};</script>",
		basePathWithSlash,
		basePath,
	)
	if strings.Contains(page, "<head>") {
		page = strings.Replace(page, "<head>", "<head>\n"+injected, 1)
	} else if strings.Contains(page, "<HEAD>") {
		page = strings.Replace(page, "<HEAD>", "<HEAD>\n"+injected, 1)
	} else {
		page = injected + page
	}

	basePrefix := strings.TrimSuffix(basePathWithSlash, "/")

	page = strings.ReplaceAll(page, "%BASE_URL%", basePrefix)

	if basePrefix != "" {
		for _, prefix := range panelStaticPathPrefixes {
			page = strings.ReplaceAll(page, `"/`+prefix, `"`+basePrefix+`/`+prefix)
		}
		for file := range panelStaticPathFiles {
			page = strings.ReplaceAll(page, `"/`+file+`"`, `"`+basePrefix+`/`+file+`"`)
		}
	}

	return page
}

func ServeStatic(w http.ResponseWriter, r *http.Request, staticDir, basePath string) {
	if staticDir == "" {
		http.Error(w, "static assets disabled", http.StatusNotFound)
		return
	}

	indexPath := filepath.Join(staticDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		http.Error(w, "panel static assets not found", http.StatusNotFound)
		return
	}

	basePath = strings.TrimSuffix(basePath, "/")
	if basePath == "" {
		basePath = "/"
	}

	basePathWithSlash := basePath
	if !strings.HasSuffix(basePathWithSlash, "/") {
		basePathWithSlash += "/"
	}

	requestPath := r.URL.Path

	if basePath != "/" {
		if !strings.HasPrefix(requestPath, basePathWithSlash) && requestPath != basePath {
			http.NotFound(w, r)
			return
		}
		requestPath = strings.TrimPrefix(requestPath, basePath)
		if requestPath == "" {
			requestPath = "/"
		}
	}

	if requestPath == "/app-config.js" {
		ServeAppConfigJS(w, basePath)
		return
	}

	cleanPath := urlpath.Clean(requestPath)
	if cleanPath == "/" || cleanPath == "." {
		ServePanelIndex(w, indexPath, basePathWithSlash, basePath)
		return
	}

	relPath := strings.TrimPrefix(cleanPath, "/")
	targetPath := filepath.Join(staticDir, filepath.FromSlash(relPath))

	targetClean := filepath.Clean(targetPath)
	staticClean := filepath.Clean(staticDir)
	if !strings.HasPrefix(targetClean, staticClean) {
		http.Error(w, "access denied", http.StatusForbidden)
		return
	}

	info, err := os.Stat(targetClean)
	if err == nil && !info.IsDir() {
		if IsHashedAsset(relPath) {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		http.ServeFile(w, r, targetClean)
		return
	}

	if isPanelStaticAssetPath(relPath) {
		http.Error(
			w,
			fmt.Sprintf("static asset not found: %s", html.EscapeString(relPath)),
			http.StatusNotFound,
		)
		return
	}

	ServePanelIndex(w, indexPath, basePathWithSlash, basePath)
}

func IsHashedAsset(relPath string) bool {
	if strings.HasPrefix(relPath, "assets/") || strings.HasPrefix(relPath, "splash_screens/") {
		return true
	}
	return hashedAssetRe.MatchString(relPath)
}

func isPanelStaticAssetPath(relPath string) bool {
	for _, prefix := range panelStaticPathPrefixes {
		if strings.HasPrefix(relPath, prefix) {
			return true
		}
	}
	_, ok := panelStaticPathFiles[relPath]
	return ok
}

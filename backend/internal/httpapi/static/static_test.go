package static

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderPanelIndexPrefixesStaticAssetsWithBasePath(t *testing.T) {
	input := `<html><head>
<script src="/assets/index.js"></script>
<link href="/assets/index.css" rel="stylesheet">
<link rel="icon" href="/favicons/logo.svg">
<link rel="manifest" href="/site.webmanifest">
<link rel="apple-touch-startup-image" href="%BASE_URL%/splash_screens/start.png">
</head><body></body></html>`

	page := RenderPanelIndex(input, "/panel/", "/panel")

	expected := []string{
		`<base href="/panel/" />`,
		`basePath:"/panel"`,
		`src="/panel/assets/index.js"`,
		`href="/panel/assets/index.css"`,
		`href="/panel/favicons/logo.svg"`,
		`href="/panel/site.webmanifest"`,
		`href="/panel/splash_screens/start.png"`,
	}
	for _, value := range expected {
		if !strings.Contains(page, value) {
			t.Fatalf("expected rendered index to contain %q, got:\n%s", value, page)
		}
	}
}

func TestRenderPanelIndexKeepsRootStaticAssetsAtRoot(t *testing.T) {
	input := `<html><head><script src="%BASE_URL%/assets/index.js"></script></head></html>`

	page := RenderPanelIndex(input, "/", "/")

	if !strings.Contains(page, `src="/assets/index.js"`) {
		t.Fatalf("expected root asset path, got:\n%s", page)
	}
	if strings.Contains(page, `%BASE_URL%`) {
		t.Fatalf("expected %%BASE_URL%% placeholder to be removed, got:\n%s", page)
	}
}

func TestServeAppConfigJSIncludesBasePath(t *testing.T) {
	rec := httptest.NewRecorder()
	ServeAppConfigJS(rec, "/panel")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if contentType := rec.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/javascript") {
		t.Fatalf("expected javascript content type, got %q", contentType)
	}
	if body := rec.Body.String(); !strings.Contains(body, `window.__EXODUS_RUNTIME__={basePath:"/panel"};`) {
		t.Fatalf("unexpected app config body:\n%s", body)
	}
}

func TestServeStaticAppConfigJS(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panel/app-config.js", nil)

	ServeStatic(rec, req, dir, "/panel")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, `basePath:"/panel"`) {
		t.Fatalf("unexpected body:\n%s", body)
	}
}

func TestServeStaticMissingAssetReturns404(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/assets/missing.js", nil)

	ServeStatic(rec, req, dir, "/")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing asset, got %d", rec.Code)
	}
}

func TestServeStaticUnknownRouteFallsBackToSPA(t *testing.T) {
	dir := t.TempDir()
	indexContent := "<html><body>spa</body></html>"
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(indexContent), 0644); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/users/123", nil)

	ServeStatic(rec, req, dir, "/")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for SPA fallback, got %d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "spa") {
		t.Fatalf("expected SPA fallback body, got:\n%s", body)
	}
}

func TestServeStaticCustomBasePathUnknownRouteFallsBackToSPA(t *testing.T) {
	dir := t.TempDir()
	indexContent := "<html><head></head><body>spa</body></html>"
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(indexContent), 0644); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panel/users/123", nil)

	ServeStatic(rec, req, dir, "/panel")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for SPA fallback with base path, got %d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, `<base href="/panel/" />`) {
		t.Fatalf("expected base href in SPA fallback, got:\n%s", body)
	}
}

func TestServeStaticCacheControlHeaders(t *testing.T) {
	dir := t.TempDir()
	indexContent := "<html><head></head><body>index</body></html>"
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(indexContent), 0644); err != nil {
		t.Fatal(err)
	}
	assetsDir := filepath.Join(dir, "assets")
	if err := os.MkdirAll(assetsDir, 0755); err != nil {
		t.Fatal(err)
	}
	hashedJS := filepath.Join(assetsDir, "main-Bx912aBc.js")
	if err := os.WriteFile(hashedJS, []byte("console.log('hi');"), 0644); err != nil {
		t.Fatal(err)
	}
	plainFile := filepath.Join(dir, "site.webmanifest")
	if err := os.WriteFile(plainFile, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}

	// 1. Hashed asset gets immutable cache
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/assets/main-Bx912aBc.js", nil)
	ServeStatic(rec, req, dir, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Fatalf("expected immutable cache control header, got %q", cc)
	}

	// 2. Unhashed file gets no-cache
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/site.webmanifest", nil)
	ServeStatic(rec, req, dir, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("expected no-cache header, got %q", cc)
	}

	// 3. index.html gets no-cache
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	ServeStatic(rec, req, dir, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("expected no-cache header for index, got %q", cc)
	}
}

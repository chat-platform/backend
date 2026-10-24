package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const payloadHash = "UNSIGNED-PAYLOAD"

// Presigner creates S3-compatible SigV4 URLs. It supports path-style endpoints,
// which works with both AWS S3 and local MinIO.
type Presigner struct {
	endpoint string
	region   string
	bucket   string
	access   string
	secret   string
	now      func() time.Time
}

func New(endpoint, region, bucket, accessKey, secretKey string) (*Presigner, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return nil, errors.New("media endpoint must be an absolute HTTP(S) URL")
	}
	if strings.TrimSpace(region) == "" || strings.TrimSpace(bucket) == "" || strings.TrimSpace(accessKey) == "" || secretKey == "" {
		return nil, errors.New("media region, bucket, and credentials are required")
	}
	return &Presigner{endpoint: strings.TrimRight(endpoint, "/"), region: region, bucket: bucket, access: accessKey, secret: secretKey, now: time.Now}, nil
}

func (p *Presigner) SignUpload(_ context.Context, key, contentType string, size int64, ttl time.Duration) (string, map[string]string, error) {
	if size <= 0 || ttl < time.Second {
		return "", nil, errors.New("invalid media signing parameters")
	}
	url, err := p.presign("PUT", key, ttl, map[string]string{
		"content-length": fmt.Sprint(size),
		"content-type":   contentType,
	})
	if err != nil {
		return "", nil, err
	}
	return url, map[string]string{"Content-Type": contentType}, nil
}

func (p *Presigner) SignDownload(_ context.Context, key string, ttl time.Duration) (string, error) {
	if ttl < time.Second {
		return "", errors.New("invalid media signing parameters")
	}
	return p.presign("GET", key, ttl, nil)
}

func (p *Presigner) presign(method, key string, ttl time.Duration, requiredHeaders map[string]string) (string, error) {
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") || key == "" {
		return "", errors.New("invalid object key")
	}
	parsed, err := url.Parse(p.endpoint)
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(parsed.Path, "/")
	parsed.Path = basePath + "/" + p.bucket + "/" + key
	parsed.RawPath = ""
	parsed.RawQuery = ""
	now := p.now().UTC()
	date := now.Format("20060102")
	stamp := now.Format("20060102T150405Z")
	scope := date + "/" + p.region + "/s3/aws4_request"

	query := parsed.Query()
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", p.access+"/"+scope)
	query.Set("X-Amz-Date", stamp)
	query.Set("X-Amz-Expires", fmt.Sprint(int64(ttl.Seconds())))
	headerNames := make([]string, 0, len(requiredHeaders)+1)
	canonicalHeaders := make([]string, 0, len(requiredHeaders)+1)
	headers := map[string]string{"host": parsed.Host}
	for name, value := range requiredHeaders {
		headers[strings.ToLower(name)] = strings.TrimSpace(value)
	}
	for name := range headers {
		headerNames = append(headerNames, name)
	}
	sort.Strings(headerNames)
	for _, name := range headerNames {
		canonicalHeaders = append(canonicalHeaders, name+":"+strings.Join(strings.Fields(headers[name]), " ")+"\n")
	}
	signedHeaders := strings.Join(headerNames, ";")
	query.Set("X-Amz-SignedHeaders", signedHeaders)
	canonicalQuery := canonicalQueryString(query)
	canonicalRequest := method + "\n" + awsURI(parsed) + "\n" + canonicalQuery + "\n" + strings.Join(canonicalHeaders, "") + "\n" + signedHeaders + "\n" + payloadHash
	requestHash := hashHex([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + requestHash
	signature := hex.EncodeToString(hmacSHA256(signingKey(p.secret, date, p.region), []byte(stringToSign)))
	query.Set("X-Amz-Signature", signature)
	parsed.RawQuery = canonicalQueryString(query)
	return parsed.String(), nil
}

func canonicalQueryString(query url.Values) string {
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		sort.Strings(values)
		for _, value := range values {
			parts = append(parts, awsEscape(key)+"="+awsEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func awsURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	return path
}

func awsEscape(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(value), "+", "%20"), "%7E", "~")
}

func hashHex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, value []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(value)
	return h.Sum(nil)
}

func signingKey(secret, date, region string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	regionKey := hmacSHA256(dateKey, []byte(region))
	serviceKey := hmacSHA256(regionKey, []byte("s3"))
	return hmacSHA256(serviceKey, []byte("aws4_request"))
}

var _ interface {
	SignUpload(context.Context, string, string, int64, time.Duration) (string, map[string]string, error)
	SignDownload(context.Context, string, time.Duration) (string, error)
} = (*Presigner)(nil)

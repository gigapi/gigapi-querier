package querier

import (
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"net/url"
	"strings"
)

type querierLayerDesc struct {
	layer    config.LayersConfiguration
	path     string
	hostname string
	key      string
	secret   string
	bucket   string
	secure   bool
	urlStyle string
}

func getQuerierLayerDesc(layer config.LayersConfiguration) (querierLayerDesc, error) {
	switch layer.Type {
	case "fs":
		return getQuerierLayerDescFs(layer)
	case "s3":
		return getQuerierLayerDescS3(layer)
	}
	return querierLayerDesc{}, fmt.Errorf("Unsupported layer type: %s", layer.Type)
}

func getQuerierLayerDescFs(layer config.LayersConfiguration) (querierLayerDesc, error) {
	return querierLayerDesc{
		layer: layer,
		path:  strings.TrimPrefix(layer.URL, "file://"),
	}, nil
}

func getQuerierLayerDescS3(layer config.LayersConfiguration) (querierLayerDesc, error) {
	s3Url, err := url.Parse(layer.URL)
	if err != nil {
		return querierLayerDesc{}, err
	}
	if s3Url.Scheme != "s3" {
		return querierLayerDesc{}, fmt.Errorf("Invalid S3 URL: %s", layer.URL)
	}

	pathParts := strings.SplitN(s3Url.Path, "/", 2)

	res := querierLayerDesc{
		layer:    layer,
		hostname: s3Url.Host,
		bucket:   pathParts[0],
		path:     pathParts[1],
		secure:   s3Url.Query().Get("secure") != "false",
		key:      layer.Auth.Key,
		secret:   layer.Auth.Secret,
	}
	if s3Url.User != nil {
		res.key = s3Url.User.Username()
		res.secret, _ = s3Url.User.Password()
	}
	res.urlStyle = "vhost"
	if s3Url.Query().Get("url-style") == "path" {
		res.urlStyle = "path"
	}
	return res, nil
}

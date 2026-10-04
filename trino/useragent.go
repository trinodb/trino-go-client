package trino

import (
	"fmt"
	"regexp"
	"runtime"
	"runtime/debug"
)

const (
	driverModulePath = "github.com/trinodb/trino-go-client"
	userAgentProduct = "trino-go-client"
	unknownVersion   = "unknown"
	userAgentHeader  = "User-Agent"
)

var userAgentSanitizePattern = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

// userAgent follows the format of the Java client's UserAgentBuilder:
// product/version followed by key=value pairs describing the platform.
var userAgent = createUserAgent(driverVersion(debug.ReadBuildInfo()))

func createUserAgent(version string) string {
	return fmt.Sprintf("%s/%s os=%s arch=%s lang/go=%s",
		userAgentProduct,
		version,
		sanitizeUserAgentValue(runtime.GOOS),
		sanitizeUserAgentValue(runtime.GOARCH),
		sanitizeUserAgentValue(runtime.Version()))
}

// driverVersion finds the version of this module in the binary's build
// information. It is unknown when the binary was built without module
// support, or from a checkout of this module, where Go reports (devel).
func driverVersion(info *debug.BuildInfo, ok bool) string {
	if !ok {
		return unknownVersion
	}
	if info.Main.Path == driverModulePath {
		return moduleVersion(&info.Main)
	}
	for _, dep := range info.Deps {
		if dep.Path == driverModulePath {
			return moduleVersion(dep)
		}
	}
	return unknownVersion
}

func moduleVersion(module *debug.Module) string {
	if module.Replace != nil {
		module = module.Replace
	}
	if module.Version == "" || module.Version == "(devel)" {
		return unknownVersion
	}
	return module.Version
}

func sanitizeUserAgentValue(value string) string {
	return userAgentSanitizePattern.ReplaceAllString(value, "_")
}

package kube

import "strings"

func dedotMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return in
	}
	ret := make(map[string]string, len(in))
	for key, value := range in {
		nKey := strings.ReplaceAll(key, ".", "_")
		ret[nKey] = value
	}
	return ret
}

package datastructs

import "github.com/Hamster601/Budd/pkg/stringutil"

func Contain(array []string, v interface{}) bool {
	vvv := stringutil.ToString(v)
	for _, vv := range array {
		if vv == vvv {
			return true
		}
	}
	return false
}

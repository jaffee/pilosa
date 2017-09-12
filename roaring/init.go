package roaring

import (
	"fmt"
	"unsafe"
)

func init() {
	fmt.Printf("sizeof container: %v\n", unsafe.Sizeof(container{}))
}

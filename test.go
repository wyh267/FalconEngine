package main

import (
	"encoding/binary"
	"fmt"
)

func main()  {

	bytes := make([]byte,binary.MaxVarintLen64)
	n:=binary.PutUvarint(bytes,399)
	fmt.Println(n)

}

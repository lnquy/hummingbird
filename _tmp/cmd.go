package main

import "github.com/lnquy/hummingbird/dashtable"

func main() {
	dtb := dashtable.New[int, string](100)
	_ = dtb
}

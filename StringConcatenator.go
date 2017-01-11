package main

type StringConcatenator struct {
	fragments []string
	totalSize int
}

func (this *StringConcatenator) Add(str string) {
	this.fragments = append(this.fragments, str)
	this.totalSize += len(str)
}

func (this *StringConcatenator) Concat() string {
	return string(this.ConcatToBytes())
}

func (this *StringConcatenator) ConcatToBytes() []byte {
	result := make([]byte, this.totalSize)
	offset := 0

	for _, fragment := range this.fragments {
		copy(result[offset:], fragment)
		offset += len(fragment)
	}

	return result
}
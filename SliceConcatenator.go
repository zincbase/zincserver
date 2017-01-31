package main

func ConcatSliceList(sliceList [][]byte) (result []byte) {
	concatenator := NewSliceConcatenatorFromSliceList(sliceList)
	return concatenator.Concat()
}

func ConcatSlices(buffers ...[]byte) (result []byte) {
	return ConcatSliceList(buffers)
}

type SliceConcatenator struct {
	sliceList   [][]byte
	TotalLength int64
}

func (this *SliceConcatenator) Append(slice []byte) {
	this.sliceList = append(this.sliceList, slice)
	this.TotalLength += int64(len(slice))
}

func (this *SliceConcatenator) AppendString(str string) {
	this.Append([]byte(str))
}

func (this *SliceConcatenator) ConcatToString() string {
	return string(this.Concat())
}

func (this *SliceConcatenator) Concat() (result []byte) {
	result = make([]byte, this.TotalLength)

	offset := 0
	for _, slice := range this.sliceList {
		copy(result[offset:], slice)
		offset += len(slice)
	}

	return
}

func NewSliceConcatenator() *SliceConcatenator {
	return &SliceConcatenator{
		sliceList:   [][]byte{},
		TotalLength: 0,
	}
}

func NewSliceConcatenatorFromSliceList(sliceList [][]byte) *SliceConcatenator {
	totalLength := 0

	for _, slice := range sliceList {
		totalLength += len(slice)
	}

	return &SliceConcatenator{
		sliceList:   sliceList,
		TotalLength: int64(totalLength),
	}
}

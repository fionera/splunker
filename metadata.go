package main

type RawdataMetaKeyItemType struct {
	representation  int
	extraIntsNeeded int
}

var (
	rmkiTypeString                  = RawdataMetaKeyItemType{0, 1}
	rmkiTypeFloat32                 = RawdataMetaKeyItemType{2, 1}
	rmkiTypeFloat32Sigfigs          = RawdataMetaKeyItemType{3, 2}
	rmkiTypeOffsetLen               = RawdataMetaKeyItemType{4, 2}
	rmkiTypeFloat32Precision        = RawdataMetaKeyItemType{6, 2}
	rmkiTypeFloat32SigfigsPrecision = RawdataMetaKeyItemType{7, 3}
	rmkiTypeUnsigned                = RawdataMetaKeyItemType{8, 1}
	rmkiTypeSigned                  = RawdataMetaKeyItemType{9, 1}
	rmkiTypeFloat64                 = RawdataMetaKeyItemType{10, 1}
	rmkiTypeFloat64Sigfigs          = RawdataMetaKeyItemType{11, 2}
	rmkiTypeOffsetLenWencoding      = RawdataMetaKeyItemType{12, 3}
	rmkiTypeFloat64Precision        = RawdataMetaKeyItemType{14, 2}
	rmkiTypeFloat64SigfigsPrecision = RawdataMetaKeyItemType{15, 0}

	valuesInOrder = []RawdataMetaKeyItemType{
		rmkiTypeString, {}, rmkiTypeFloat32, rmkiTypeFloat32Sigfigs, rmkiTypeOffsetLen, {}, rmkiTypeFloat32Precision, rmkiTypeFloat32SigfigsPrecision, rmkiTypeUnsigned, rmkiTypeSigned,
		rmkiTypeFloat64, rmkiTypeFloat64Sigfigs, rmkiTypeOffsetLenWencoding, {}, rmkiTypeFloat64Precision, rmkiTypeFloat64SigfigsPrecision}
)

func getTypeFromCombined(v int64) RawdataMetaKeyItemType {
	return valuesInOrder[int(v&0xF)]
}

func getCodeFromCombined(v int64) int {
	return int(v >> 4)
}

func (r RawdataMetaKeyItemType) isFloatType() bool {
	return (r.representation & 0x2) != 0
}

func readMetadata(r *CountedReader, o Opcode) error {
	metaKey, err := readVariableWidthLong(r)
	if err != nil {
		return err
	}
	numToRead := -1

	if o <= 2 {
		metaKey <<= 3
		//TODO: Add metaKey
		numToRead = 1
	} else {
		if o < 36 {
			metaKey <<= 2
		}
		//TODO Add metaKey

		t := getTypeFromCombined(metaKey)
		numToRead = t.extraIntsNeeded
	}

	for i := 0; i < numToRead; i++ {
		long, err := readVariableWidthSignedLong(r)
		if err != nil {
			return err
		}
		//TODO add long
		_ = long
	}
	return nil
}

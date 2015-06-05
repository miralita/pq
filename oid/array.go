package oid

func IsArrayType(oid Oid) bool {
	_, exists := arrayTypes[oid]
	return exists
}

func GetArrayType(oid Oid) Oid {
	return arrayTypes[oid]
}

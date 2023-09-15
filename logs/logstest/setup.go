package logstest

//go:generate go run go.uber.org/mock/mockgen@latest -source ../logs.go -destination ./mock.gen.go -package logstest -typed RecordStore

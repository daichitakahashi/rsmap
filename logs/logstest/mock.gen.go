// Code generated by MockGen. DO NOT EDIT.
// Source: ../logs.go

// Package logstest is a generated GoMock package.
package logstest

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockResourceRecordStore is a mock of ResourceRecordStore interface.
type MockResourceRecordStore[T any] struct {
	ctrl     *gomock.Controller
	recorder *MockResourceRecordStoreMockRecorder[T]
}

// MockResourceRecordStoreMockRecorder is the mock recorder for MockResourceRecordStore.
type MockResourceRecordStoreMockRecorder[T any] struct {
	mock *MockResourceRecordStore[T]
}

// NewMockResourceRecordStore creates a new mock instance.
func NewMockResourceRecordStore[T any](ctrl *gomock.Controller) *MockResourceRecordStore[T] {
	mock := &MockResourceRecordStore[T]{ctrl: ctrl}
	mock.recorder = &MockResourceRecordStoreMockRecorder[T]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockResourceRecordStore[T]) EXPECT() *MockResourceRecordStoreMockRecorder[T] {
	return m.recorder
}

// ForEach mocks base method.
func (m *MockResourceRecordStore[T]) ForEach(fn func(string, *T) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ForEach", fn)
	ret0, _ := ret[0].(error)
	return ret0
}

// ForEach indicates an expected call of ForEach.
func (mr *MockResourceRecordStoreMockRecorder[T]) ForEach(fn interface{}) *ResourceRecordStoreForEachCall[T] {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForEach", reflect.TypeOf((*MockResourceRecordStore[T])(nil).ForEach), fn)
	return &ResourceRecordStoreForEachCall[T]{Call: call}
}

// ResourceRecordStoreForEachCall wrap *gomock.Call
type ResourceRecordStoreForEachCall[T any] struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *ResourceRecordStoreForEachCall[T]) Return(arg0 error) *ResourceRecordStoreForEachCall[T] {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *ResourceRecordStoreForEachCall[T]) Do(f func(func(string, *T) error) error) *ResourceRecordStoreForEachCall[T] {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *ResourceRecordStoreForEachCall[T]) DoAndReturn(f func(func(string, *T) error) error) *ResourceRecordStoreForEachCall[T] {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Get mocks base method.
func (m *MockResourceRecordStore[T]) Get(identifier string) (*T, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", identifier)
	ret0, _ := ret[0].(*T)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockResourceRecordStoreMockRecorder[T]) Get(identifier interface{}) *ResourceRecordStoreGetCall[T] {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockResourceRecordStore[T])(nil).Get), identifier)
	return &ResourceRecordStoreGetCall[T]{Call: call}
}

// ResourceRecordStoreGetCall wrap *gomock.Call
type ResourceRecordStoreGetCall[T any] struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *ResourceRecordStoreGetCall[T]) Return(arg0 *T, arg1 error) *ResourceRecordStoreGetCall[T] {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *ResourceRecordStoreGetCall[T]) Do(f func(string) (*T, error)) *ResourceRecordStoreGetCall[T] {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *ResourceRecordStoreGetCall[T]) DoAndReturn(f func(string) (*T, error)) *ResourceRecordStoreGetCall[T] {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Set mocks base method.
func (m *MockResourceRecordStore[T]) Set(identifier string, record *T) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Set", identifier, record)
	ret0, _ := ret[0].(error)
	return ret0
}

// Set indicates an expected call of Set.
func (mr *MockResourceRecordStoreMockRecorder[T]) Set(identifier, record interface{}) *ResourceRecordStoreSetCall[T] {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Set", reflect.TypeOf((*MockResourceRecordStore[T])(nil).Set), identifier, record)
	return &ResourceRecordStoreSetCall[T]{Call: call}
}

// ResourceRecordStoreSetCall wrap *gomock.Call
type ResourceRecordStoreSetCall[T any] struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *ResourceRecordStoreSetCall[T]) Return(arg0 error) *ResourceRecordStoreSetCall[T] {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *ResourceRecordStoreSetCall[T]) Do(f func(string, *T) error) *ResourceRecordStoreSetCall[T] {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *ResourceRecordStoreSetCall[T]) DoAndReturn(f func(string, *T) error) *ResourceRecordStoreSetCall[T] {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

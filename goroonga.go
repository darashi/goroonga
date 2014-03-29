package goroonga

/*
#cgo pkg-config: groonga
#include <groonga.h>
#include <stdlib.h>
*/
import "C"
import "unsafe"
import "fmt"

type Context struct {
	ctx *C.grn_ctx
}

type Database struct {
	obj *C.grn_obj
}

func Init() error {
	if rc := C.grn_init(); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_init() failed (%d)", rc)
	}
	return nil
}

func Fin() error {
	if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_fin() failed (%d)", rc)
	}
	return nil
}

func NewContext() (*Context, error) {
	ctx := &Context{C.grn_ctx_open(0)}
	if ctx == nil {
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	return ctx, nil
}

func (c *Context) Fin() error {
	if rc := C.grn_ctx_fin(c.ctx); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ct_fin() failed")
	}
	return nil
}

func (c *Context) CreateDatabase(path string) (*Database, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	db := C.grn_db_create(c.ctx, cPath, nil)
	if db == nil {
		message := C.GoString(&c.ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_create() failed: %s", message)
	}
	return &Database{db}, nil
}

func (c *Context) OpenDatabase(path string) (*Database, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	db := C.grn_db_open(c.ctx, cPath)
	if db == nil {
		message := C.GoString(&c.ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_open() failed: %s", message)
	}
	return &Database{db}, nil
}

func (c *Context) CloseDatabase(db *Database) error {
	if rc := C.grn_obj_close(c.ctx, db.obj); rc != C.GRN_SUCCESS {
		message := C.GoString(&c.ctx.errbuf[0])
		return fmt.Errorf("grn_obj_close() failed: %s", message)
	}
	return nil
}

func (c *Context) Send(command string) error {
	cCommand := C.CString(command)
	defer C.free(unsafe.Pointer(cCommand))
	rc := C.grn_ctx_send(c.ctx, cCommand, C.uint(len(command)), 0)
	if rc != C.GRN_SUCCESS {
		message := C.GoString(&c.ctx.errbuf[0])
		return fmt.Errorf("grn_ctx_send() failed: %s", message)
	}
	if c.ctx.rc != C.GRN_SUCCESS {
		message := C.GoString(&c.ctx.errbuf[0])
		return fmt.Errorf("%s", message)
	}
	return nil
}

func (c *Context) Receive() ([]byte, error) {
	var resultBuffer *C.char
	var resultLength C.uint
	var flags C.int

	rc := C.grn_ctx_recv(c.ctx, &resultBuffer, &resultLength, &flags)
	if rc != C.GRN_SUCCESS {
		message := C.GoString(&c.ctx.errbuf[0])
		return nil, fmt.Errorf("grn_ctx_recv() failed: %s", message)
	}
	result := C.GoBytes(unsafe.Pointer(resultBuffer), C.int(resultLength))

	return result, nil
}

func (c *Context) Query(str string) ([]byte, error) {
	err := c.Send(str)
	if err != nil {
		return nil, err
	}
	return c.Receive()
}
